# State and Persistence

Minions treats workflow context as durable. Before each step executes, Gru saves context + step index to the configured `StateStore`; when a workflow finishes or aborts, the entry is removed. On startup, Gru reloads any saved contexts and resumes from the stored step index.

## Defaults

- **State store**: SQLite-backed store by default.
- **Disable persistence**: pass `state_store=None` to {py:meth}`minions.Gru.create`.
- **Custom stores**: implement {py:class}`minions.interfaces.StateStore` with `save_context`, `delete_context`, `get_contexts_for_orchestration`, and `get_all_contexts`. State stores persist opaque workflow context blobs; the Minions runtime owns event/context serialization before calling the store. Override `startup` and `shutdown` only when the store needs async setup or cleanup.

## Workflow lifecycle

- Context and events must be serializer-supported structured types (dataclasses/TypedDicts/`msgspec.Struct`, not bare primitives).
- Map-like event/context data must use string keys when annotated explicitly (`dict[str, V]` or `Mapping[str, V]`). Bare `dict` remains accepted as an ad hoc/prototyping schema, but production workflow state should use explicit serializable value types.
- Context is saved **before** each step runs; failures keep the last saved state for debugging/resume.
- Success deletes the context at the end; aborts log and clean up.
- Runtime cancellation is treated as an interruption, not an intentional abort. Interrupted workflows keep their persisted context so they can be replayed later; raise `AbortWorkflow` from a step when the workflow should stop as an intentional terminal outcome.
- Gru logs workflow/step transitions and surfaces errors with user file/line when available.

## Durable component identity

Gru accepts Minion, Pipeline, and Resource classes with or without explicit component IDs.

Id-less components are useful for first runs, examples, notebooks, and other prototype workflows. They keep the current fallback behavior: Gru derives identity from the import/module address it can resolve at orchestration start. That keeps onboarding low-friction, but it does not promise refactor-stable resume or metric continuity if files, modules, or class addresses move.

The identity sources are:

| Runtime object | Durable identity | Fallback identity | Stability |
| --- | --- | --- | --- |
| Minion | `@minion_id(...)` UUID | Class module/name, or the string entrypoint module | Fallback changes when its address changes |
| Pipeline | `@pipeline_id(...)` UUID | Class module/name, or the string entrypoint module | Fallback changes when its address changes |
| Resource | `@resource_id(...)` UUID | Class module/name | Fallback changes when its address changes |
| File-backed config | Top-level `_minions_config_id` UUID | Project-relative path, or absolute path outside the project | Fallback changes when the path changes |
| Inline config | Not stampable | Content-derived `<inline:digest>` | Stable for the same serializable type and value |

Class-based versus string-based startup controls how Gru loads a component and
which fallback address is available. It does not determine whether the
component has durable identity. Explicit component and config UUIDs provide
refactor-stable identity in either startup form.

For durable systems, stamp UUID component IDs on source classes:

```python
from minions import Minion, Pipeline, Resource, minion_id, pipeline_id, resource_id

@pipeline_id("22222222-2222-4222-8222-222222222222")
class PricePipeline(Pipeline[PriceEvent]):
    ...

@resource_id("33333333-3333-4333-8333-333333333333")
class PriceClient(Resource):
    ...

@minion_id("11111111-1111-4111-8111-111111111111")
class TradingMinion(Minion[PriceEvent, TradingCtx]):
    client: PriceClient
    ...
```

These IDs are opaque canonical UUID strings. Do not encode names, environments, or business meaning into them. Gru keeps a cheap loaded-class duplicate check so copy/paste mistakes fail clearly, but uniqueness comes from generated UUIDs rather than a persistent registry.

Use the CLI helper to stamp missing component IDs:

```bash
python -m minions stamp component-ids path/to/minion.py path/to/pipeline.py path/to/resource.py
```

This command edits source files by adding missing `@minion_id(...)`, `@pipeline_id(...)`, and `@resource_id(...)` decorators with generated UUIDs. Use `--dry-run` to inspect what it would stamp without writing files.

If a project has already run workflows with id-less prototype identities, drain those workflows before stamping durable component/config IDs and redeploying. Stamping changes the orchestration identity from path/module fallback identity to durable UUID-based identity, so previously persisted prototype workflows are not expected to resume automatically after the cutover.

TOML, YAML, and JSON config files can also carry a top-level `_minions_config_id` UUID. Gru reads that key before normal `Minion.load_config(...)` runs and uses it as the config identity for orchestration IDs. Config files without `_minions_config_id` still use path fallback identity.

Stamped TOML/YAML config files include a short generated comment:

```toml
# Generated by `minions stamp config-ids`; do not edit manually.
_minions_config_id = "66666666-6666-4666-8666-666666666666"

[config]
name = "alpha"
```

```yaml
# Generated by `minions stamp config-ids`; do not edit manually.
_minions_config_id: "66666666-6666-4666-8666-666666666666"

config:
  name: alpha
```

An `orchestration_id` is an opaque deterministic identity for the current runtime composition: `minion_id`, `pipeline_id`, and `minion_config_id`. It is a fixed-width, 44-character base62 string backed by a SHA-256 digest. The base62 form keeps IDs shorter than hexadecimal while remaining alphanumeric and stable for logs, metrics, CLI output, and persistence lookup. It is intentionally not parseable. Orchestration-scoped logs include the compact `orchestration_id` plus the source identities as separate fields: `minion_id`, `pipeline_id`, and `minion_config_id`.

Inline `minion_config` is intended for exploration and tests, not production durability. Use file-backed configs with `_minions_config_id` before relying on persisted workflow resume across deploys or refactors.

When Gru resumes persisted workflows for a started orchestration, it decodes events and contexts using the current Minion class schema. That means moving a context class import path does not by itself prevent resume, as long as the current Minion declares a compatible event/context model and the durable component/config IDs are unchanged.

## StateStore write contract

Every `StateStore` implementation persists opaque serialized workflow context
blobs. The runtime owns serialization and deserialization; stores do not
inspect or mutate workflow event/context data.

By default those blobs are internal binary `msgspec` payloads. They are a runtime
durability format, not a human-readable interchange format or a public storage
schema for custom stores to inspect.

The important contract for write methods is:

- `save_context(...)` returns only after the requested checkpoint is persisted
- `delete_context(...)` returns only after the delete is persisted

Returning after mere acceptance into an in-memory buffer is not sufficient for
the `StateStore` interface.

## Persistence failure policy

- Configure `workflow_persistence_failure_policy` on {py:meth}`minions.Gru.create`.
- `continue-on-failure` is the default. Persistence failures are logged, the workflow keeps running, and the runtime retries persistence at the next workflow checkpoint.
- `idle-until-persisted` pauses workflow advancement after a failed persistence attempt and resumes only after persistence succeeds. Use it when durable checkpoint storage must succeed before the next step is allowed to run.
- Workflow checkpoints are the persistence boundaries before each workflow step begins.
- Serialization failures are treated as deterministic and non-retryable. The workflow does not advance past the checkpoint, and any prior durable checkpoint is preserved for inspection or replay.
- StateStore save failures are treated as retryable operational failures. In `idle-until-persisted`, they retry indefinitely with capped exponential backoff.
- After user code reaches a terminal workflow outcome, Minions still treats checkpoint deletion as part of workflow resolution. If checkpoint deletion fails, the workflow does not become terminal until the delete succeeds.
- Idle retry backoff starts at `workflow_persistence_retry_delay_seconds` (default `1.0`), multiplies by `workflow_persistence_retry_backoff_multiplier` (default `2.0`), is capped by `workflow_persistence_retry_max_delay_seconds` (default `60.0`), and applies `workflow_persistence_retry_jitter_ratio` jitter (default `0.1`) to avoid synchronized retries.
- Sustained idle retries log repeated warnings every `workflow_persistence_retry_warning_interval_seconds` (default `30.0`) and escalate to error logs after `workflow_persistence_retry_error_after_seconds` (default `60.0`; pass `None` to disable escalation).
- Persistence failure logs include the workflow checkpoint, persistence operation (`save` or `delete`), retry attempt, elapsed retry time, failure stage, state store, event/context types, minion identity, and exception details when available.

## SQLite batching semantics

`SQLiteStateStore` may group accepted save/delete operations into shared SQLite
transactions for throughput, but it does not weaken the `StateStore` write
contract:

- batched `save_context(...)` still returns only after the batch commit that
  contains the write succeeds
- batched `delete_context(...)` still returns only after the batch commit that
  contains the delete succeeds

So batching in SQLite is a performance strategy, not a fire-and-forget
durability mode. The runtime-level persistence policy still lives in
`Gru.create(...)` via `workflow_persistence_failure_policy` and the retry
settings.

## Persistence telemetry

Workflow persistence metrics describe the runtime durability guarantee as experienced by workflows. They are separate from lower-level `state_store_*` backend health metrics.

- `minion_workflow_persistence_attempts_total`
- `minion_workflow_persistence_succeeded_total`
- `minion_workflow_persistence_failures_total`
- `minion_workflow_persistence_duration_seconds`
- `minion_workflow_persistence_blocked_gauge`

Use `minion_workflow_persistence_blocked_gauge` and sustained persistence failures to alert on workflow durability impact. Use labels such as `minion_workflow_persistence_failure_stage`, `minion_workflow_persistence_retryable`, `minion_workflow_persistence_policy`, `minion_workflow_persistence_checkpoint_type`, `minion_workflow_step`, and `state_store` to distinguish serialization problems from StateStore outages.

### How to read persistence telemetry

- `minion_workflow_persistence_attempts_total` counts every runtime-owned persistence attempt, including step-checkpoint saves and terminal checkpoint deletes.
- `minion_workflow_persistence_succeeded_total` counts successful attempts using the same label set as `attempts_total`.
- `minion_workflow_persistence_failures_total` counts failed attempts and adds failure classification labels such as `minion_workflow_persistence_failure_stage` and `minion_workflow_persistence_retryable`.
- `minion_workflow_persistence_duration_seconds` records end-to-end latency for each save or delete attempt.
- `minion_workflow_persistence_blocked_gauge` reports how many workflows are currently unable to advance or resolve because a retryable persistence operation is still outstanding.

The most important labels are:

- `minion_workflow_persistence_operation`: `save` means Minions is trying to durably checkpoint workflow progress; `delete` means user code has already reached a terminal outcome and the runtime is resolving the workflow by removing its checkpoint.
- `minion_workflow_persistence_checkpoint_type`: `workflow_start`, `before_step`, or `workflow_resolve` tells you where the runtime is in the workflow lifecycle.
- `minion_workflow_persistence_failure_stage`: distinguishes codec/serialization problems from StateStore save/delete failures.
- `minion_workflow_persistence_retryable`: `false` means the workflow data itself is not persistable and retrying will not help; `true` means the failure is operational and the runtime may retry.
- `minion_workflow_persistence_policy`: tells you whether the workflow is configured to continue after save failure or idle until the checkpoint is durably persisted.
- `state_store`: identifies which backend is experiencing the problem.

Common interpretations:

- `operation="save"` and a non-zero blocked gauge means workflows are paused before the next step because Minions cannot safely persist progress.
- `operation="delete"` and a non-zero blocked gauge means user code is done, but workflow resolution is waiting for checkpoint cleanup before Minions marks the workflow terminal.
- `failure_stage="serialize"` with `retryable="false"` points to workflow event/context data that the persistence codec cannot encode.
- `failure_stage="save"` or `failure_stage="delete"` with `retryable="true"` points to a StateStore or backend availability problem.

For a broader operator view, see {doc}`/guides/operating-with-metrics`.

## Config loading

Each file-backed minion receives a config path when started. Override
`Minion.load_config` to parse that path and return a dataclass or
`msgspec.Struct` config model. Declare the model as the Minion's typed
`config` attribute before accessing `self.config` from workflow steps. Minions
validates the returned config model before workflows start; it does not expose
raw parsed config dictionaries to step code.

`_minions_config_id` is reserved runtime metadata. If your custom `load_config` parses a TOML, YAML, or JSON file with `_minions_config_id`, you can ignore that key when building the user-facing config model. The runtime reads it separately only to identify the durable config; config contents and config revisions remain application
responsibility.

## Persistence strategy

- Keep contexts small; store IDs and lightweight data, not blobs.
- Make steps **idempotent** so replays after crashes are safe.
- Use resource-level rate limiting when resuming many workflows to avoid thrashing dependencies.
