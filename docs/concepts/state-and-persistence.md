# State and Persistence

Minions treats workflow context as durable. Before each step executes, Gru saves context + step index to the configured `StateStore`; when a workflow finishes or aborts, the entry is removed. On startup, Gru reloads any saved contexts and resumes from the stored step index.

## Defaults

- **State store**: SQLite-backed store by default.
- **Disable persistence**: pass `state_store=None` to {py:meth}`minions.Gru.create`.
- **Custom stores**: implement {py:class}`minions.interfaces.StateStore` with `save_context`, `delete_context`, `get_contexts_for_orchestration`, and `get_all_contexts`. State stores persist opaque workflow context blobs; the Minions runtime owns event/context serialization before calling the store. Override `startup` and `shutdown` only when the store needs async setup or cleanup.

## Workflow lifecycle

- Context and events must be JSON-serializable structured types (dataclasses/TypedDicts, not bare primitives).
- Map-like event/context data must use string keys when annotated explicitly (`dict[str, V]` or `Mapping[str, V]`). Bare `dict` remains accepted as an ad hoc/prototyping schema, but production workflow state should use explicit serializable value types.
- Context is saved **before** each step runs; failures keep the last saved state for debugging/resume.
- Success deletes the context at the end; aborts log and clean up.
- Runtime cancellation is treated as an interruption, not an intentional abort. Interrupted workflows keep their persisted context so they can be replayed later; raise `AbortWorkflow` from a step when the workflow should stop as an intentional terminal outcome.
- Gru logs workflow/step transitions and surfaces errors with user file/line when available.

## StateStore write contract

Every `StateStore` implementation persists opaque serialized workflow context
blobs. The runtime owns serialization and deserialization; stores do not
inspect or mutate workflow event/context data.

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

Each minion receives a config path when started. The default loader supports TOML, YAML, and JSON, returning a `dict`. Override `Minion.load_config` for custom formats or validation.

## Persistence strategy

- Keep contexts small; store IDs and lightweight data, not blobs.
- Make steps **idempotent** so replays after crashes are safe.
- Use resource-level rate limiting when resuming many workflows to avoid thrashing dependencies.
