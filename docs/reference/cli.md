# CLI Reference

Minions ships a small CLI surface for local tuning helpers and transitional runtime control.

## Supported entrypoints

- Preferred during local development: `python -m minions ...`
- Installed console script after package install: `minions ...`

Current commands:

- `python -m minions shell`
- `python -m minions stamp component-ids PATH [...]`
- `python -m minions stamp config-ids PATH [...]`
- `python -m minions stamp all PATH [...]`
- `python -m minions doctor ids PATH [...]`
- `python -m minions tune sqlite --recommend-config`

`minions.shell` exposes `GruShell`, a deprecated in-process controller for a running Gru. It is retained as a local demo/helper and as design material for the planned `minions gru serve` / `minions gru attach` control model. Do not treat it as the production operator interface.

## Entry points

- Import directly: `from minions import GruShell`
- Module CLI:
  - `python -m minions shell` (deprecated transitional helper)
  - `python -m minions stamp component-ids PATH [...]`
  - `python -m minions stamp config-ids PATH [...]`
  - `python -m minions stamp all PATH [...]`
  - `python -m minions doctor ids PATH [...]`
  - `python -m minions tune sqlite --recommend-config`
- Installed CLI after package install:
  - `minions shell`
  - `minions stamp component-ids PATH [...]`
  - `minions stamp config-ids PATH [...]`
  - `minions stamp all PATH [...]`
  - `minions doctor ids PATH [...]`
  - `minions tune sqlite --recommend-config`
- Module paths passed to `start` follow Python import semantics (e.g., `my_app.minions`). The recommended shape is one `Minion` or `Pipeline` subclass defined in each entrypoint module.
- If a module contains multiple local component classes, expose a module-level variable named `minion` or `pipeline` pointing to the desired entrypoint class.

## SQLite tuning

Use the SQLite tuner when you want a recommendation for
`SQLiteStateStore` transaction grouping on the current machine and
workload shape.

Examples:

```bash
python -m minions tune sqlite --recommend-config
python -m minions tune sqlite --recommend-config --recommend-output json
python -m minions tune sqlite --tune-curve
```

The recommender reports:

- immediate baseline metrics
- one `batched_balanced` recommendation
- one `batched_max_throughput` recommendation

This command is operational tooling; it does not change runtime behavior
automatically. It only tunes how SQLite groups accepted save/delete
operations into transactions.

Workflow persistence retry behavior is configured when creating `Gru`, not
through the SQLite tuner. Use `workflow_persistence_failure_policy` and the
`workflow_persistence_retry_*_seconds` settings on `Gru.create(...)` to control
whether workflows continue after checkpoint failures or idle until persistence
recovers. Retryable StateStore save failures use capped exponential backoff;
non-retryable serialization failures stop workflow advancement and preserve any
previous durable checkpoint.

## Component ID stamping

Use the component ID stamper when you are ready to turn prototype components into durable components with refactor-stable identity:

```bash
python -m minions stamp component-ids src/my_app
python -m minions stamp component-ids --dry-run src/my_app
```

The command accepts Python files or directories. For directories, it recursively scans `*.py` files, skipping hidden/cache/build folders. For each direct subclass missing the matching decorator, it inserts a generated canonical UUID decorator:

- `@minion_id("...")`
- `@pipeline_id("...")`
- `@resource_id("...")`

It also adds the required imports from `minions`. Classes that already have the matching component ID decorator are left unchanged.

The stamped UUID is the durable component identity. Id-less components still work, but they use module/path fallback identity and should be treated as prototype/non-durable for refactor-stable resume and metric continuity.

Before stamping IDs on a project that has run id-less prototype workflows, drain those workflows and deploy from a clean durable boundary. Stamping IDs changes the orchestration identity, so old prototype persisted contexts are not expected to resume automatically under the new UUID-based identity.

Use the config ID stamper to add config UUIDs to TOML, YAML, and JSON config files:

```bash
python -m minions stamp config-ids configs
python -m minions stamp config-ids --dry-run configs
```

The command accepts config files or directories. For directories, it recursively scans TOML, YAML, and JSON files, skipping hidden/cache/build folders. It inserts a top-level `_minions_config_id` when missing. Gru uses that UUID as the durable config identity. Config files without `_minions_config_id` still work, but they use path fallback identity.

Use `stamp all` when you intentionally want both source component IDs and config IDs stamped in one pass:

```bash
python -m minions stamp all .
python -m minions stamp all --dry-run .
```

For TOML and YAML, the stamper also adds a short generated comment:

```toml
# Generated by `minions stamp config-ids`; do not edit manually.
_minions_config_id = "66666666-6666-4666-8666-666666666666"
```

```yaml
# Generated by `minions stamp config-ids`; do not edit manually.
_minions_config_id: "66666666-6666-4666-8666-666666666666"
```

## Identity diagnostics

Use `doctor ids` as a non-mutating preflight before deployment:

```bash
python -m minions doctor ids .
```

The command accepts files or directories, recursively scans Python source and TOML/YAML/JSON config files, and exits non-zero when it finds identity issues:

- missing component ID decorators
- invalid component UUIDs
- duplicate component UUIDs by component kind
- missing `_minions_config_id`
- invalid config UUIDs
- duplicate config UUIDs

Run `stamp ... --dry-run` when you want to preview edits, and `doctor ids` when you want a read-only project identity check.

## Command details

- `start MINION_MODULE_PATH MINION_CONFIG_PATH PIPELINE_MODULE_PATH`
  - Starts an orchestration if one with the same orchestration identity is not already running. Returns a `StartResult` with an opaque deterministic SHA-256 `orchestration_id`.
- `stop ORCHESTRATION_ID ...`
  - Stops matching orchestrations and cleans up resources/pipelines that are no longer referenced.
- `status [--await] [--timeout N] [targets...]`
  - Without arguments, prints a summary of pending/starting/running/stopping counts.
  - With targets, shows state for specific IDs or pending start handles (`pending:<id>`).
  - `--await` waits for pending operations before printing.
- `shutdown`
  - Cancels running tasks, shuts down metrics/loggers/state store, and exits the shell.

The shell is synchronous and in-process; run it in a thread (`asyncio.to_thread(shell.cmdloop)`) so it can submit work to the asyncio loop without blocking it. Exiting the shell tears down that local runtime, which is why the long-term operator flow is moving to separate `serve` and `attach` commands.
