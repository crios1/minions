# CLI Reference

Minions ships a small CLI surface for interactive control and local tuning helpers.

## Supported entrypoints

- Preferred during local development: `python -m minions ...`
- Installed console script after package install: `minions ...`

Current commands:

- `python -m minions shell`
- `python -m minions tune sqlite --recommend-config`

`minions.shell` exposes `GruShell`, an interactive controller for a running Gru. It wraps core orchestration calls (`start_minion`, `stop_minion`, `shutdown`) and prints status snapshots.

## Entry points

- Import directly: `from minions import GruShell`
- Module CLI:
  - `python -m minions shell`
  - `python -m minions tune sqlite --recommend-config`
- Installed CLI after package install:
  - `minions shell`
  - `minions tune sqlite --recommend-config`
- Module paths passed to `start` follow Python import semantics (e.g., `my_app.minions`). Each module must expose either:
  - a single `Minion`/`Pipeline` subclass, or
  - a module-level variable named `minion`/`pipeline` pointing to the desired class.

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

## Command details

- `start MINION_MODULEPATH MINION_CONFIG_PATH PIPELINE_MODULEPATH`
  - Starts a minion if one with the same composite key is not already running. Returns a `StartMinionResult` with `instance_id` and `name`.
- `stop NAME_OR_INSTANCE_ID ...`
  - Stops matching minions and cleans up resources/pipelines that are no longer referenced. May return conflict info when multiple minions share a name.
- `status [--await] [--timeout N] [targets...]`
  - Without arguments, prints a summary of pending/starting/running/stopping counts.
  - With targets, shows state for specific IDs or pending start handles (`pending:<id>`).
  - `--await` waits for pending operations before printing.
- `shutdown`
  - Cancels running tasks, shuts down metrics/loggers/state store, and exits the shell.

The shell is synchronous; run it in a thread (`asyncio.to_thread(shell.cmdloop)`) so it can submit work to the asyncio loop without blocking it.
