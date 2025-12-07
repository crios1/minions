# CLI Reference

`minions.shell` exposes `GruShell`, an interactive controller for a running Gru. It wraps core orchestration calls (`start_minion`, `stop_minion`, `shutdown`) and prints status snapshots.

## Entry points

- Import directly: `from minions import GruShell`
- Module paths passed to `start` follow Python import semantics (e.g., `my_app.minions`). Each module must expose either:
  - a single `Minion`/`Pipeline` subclass, or
  - a module-level variable named `minion`/`pipeline` pointing to the desired class.

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
