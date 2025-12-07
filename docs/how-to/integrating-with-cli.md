# Integrating with the CLI

`GruShell` is an experimental control plane for a running Gru instance. It is built on `cmd.Cmd`, so it runs in the foreground and submits orchestration actions to the event loop.

## Wiring it up

Run the shell in a background thread so it does not block the asyncio loop:

```python
shell = GruShell(gru)
await asyncio.to_thread(shell.cmdloop)
```

The shell shares the Gru event loop; commands submit coroutines with `asyncio.run_coroutine_threadsafe`.

## Commands

- `start MINION_MODULEPATH MINION_CONFIG_PATH PIPELINE_MODULEPATH`
  - Queues a start. Module paths must resolve to a `minion`/`pipeline` variable or a single subclass in the module.
- `stop NAME_OR_INSTANCE_ID ...`
  - Stops matching minions. Names map to `Minion.name`; instance IDs come from `start` responses and `status`.
- `status [--await] [--timeout N] [targets...]`
  - Without flags: prints a snapshot of pending/starting/running/stopping counts or specific targets.
  - With `--await`: waits for pending operations to settle.
- `shutdown`
  - Cancels running tasks, shuts down resources/state/metrics/loggers, and exits the shell loop.

`help` in the shell shows the same usage strings.

## Caveats

- The shell API is evolving; method names may change as the CLI is refined.
- Commands are non-blocking by default; use `status --await` to wait for orchestration to finish.
- Run inside `tmux`/`screen` or a supervisor when embedding into a long-lived process.
