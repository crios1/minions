# Integrating with the CLI

`GruShell` is a deprecated in-process control helper for a running Gru instance. It is built on `cmd.Cmd`, runs in the foreground, and submits orchestration actions to the event loop.

The planned replacement is a split `minions gru serve` / `minions gru attach` model: `serve` owns the long-lived runtime, while `attach` connects to it without stopping the runtime when the attach session exits. Treat `GruShell` as a temporary local/demo path and as source material for that control model, not as the production operator interface.

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

- The shell API is deprecated and may be removed once `minions gru serve` / `minions gru attach` exists.
- Commands are non-blocking by default; use `status --await` to wait for orchestration to finish.
- Exiting this in-process shell tears down the local runtime; use a supervisor only for temporary experiments until the serve/attach model replaces it.
