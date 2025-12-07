# Testing Minions

Strategies for validating workflows and resources.

- **Test steps directly**: instantiate your minion with fake `Logger/Metrics/StateStore` (or pass `None` via `Gru.create` and inject test doubles) and call step coroutines with synthetic contexts/events.
- **Use in-memory helpers**: the `tests/assets/support` directory contains simple in-memory loggers, metrics, and state stores you can mimic for your own tests.
- **Simulate persistence**: ensure context objects round-trip through your state store implementation; assert that `step_index` increments and contexts are deleted on success.
- **Exercise config loading**: write tests around `load_config` to reject invalid files early.
- **Observe metrics/logs**: assert that important counters (workflow started/succeeded/failed) and log messages are produced when steps succeed or raise.
- **Keep workflows deterministic**: avoid hidden timeouts or sleeping in steps; use dependency injection to stub I/O and clocks.
