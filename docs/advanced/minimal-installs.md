# Minimal installs and feature opt-outs

Most users can stick with `pip install minions`—the defaults are lightweight. If you’re targeting small devices (RPi, phones, embedded boards) and want to trim behavior rather than uninstall packages, use Gru’s knobs to opt out of features.

## What ships by default

- **Persistence**: SQLite via `aiosqlite` (small footprint). Disable by passing `state_store=None` to `Gru.create(...)` or by providing your own `StateStore`.
- **Metrics**: `prometheus-client` for counters/gauges/histograms. You can ignore the endpoint or override the metrics implementation if you truly don’t want Prometheus.
- **Docs/dev tooling**: only installed with `minions[dev]` (Sphinx, pytest, etc.).

These two runtime deps are modest; for most devices it’s simpler to leave them installed even if you opt out at runtime.

## If you really want to slim further

- Turn off persistence (`state_store=None`) and metrics (custom `Metrics` impl).  
- Removing packages from `pip install minions` isn’t supported via extras (extras can add, not remove). Uninstalling `aiosqlite` or `prometheus-client` will break the default store/metrics; only do it if you supply replacements and never import the defaults.

## Bottom line

Use configuration to disable features; keep the small default deps unless you’re certain you’re replacing them. That keeps your deployment simple while remaining ready to re-enable features later.
