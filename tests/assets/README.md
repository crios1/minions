# tests/assets

Directory-first taxonomy for test assets.

- `support/`: shared support types used by assets.
- `config/`: TOML config fixtures.
- `resources/`: resource fixtures.
- `minions/`: minion fixtures.
- `pipelines/`: pipeline fixtures.
- `entrypoints/`: module-entrypoint composition fixtures.
- `events/`: shared event types used by simple-event assets.

Layout convention:
- `tests/assets/<kind>/<scenario_family>/<case>.py`
- If a scenario family exists for multiple event types, use:
  - `tests/assets/<kind>/<scenario_family>/<event_family>/<case>.py`

Event-family policy:
- `counter` is the default event family for orchestration scenarios.
- `simple` is a secondary event family for generic type-shape coverage.

Naming rules:
- Use semantic scenario families as directories.
- Use short snake_case case names.
- Use `_a/_b/_c` clones only for identity-separated equivalents.
- Invalid-definition fixtures live under `entrypoints/invalid` and `minions/invalid` / `pipelines/invalid` as applicable.
