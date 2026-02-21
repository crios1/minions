# tests/assets_revamp

Directory-first taxonomy for test assets.

- `support/`: shared support types used by assets.
- `config/`: TOML config fixtures.
- `resources/`: resource fixtures.
- `minions/`: minion fixtures.
- `pipelines/`: pipeline fixtures.
- `entrypoints/`: module-entrypoint composition fixtures.

Naming rules:
- Use semantic scenario families as directories.
- Use short snake_case case names.
- Use `_a/_b/_c` clones only for identity-separated equivalents.
- Invalid-definition fixtures live under `entrypoints/invalid` and `minions/invalid` / `pipelines/invalid` as applicable.
