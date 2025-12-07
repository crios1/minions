# State and Persistence

Minions treats workflow context as durable. Before each step executes, the context is saved to the configured `StateStore`; when a workflow finishes or aborts, the context entry is removed.

## Defaults

- **State store**: SQLite backed store (`SQLiteStateStore`) by default.
- **Disable persistence**: pass `state_store=None` to `{py:class}``minions._internal._domain.gru.Gru.create`` to opt out.
- **Custom stores**: implement `{py:class}``minions._internal._framework.state_store.StateStore`` with `save_context`, `delete_context`, and `load_all_contexts`.

## Workflow lifecycle

- On startup, Gru loads any stored contexts and resumes workflows from the saved `step_index`.
- Before each step runs, the current context is saved. Failures keep the last saved state for debugging/resume; successes delete it at the end.
- Context and events must be JSON-serializable (structured types, not bare primitives).

## Config loading

Each minion receives a config path when started. The default loader supports TOML, YAML, and JSON, returning a `dict`. Override `Minion.load_config` for custom formats or validation.

## Persistence strategy

- Keep contexts small; store IDs and lightweight data, not blobs.
- Design steps to be **idempotent** so replays after crashes are safe.
- Use resource-level rate limiting when resuming many workflows to avoid thrashing dependent services.
