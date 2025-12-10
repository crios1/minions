# State and Persistence

Minions treats workflow context as durable. Before each step executes, Gru saves context + step index to the configured `StateStore`; when a workflow finishes or aborts, the entry is removed. On startup, Gru reloads any saved contexts and resumes from the stored step index.

## Defaults

- **State store**: SQLite-backed store by default.
- **Disable persistence**: pass `state_store=None` to `{py:class}``minions._internal._domain.gru.Gru.create``.
- **Custom stores**: implement `{py:class}``minions._internal._framework.state_store.StateStore`` with `save_context`, `delete_context`, and `load_all_contexts`.

## Workflow lifecycle

- Context and events must be JSON-serializable structured types (dataclasses/TypedDicts, not bare primitives).
- Context is saved **before** each step runs; failures keep the last saved state for debugging/resume.
- Success deletes the context at the end; aborts log and clean up.
- Gru logs workflow/step transitions and surfaces errors with user file/line when available.

## Config loading

Each minion receives a config path when started. The default loader supports TOML, YAML, and JSON, returning a `dict`. Override `Minion.load_config` for custom formats or validation.

## Persistence strategy

- Keep contexts small; store IDs and lightweight data, not blobs.
- Make steps **idempotent** so replays after crashes are safe.
- Use resource-level rate limiting when resuming many workflows to avoid thrashing dependencies.
