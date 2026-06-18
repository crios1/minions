# CLI Internals

CLI-related APIs for the deprecated `GruShell` helper and internal tooling entrypoints.

```{autosummary}
:toctree: _generated
:recursive:

minions._internal._domain.gru_shell
```

`GruShell` is deprecated as a control-plane direction. It remains available as a transitional local helper and as design material for the planned `minions gru serve` / `minions gru attach` model.

The SQLite tuning and identity commands are supported as CLI entrypoints rather
than public Python APIs. See {doc}`../cli` for their command reference.
