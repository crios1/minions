# Startup Forms: Inline vs. Module-Based

This page describes how Minions Core loads workflow components inside a running `Gru` process. It is separate from the broader Core/Compose/Cluster {doc}`execution ladder <execution-ladder>`.

Minions Core supports two startup forms:

1. **Inline mode** — pass Minion/Pipeline classes and an optional dataclass or `msgspec.Struct` config.
2. **Module-based mode** — pass module paths and a file-backed config.

Both forms are fully supported. They differ in how Gru loads components and
supplies config, but durable identity is controlled separately by component and
config IDs.

---

## Inline mode (class-based)

Inline mode is the ergonomic, “local-first” way to start a minion:

```python
from dataclasses import dataclass


@dataclass
class MyConfig:
    my_key: str


gru.start_orchestration(
    MyPipeline,
    MyMinion,
    minion_config=MyConfig(my_key="my_value"),
)
```

`MyMinion` declares `config: MyConfig` when its steps read
`self.config`.

Use inline mode when you’re exploring, testing, or composing a runtime directly
in Python. Gru derives the config identity from the serializable config type and
value as `<inline:digest>`. Component identities come from `@minion_id(...)`
and `@pipeline_id(...)` when present; otherwise Gru falls back to each class's
module and name. Starting the same component identities with the same inline
config value addresses the same orchestration.

Inline config identity is deterministic, but inline config is not intended as a
long-lived deployment slot. Use a stamped file-backed config before relying on
resume across deployment refactors.

## Module-based mode

Module-based mode loads components from module strings and can use a file-backed
config:

```python
gru.start_orchestration(
    "minions.examples.pipeline",
    "minions.examples.strategy",
    minion_config_path="configs/strategy-client-a.yaml",
)
```

Use module-based mode for declared deployments, multiple configured instances,
and operator-controlled startup. Component identities still come from
`@minion_id(...)` and `@pipeline_id(...)` when present. Id-less components use
the supplied module strings as fallback identities.

A TOML, YAML, or JSON config with a top-level `_minions_config_id` uses that UUID
as its durable identity. An id-less config falls back to its project-relative
path, or its absolute path when it is outside the project.

## Choosing between the two

- Pick **inline** for tests, notebooks/REPLs, and direct Python composition.
- Pick **module-based** for declared, file-configured, or operator-managed runtime instances.
- Stamp component and file config IDs before relying on refactor-stable resume or metric continuity in either mode.

## Why two modes?

Minions supports direct Python composition without making module-based
startup cumbersome. The startup form and durability contract are separate:
choose the form that fits how the runtime is launched, then add explicit IDs
when the identities must survive source and config moves. See
{doc}`state-and-persistence` for the complete identity matrix and migration
guidance.
