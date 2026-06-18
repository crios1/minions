# Portability and Deployment Footprint

Minions is designed around a clear principle:

**A Minions system should be portable as a single project directory.**  
Copy the folder anywhere, and your entire system—code, configs, orchestrations, and state—runs exactly the same.

This page explains how Minions achieves that, and what counts as part of your portable deployment footprint.

---

## Why portability matters

Traditional microservice systems are tied to:

- container registries  
- orchestration manifests  
- environment-specific config paths  
- operational infrastructure  

Minions takes a different approach: your *project directory* is the deployment artifact.

A Minions system is just:

```
my_project/
    minions/
    pipelines/
    resources/
    configs/
    state.sqlite
    your_entrypoint.py
```

If this directory exists, Minions can orchestrate it.

---

## What Minions treats as portable

Minions treats the following as part of the portable deployment footprint:

### 1. Component identities

Durable Minion, Pipeline, and Resource identity is stored with the component
class:

```python
from minions import minion_id, pipeline_id, resource_id

@minion_id("11111111-1111-4111-8111-111111111111")
class MeanReversionMinion(...):
    ...
```

The UUID moves with the class, so normal module moves and renames do not change
its runtime identity. Id-less components remain supported for prototypes and
fall back to their class address or supplied entrypoint module. Those fallback
identities are portable when module names remain unchanged, but they are not
refactor-stable.

### 2. Config files inside the project directory

If you start a minion with:

```python
gru.start_orchestration(
    "myapp.pipelines.pricing",
    "myapp.strategies.mean_reversion",
    minion_config_path="configs/client-a.yaml",
)
```

For durable deployments, stamp the config with a top-level UUID:

```yaml
_minions_config_id: "44444444-4444-4444-8444-444444444444"
```

Gru then uses that UUID as the config identity, independent of where the project
or config file is moved. Without `_minions_config_id`, an in-project config uses
its project-relative path as fallback identity.

---

## What Minions treats as non-portable

If you point Minions at a config file *outside* the project directory:

```python
gru.start_orchestration(
    "myapp.pipelines",
    "myapp.strategies",
    minion_config_path="/etc/minions/client-a.yaml",
)
```

Without `_minions_config_id`, Gru uses the resolved absolute config path as the
fallback identity. That fallback is machine-specific. A stamped external config
retains its UUID identity, but the external file is still outside the
self-contained deployment artifact and must be provisioned separately.

---

## Inline config and portability

Class-based “inline config” (using a dataclass or `msgspec.Struct` instance) uses
a content-derived identity:

```
<inline:digest>
```

Example:

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

The configured Minion declares `config: MyConfig` before its steps access
`self.config`.

Inline deployments are portable, but their identity is tied to the in-memory
config value. They’re intended for development, teaching, and exploration. For
long-lived deployment slots, use file-based configs with
`_minions_config_id`.

---

## Why Minions defaults to relative paths

Stamped component and config IDs give Minions systems a property that
distributed systems often require additional deployment infrastructure to
maintain:

> **A Minions deployment is fully self-contained. Copy the folder anywhere and everything—including running state—still works.**

Your SQLite state store, configs, pipelines, and code remain coherent as a unit.
Relative paths remain useful fallbacks while prototyping, but explicit IDs are
the durable contract for moving and refactoring that unit.

---

## Best practices for portable Minions systems

- Place all configs in a dedicated `configs/` folder inside your project.  
- Stamp Minion, Pipeline, Resource, and file config IDs before durable deployment.
- Run `python -m minions doctor ids .` as a deployment preflight.
- Ensure your entrypoint (e.g., `python -m yourapp.main`) lives inside the project.
- Use relative config paths even when configs are stamped so loading remains portable.
- Let Minions resolve absolute paths internally for loading.  
- Keep state stores (SQLite, files, checkpoints) inside the project directory.  

If you follow this structure, Minions gives you a deployment artifact that works anywhere.

---

## Summary

- Explicit UUIDs provide refactor-stable Minion, Pipeline, Resource, and file config identities.
- Id-less components and configs use module/path fallbacks for low-friction prototypes.
- External configs remain external deployment dependencies even when stamped.
- Inline config is portable and identity follows its serializable type and value.
- A Minions project directory is intentionally designed to be a **self-contained system**.

Portability isn’t an accident—it’s a design goal. Minions gives you microservice structure with single-artifact deployment simplicity.

See {doc}`state-and-persistence` for the complete identity matrix and the
required migration boundary when stamping an existing deployment.
