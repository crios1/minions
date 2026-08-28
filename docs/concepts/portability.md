# Portability and Deployment Footprint

Minions is designed around a clear principle:

**A Minions project should keep its code, in-project configuration, component
identities, and local workflow state together.**

Moving that directory can preserve code, in-project configuration, component
identity, and local workflow state. The destination still needs a compatible
Python environment and access to any external services, assets, configuration,
or persistence the system deliberately keeps outside the project.

This page explains how Minions achieves that, and what counts as part of your portable deployment footprint.

---

## Why portability matters

Traditional microservice systems are tied to:

- container registries  
- orchestration manifests  
- environment-specific config paths  
- operational infrastructure  

A project directory can serve as the deployment artifact for that code,
configuration, identity, and local state.

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

When its Python environment and external dependencies are available, Minions
can load and orchestrate this project as a unit.

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

Passing `minion_config` requires the Minion to declare the framework-defined
`config` attribute with the accepted model type. Gru validates and binds the
model by value before workflows start; steps then access the independent
snapshot as `self.config`. Later mutations to the caller's object do not affect
the Minion, and the bound snapshot should be treated as immutable.

Inline startup is portable, but its identity is tied to the captured config type
and value. Use it for development, teaching, and exploration. For long-lived
deployment slots, use file-based configs with `_minions_config_id`.

---

## Why Minions defaults to relative paths

Stamped component and config IDs let source, configuration, and persisted
workflow state retain their identities when an in-project deployment footprint
is relocated.

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

If you follow this structure, the project directory contains the Minions data
needed to relocate the runtime. Provision the destination's Python environment
and any external dependencies separately.

---

## Summary

- Explicit UUIDs provide refactor-stable Minion, Pipeline, Resource, and file config identities.
- Id-less components and configs use module/path fallbacks for low-friction prototypes.
- External configs remain external deployment dependencies even when stamped.
- Inline config is portable and identity follows its serializable type and value.
- A Minions project directory can contain code, in-project configuration,
  identities, and local state as one relocatable unit.

Portability isn’t an accident—it’s a design goal. Minions gives you explicit
component structure while keeping these in-project parts together.

See {doc}`state-and-persistence` for the complete identity matrix and the
required migration boundary when stamping an existing deployment.
