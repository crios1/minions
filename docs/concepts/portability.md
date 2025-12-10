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

### 1. Module locations (minions and pipelines)

Minion and Pipeline identity is based on:

```
cls.__module__
```

This is independent of absolute file paths. If your project folder moves, your module names stay the same.

### 2. Config files inside the project directory

If you start a minion with:

```python
gru.start_minion(
    "myapp.strategies.mean_reversion",
    "myapp.pipelines.pricing",
    minion_config_path="configs/client-a.yaml",
)
```

And `configs/client-a.yaml` lives **inside your project folder**, Minions records it in the composite key as:

```
myapp.strategies.mean_reversion|configs/client-a.yaml|myapp.pipelines.pricing
```

Not an absolute path. This identity is fully portable: works if you zip and move the project, commit to Git, or run inside Docker. Minions loads the file using the absolute resolved path, but stores the **relative** path as the deployment identity.

---

## What Minions treats as non-portable

If you point Minions at a config file *outside* the project directory:

```python
gru.start_minion(
    "myapp.strategies",
    "myapp.pipelines",
    minion_config_path="/etc/minions/client-a.yaml",
)
```

Then the composite key becomes:

```
myapp.strategies|/etc/minions/client-a.yaml|myapp.pipelines
```

This is correct, but not portable. Minions does not restrict you—you can load configs from anywhere—but the identity reflects that you chose an external dependency. This keeps flexibility without hiding operational realities.

---

## Inline config and portability

Class-based “inline config” (using a Python dict) uses a fixed identity:

```
<inline>
```

Example:

```python
gru.start_minion(MyMinion, MyPipeline, minion_config={"pair": "ETH/USDC"})
```

Composite key:

```
myapp.minions.MyMinion|<inline>|myapp.pipelines.MyPipeline
```

Inline deployments are portable, but they occupy a single slot per (minion, pipeline). They’re intended for development, teaching, and exploration. For multiple independent instances, use file-based configs.

---

## Why Minions defaults to relative paths

Relative config paths give Minions systems a property that distributed systems lack:

> **A Minions deployment is fully self-contained. Copy the folder anywhere and everything—including running state—still works.**

Your SQLite state store, configs, pipelines, and code remain coherent as a unit. This is a major ergonomic advantage of Minions and one of the runtime’s core values.

---

## Best practices for portable Minions systems

- Place all configs in a dedicated `configs/` folder inside your project.  
- Ensure your entrypoint (e.g., `python -m yourapp.main`) lives inside the project.  
- Use relative config paths when calling `gru.start_minion`.  
- Let Minions resolve absolute paths internally for loading.  
- Keep state stores (SQLite, files, checkpoints) inside the project directory.  

If you follow this structure, Minions gives you a deployment artifact that works anywhere.

---

## Summary

- Minions uses **module names** (not file paths) to identify minions and pipelines.  
- Config paths inside the project directory become **portable deployment identities**.  
- Config paths outside the project directory become **absolute, external identities**.  
- Inline config is portable but single-instance.  
- A Minions project directory is intentionally designed to be a **self-contained system**.

Portability isn’t an accident—it’s a design goal. Minions gives you microservice structure with single-artifact deployment simplicity.
