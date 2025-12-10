# Runtime Modes: Inline vs. Deployment

Minions supports two ways of starting minions:

1. **Inline mode** — pass Minion/Pipeline classes and an optional Python dict config.
2. **Deployment mode** — pass module paths and a file-based config.

Both modes are fully supported, but they serve different purposes and have different guarantees.

---

## Inline mode (class-based)

Inline mode is the ergonomic, “local-first” way to start a minion:

```python
gru.start_minion(
    MyMinion,
    MyPipeline,
    minion_config={"pair": "ETH/USDC"},
)
```

Use inline mode when you’re exploring, prototyping, or running a single instance. Configs live in memory, and the runtime uses a single placeholder identity (`<inline>`) for the deployment slot: `/abs/path/to/minion.py|<inline>|/abs/path/to/pipeline.py`. Starting the same (minion, pipeline) again replaces the prior inline instance; it’s not intended for multiple distinct instances of the same minion.

## Deployment mode (string-based)

Deployment mode is the production path—module strings plus a real config file:

```python
gru.start_minion(
    "minions.examples.strategy:MyMinion",
    "minions.examples.pipeline:MyPipeline",
    minion_config_path="configs/strategy-client-a.yaml",
)
```

Use deployment mode when you need multiple independent instances, long-running workloads, resumability, or operational safety. Identity is derived from real file paths: `/abs/path/to/minion.py|/abs/path/to/config.yaml|/abs/path/to/pipeline.py`. That stable identity enables resumable workflows, deterministic redeployments, and dependency-aware draining or cutovers.

## Choosing between the two

- Pick **inline** for first-time trials, notebooks/REPLs, simple scripts, and single-instance runs where you don’t need durable identity.
- Pick **deployment** for anything long-lived or multi-instance, where you want restartability, operational auditability, and deterministic identity tied to config.

## Why two modes?

Minions is a runtime, not just a library. Runtimes need explicit operational identity for long-lived workloads, but developers also need a frictionless on-ramp. Inline mode keeps experimentation fast and Pythonic; deployment mode provides the stability, resumability, and orchestration guarantees you want in real systems. Start inline in a handful of lines, then graduate to deployment mode as your system grows.
