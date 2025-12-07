# Deployment Strategies

Minions is built for single-process orchestrations. Here are pragmatic ways to run it in the real world.

- **Supervised process**: wrap your app with `systemd`, `supervisord`, or a container runtime that restarts on failure. Gru enforces a single instance per process.
- **Separate risky code**: if you depend on native extensions that can segfault, keep them in subprocesses and talk over a narrow IPC API; let Gru restart cleanly.
- **Metrics endpoint**: expose Prometheus metrics (default port 8081) and alert on workflow failures, resource errors, and high memory/CPU gauges.
- **Logs**: default logger writes to files; inject your own logger to ship to structured log pipelines.
- **Persistence**: keep `minions.db` (SQLite) on durable storage if you rely on workflow resumption; or set `state_store=None` if you prefer stateless runs.
- **Graceful shutdown**: use `Gru.shutdown()` on SIGTERM to cancel tasks and close resources before the supervisor kills the process.

As the project matures, expect richer deployment helpers and CI hooks to build docs (`make -C docs html`) and fail on Sphinx warnings.
