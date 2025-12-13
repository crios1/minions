# Deployment Strategies

Minions is built for single-process orchestrations. Here are pragmatic ways to run it in the real world.

- **Supervised process**: wrap your app with `systemd`, `supervisord`, or a container runtime that restarts on failure. Gru enforces a single instance per process.
- **Separate risky code**: if you depend on native extensions that can segfault, keep them in subprocesses and talk over a narrow IPC API; let Gru restart cleanly.
- **Scale-out topology**: when scale-up isn’t enough, replicate runtimes with sharded ownership or offload hotspots to sidecars (see {doc}`scale-out-strategies`).
- **Metrics endpoint**: expose Prometheus metrics (default port 8081) and alert on workflow failures, resource errors, and high memory/CPU gauges.
- **Logs**: default logger writes to files; inject your own logger to ship to structured log pipelines.
- **Persistence**: keep `minions.db` (SQLite) on durable storage if you rely on workflow resumption; or set `state_store=None` if you prefer stateless runs.
- **Graceful shutdown**: use `Gru.shutdown()` on SIGTERM to cancel tasks and close resources before the supervisor kills the process.

## Linux + systemd quick recipe

Deploy the runtime as a supervised service so crashes restart automatically and stdout/err land in `journald` for inspection:

```
# /etc/systemd/system/minions.service
[Unit]
Description=Minions runtime
After=network.target

[Service]
WorkingDirectory=/opt/minions
ExecStart=/usr/bin/python3 -m yourapp.orchestrations.launch
Restart=on-failure
RestartSec=2
User=minions
Group=minions
Environment=PYTHONUNBUFFERED=1
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Then:

```
sudo systemctl daemon-reload
sudo systemctl enable --now minions.service
```

If the process crashes, systemd restarts it; `journalctl -u minions.service` shows stdout/err. Keep your project directory (code, configs, state) on durable storage; the runtime will resume workflows from the persisted state store.

As the project matures, expect richer deployment helpers and CI hooks to build docs (`make -C docs html`) and fail on Sphinx warnings.
