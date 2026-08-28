# Cancellation-pressure and resume campaign

This campaign stops or shuts down Gru while hundreds of workflows are blocked
inside one shared `Resource`.

The runtime contract treats cancellation as an interruption:

- the in-progress step and workflow are measured with status `interrupted`;
- the last pre-step checkpoint remains in the StateStore; and
- restarting the orchestration resumes the retained workflow from that
  checkpoint.

The campaign verifies that contract end to end for:

- concurrent `stop_orchestration()` across 32 subscribers with 16 events each
  (512 interrupted workflows); and
- terminal `shutdown()` across 16 subscribers with 16 events each
  (256 interrupted workflows).

After cancellation, every expected checkpoint must exist exactly once. The
shared Resource is then released, the same orchestration identities are
restarted, all retained workflows must succeed, and all checkpoints must be
deleted. Pipeline production is disabled during restart so resumed-workflow
counts cannot be confused with newly generated live events.

The campaign uses `InMemoryStateStore`, `InMemoryMetrics`, and `NoOpLogger`. It
performs no SQLite or filesystem I/O.

Run it directly:

```shell
.venv/bin/python -m pytest -q -s \
  tests/experimental/runtime_resilience/cancellation_pressure/campaign.py
```
