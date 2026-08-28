# Concurrent lifecycle linearizability campaign

This campaign applies seeded batches of overlapping `start_orchestration()` and
`stop_orchestration()` calls to the same orchestration identities.

For each batch, a small reference state machine enumerates every result that
could arise from some valid serialized ordering of the overlapping operations.
The observed success counts and final active state must match one of those
linearizable outcomes. The campaign also checks the public runtime snapshot and
internal component/resource-map consistency after every batch.

It uses `InMemoryStateStore`, `InMemoryMetrics`, and `InMemoryLogger` so the
campaign isolates lifecycle coordination rather than storage durability.

Default workload:

- 32 deterministic seeds;
- 24 concurrent batches per seed;
- 2–6 lifecycle operations per batch; and
- three orchestration compositions, including two that share a Pipeline.

A second seeded scenario races terminal `shutdown()` against twelve starts and
stops. Operations reserved first must drain, later operations may be rejected
as shutting down, and the terminal runtime snapshot must always be empty.

Run it directly:

```shell
.venv/bin/python -m pytest -q -s \
  tests/experimental/runtime_resilience/concurrent_lifecycle/campaign.py
```
