# Shared-resource failure-storm campaign

This campaign repeatedly fails a shared transitive `Resource` while Gru is
serving both dependent and unrelated orchestrations.

Each cycle:

1. starts two orchestrations whose distinct Minions share one dependent
   Resource and its failing dependency;
2. verifies the expected Resource topology and reference counts;
3. triggers the dependency's runtime task failure;
4. concurrently starts an unrelated probe orchestration;
5. waits for failure finalization to remove only the dependent
   orchestrations;
6. verifies the healthy and probe branches, their shared Resources, and all
   runtime maps remain consistent; and
7. stops the probe before rebuilding the failed branch in the next cycle.

The workload uses `InMemoryStateStore`, `InMemoryMetrics`, and
`InMemoryLogger`. It performs no SQLite or filesystem I/O.

Run it directly:

```shell
.venv/bin/python -m pytest -q -s \
  tests/experimental/runtime_resilience/resource_failure_storm/campaign.py
```

The default workload is 64 consecutive failure/rebuild cycles.
