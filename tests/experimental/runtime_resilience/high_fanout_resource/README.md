# High-fanout shared-resource campaign

This campaign stresses one shared pipeline and one shared slow `Resource` across
many independently configured minion instances.

It verifies:

- every subscriber receives exactly one event and completes one workflow;
- slow resource calls overlap under fanout instead of being silently lost or
  serialized;
- workflow, pipeline, and resource metrics have the expected totals and
  label-set cardinality;
- metric emissions continue to satisfy the framework label contract;
- in-memory checkpoints are drained after successful workflows; and
- shutdown leaves no runtime state or campaign-created asyncio tasks behind.

The campaign intentionally uses `InMemoryStateStore` and `InMemoryMetrics`.
Durable recovery is covered by the subprocess campaign; this workload isolates
runtime scheduling, shared ownership, and instrumentation without disk I/O.

Run it directly:

```shell
.venv/bin/python -m pytest -q -s \
  tests/experimental/runtime_resilience/high_fanout_resource/campaign.py
```

The default scenarios are single-event fanout to 16, 64, and 128 subscribers,
plus a 32-event burst to 32 subscribers (1,024 workflows). Timing assertions
are broad liveness bounds, not performance benchmarks.
