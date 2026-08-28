# Stateful Gru lifecycle campaign

This explicitly invoked campaign compares seeded public Gru operations with a
small intent-level reference model. It is not part of default pytest
collection.

Run it from the repository root:

```bash
.venv/bin/python -m pytest \
  tests/experimental/runtime_resilience/stateful_lifecycle/campaign.py
```

The initial model covers orchestration membership and component ownership for:

- starting inactive compositions;
- rejecting duplicate starts;
- stopping active compositions;
- rejecting missing or already-stopped orchestration IDs;
- shared versus distinct Pipeline ownership;
- Gru shutdown and repeated shutdown.

Every command is followed by assertions over the public runtime snapshot.
Failures include their seed and command index so the exact sequence can be
replayed and reduced into a focused regression test.

The default campaign executes 128 deterministic seeds with 64 commands each,
for 8,192 modeled public operations per run.
