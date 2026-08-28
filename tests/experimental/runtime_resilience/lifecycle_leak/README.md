# Gru lifecycle leak campaign

This explicitly invoked campaign looks for retained runtime state across
repeated in-memory Gru lifecycle cycles. It is not part of default pytest
collection.

Run it from the repository root:

```bash
.venv/bin/python -m pytest \
  tests/experimental/runtime_resilience/lifecycle_leak/campaign.py
```

The campaign uses warm-up cycles before measuring:

- runtime snapshot emptiness;
- live asyncio task count;
- file descriptor count;
- weak retention of Gru and framework component instances;
- traced Python allocation growth;
- process RSS growth.

RSS and allocator assertions use conservative plateau limits rather than exact
memory equality.
