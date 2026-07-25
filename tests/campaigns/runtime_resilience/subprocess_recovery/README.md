# Subprocess recovery campaign

This directory contains explicitly invoked runtime-verification scenarios. It
is not part of the default pytest collection.

Run the campaign from the repository root:

```bash
.venv/bin/python -m pytest \
  tests/campaigns/runtime_resilience/subprocess_recovery/campaign.py
```

The campaign uses isolated temporary SQLite databases and kills only
subprocesses that it creates. Each scenario enforces a 7 MiB artifact limit,
bounding the thirteen-scenario campaign below 91 MiB.

Covered recovery boundaries:

- before the first checkpoint, while queued, inside an active transaction, and
  immediately after commit;
- during a step, between steps, before terminal-delete commit, and after
  terminal-delete commit;
- during orchestration stop, during Gru shutdown, and through an
  application-owned graceful SIGTERM handler;
- truncated persisted state and state incompatible with the current Minion
  event/context types.

Every recovery checks retained workflow state, step execution counts, and
SQLite integrity. Transaction-sensitive scenarios also verify writer-lock
state from a separate SQLite connection.

When a campaign discovers a runtime defect, add the smallest useful regression
test to the ordinary test suite. Keep campaign machinery here unless its
maintenance as first-class test infrastructure is explicitly accepted.
