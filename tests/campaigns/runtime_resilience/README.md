# Runtime resilience verification

These campaigns exercise recovery, lifecycle coordination, resource ownership,
fanout pressure, cancellation/replay, and process/resource cleanup beyond the
default unit-test suite.

Run every campaign once, sequentially:

```shell
.venv/bin/python scripts/runtime_resilience_soak.py
```

Run a longer soak with bounded concurrency and a JSON summary:

```shell
.venv/bin/python scripts/runtime_resilience_soak.py \
  --repeat 10 \
  --jobs 2 \
  --json-summary /tmp/minions-resilience-summary.json
```

Run selected campaigns:

```shell
.venv/bin/python scripts/runtime_resilience_soak.py \
  --campaign concurrent-lifecycle \
  --campaign cancellation-pressure \
  --repeat 25
```

Each invocation runs in an isolated pytest process with its own temporary
directory and log. Passing artifacts are removed by default; failing and timed
out logs are retained. Use `--retain-passing-artifacts` to keep everything.

The default per-process timeout is 180 seconds and the default concurrency is
one. `--stop-on-failure` stops launching new processes after the first failure.

## Storage impact

All campaigns except `subprocess-recovery` use in-memory components and do not
intentionally write application state to disk. The subprocess recovery
campaign uses temporary file-backed SQLite because durability is the behavior
under test. Its conservative bound is less than 91 MiB of database writes per
campaign invocation, so `--repeat N` has a conservative upper bound below
`91 × N` MiB plus small pytest logs and temporary metadata.

## Campaigns

- `subprocess-recovery`: hard-death and restart persistence boundaries.
- `stateful-lifecycle`: seeded sequential lifecycle reference model.
- `lifecycle-leak`: repeated construction/shutdown resource bounds.
- `high-fanout-resource`: fanout, burst, slow Resource, and metric cardinality.
- `concurrent-lifecycle`: linearizability and shutdown admission races.
- `resource-failure-storm`: repeated shared dependency failure containment.
- `cancellation-pressure`: mass interruption, retained checkpoints, and replay.
