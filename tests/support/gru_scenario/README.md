# Gru Scenario DSL

This package provides a light, test-focused DSL for scripting Gru scenarios and validating them with a single Plan -> Run -> Verify pass.

## Status
- Canonical package: `tests.support.gru_scenario`
- Canonical entrypoint: `run_gru_scenario(...)` from this package
- Scope: this is the only supported Gru scenario DSL implementation under `tests/support`

## Scope Boundary
- This DSL is for orchestration tests: lifecycle sequencing, waits, stop/shutdown behavior, concurrency, and shared-resource runtime behavior.
- This DSL is not for composition/loader validation tests (for example: module entrypoint resolution, invalid `minion`/`pipeline` declarations, class-subclass/type declaration checks).
- Keep composition acceptance/rejection coverage in manual `gru.start_minion(...)` tests.

## Quick Start
```python
directives = [
    Concurrent(
        MinionStart(...),
        MinionStart(...),
    ),
    WaitWorkflows(),
    MinionStop(expect_success=True, name_or_instance_id="m1"),
    MinionStop(expect_success=True, name_or_instance_id="m2"),
    GruShutdown(expect_success=True),
]

await run_gru_scenario(
    gru,
    logger,
    metrics,
    state_store,
    directives,
    pipeline_event_counts={pipeline_modpath: 1},
)
```

## Directives
- `MinionStart(...)` starts a minion with a pipeline and optional run spec.
  - `minion` and `pipeline` are module path strings in the canonical DSL.
- `MinionStop(...)` stops a minion by name or instance id.
- `Concurrent(...)` runs child directives concurrently.
- `WaitWorkflows(...)` waits for workflow completion.
- `GruShutdown(...)` shuts down the Gru runtime.

## Directive Effects
| Directive | Writes Receipts | Affects Waits | Affects Verification Counts |
|---|---|---|---|
| `MinionStart` | Yes (`success`, `instance_id`, `resolved_name`, directive index) | Yes (successful starts contribute expected workflow waits) | Yes (minion start counts, workflow-step counts, pipeline attempt/success bounds, state-store totals) |
| `MinionStop` | No | No | Indirect only (runtime state changes that may affect later call histories) |
| `Concurrent` | No (container only) | No (container only) | No (container only) |
| `WaitWorkflows` | No | Yes (waits by all started or named subset) | No direct counts; ensures async work is drained before assertions |
| `GruShutdown` | No | No | Yes (`seen_shutdown` gates shutdown-related expectations) |

## Minion Run Spec
Attach this to `MinionStart` when you need workflow-level checks or overrides:
```python
MinionStart(
    ...,
    expect=MinionRunSpec(
        workflow_resolutions={"succeeded": 1, "failed": 0, "aborted": 0},
        minion_call_overrides={"some_step": 0},
    ),
)
```

## Waiting for Workflows
- `WaitWorkflows()` waits for all workflows expected from successful starts so far.
- `WaitWorkflows(minion_names=set())` is a no-op.
- `WaitWorkflows(minion_names={...})` waits only for those names and fails on unknown names.
- The wait first blocks on workflow-step spy call counts, then drains minion tasks.

## Per-Run Expectations
Provide `pipeline_event_counts` for exactly the pipelines started by successful `MinionStart(...)` directives:
```python
await run_gru_scenario(
    ...,
    pipeline_event_counts={pipeline_modpath: 3},
)
```
Expected workflow counts are derived from `pipeline_event_counts`:
- For each successful `MinionStart`, expected workflows = `pipeline_event_counts[pipeline_modpath]`.
- Counts are summed per minion class.
- These counts drive `WaitWorkflows`, workflow-step expectations, and state store save/delete totals.
- Validation is strict:
  - Missing counts for started pipelines fail plan creation.
  - Unused pipeline keys in `pipeline_event_counts` fail plan creation.
  - Counts must be integers `>= 0`.

## Verification Tolerances
- Pipeline `produce_event` counts allow off-by-one: expected `N` or `N+1`.
- Pipeline lifecycle counts are bounded by observed start attempts and successes.
  - `__init__`: between `0/1` (depending on successful starts) and start attempts.
  - `startup` and `run`: bounded similarly, with `run <= startup`.
- Minion `startup`/`run` expectations are derived from successful starts (failed starts are not treated as running minions).
- Resource classes allow unlisted calls when pinning counts.
- Unexpected extra calls during pinning are collected and reported after call-order checks.

## Verification Backlog
- Verification-specific TODOs live in `tests/support/gru_scenario/VERIFICATION_TODOS.md`.
- Usage tests may reference backlog IDs, but verification capability work should be implemented and tested in this DSL package.

## Runtime-Dependent Notes
- Some tolerances intentionally reflect current runtime behavior under concurrency (attempt/success bounds), not idealized singleton assumptions.
- If runtime lifecycle locking/ordering changes, revisit these tolerances and related verifier tests together.

## Options
- `per_verification_timeout`: timeout for waits and spy assertions.

## Test Fixture Policy
- Use `gru` fixture for default wiring (`InMemoryLogger`, `InMemoryMetrics`, `InMemoryStateStore`).
- Use `gru_factory` when a test needs custom `Gru.create(...)` kwargs (for example `None`, `NoOp*`, `ConsoleLogger`).
- Do not import lifecycle helpers directly from `conftest.py`; tests should consume fixtures only.
- Do not open concurrent/nested `gru_factory(...)` contexts in one test because only one `Gru` instance can exist per process.

## Debugging Tips
- Use SpyMixin helpers: `get_call_counts()`, `get_call_history()`, `assert_call_order_for_instance()`.
- Check metrics via `InMemoryMetrics.snapshot_counters()` for workflow resolution mismatches.
- Validate `pipeline_event_counts` against pipeline `produce_event` spy counts.
