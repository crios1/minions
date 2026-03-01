# Gru Scenario DSL

This package provides a light, test-focused DSL for scripting Gru scenarios and validating them with a single Plan -> Run -> Verify pass.

## Status
- Canonical package: `tests.support.gru_scenario`
- Canonical entrypoint: `run_gru_scenario(...)` from this package
- Scope: this is the only supported Gru scenario DSL implementation under `tests/support`
- Canonical component contract for DSL runs:
  - logger: `tests.assets.support.logger_spied.SpiedLogger`
  - metrics: `tests.assets.support.metrics_spied.SpiedMetrics`
  - state store: `tests.assets.support.state_store_spied.SpiedStateStore`
  - default fixtures (`InMemoryLogger`, `InMemoryMetrics`, `InMemoryStateStore`) satisfy this contract.

## Scope Boundary
- This DSL is for orchestration tests: lifecycle sequencing, waits, stop/shutdown behavior, concurrency, and shared-resource runtime behavior.
- This DSL is not for composition/loader validation tests (for example: module entrypoint resolution, invalid `minion`/`pipeline` declarations, class-subclass/type declaration checks).
- Keep composition acceptance/rejection coverage in manual `gru.start_minion(...)` tests.

## Official Contract
- DSL orchestration tests are deterministic by policy.
- Tests must declare orchestration and expectations up front (plan-first).
- Core verification must assert exact expected outcomes, not timing-dependent approximations.
- No optional nondeterministic mode is supported for DSL confidence/correctness tests.
- Orchestration testing is DSL-only policy:
  - add/update orchestration tests in DSL usage classes and DSL verifier tests,
  - do not add manual orchestration tests or weaker orchestration-only validations outside DSL,
  - non-DSL/weaker orchestration validation is intentionally not pursued because DSL deterministic verification eclipses its value.

### Failure Semantics
- Use `pytest.fail(...)` for scenario-contract failures:
  - invalid scenario usage (for example unknown names/directives),
  - timeout/verification failures,
  - expectation mismatches between declared directives and observed runtime behavior.
- `ScenarioPlan(...)` raises `ValueError` for plan-construction validation errors
  (for example missing/unused `pipeline_event_counts` entries or invalid counts).
- Use `AssertionError` only for strict internal invariants in DSL internals:
  - impossible internal state that indicates a DSL implementation bug (not a scenario-authoring issue).
- Reasoning:
  - scenario-contract failures should surface as explicit test failures with actionable diagnostics;
  - internal invariants should clearly indicate framework bugs;
  - neither category should depend on Python `assert` statements, which can be stripped under `-O`/`-OO`.

### Why DSL Deterministic Verification Eclipses Weaker Orchestration Checks
- Deterministic DSL orchestration tests provide higher-value verification because they:
  - declare orchestration and expected outcomes up front,
  - enforce synchronization invariants required for exact fanout/subscription assertions,
  - produce stable, reproducible pass/fail signals with actionable diagnostics.
- Weaker/non-DSL orchestration checks provide lower confidence because they cannot reliably prove exact runtime behavior across timing-sensitive scenarios.

### Synchronization Invariant
- Shared/fanout verification requires an explicit subscription barrier before event emission.
- Until the barrier is owned by DSL internals, tests should use synchronization-capable test pipelines (gated assets) for deterministic fanout checks.
- Pipeline assets used for deterministic fanout checks are test synchronization assets by design; this does not redefine production runtime semantics.
- For strict overlap-window assertions, prefer scripted deterministic fixtures:
  - base: `tests.assets.support.pipeline_scripted.ScriptedSpiedPipeline`,
  - overlap fixture: `tests.assets.support.pipeline_overlap_window`.
- Reason: base `Pipeline.run` loops indefinitely and may produce an additional `produce_event` call beyond target counts; scripted fixtures bound event emission so strict window mismatches fail for the intended reason.

## Quick Start
```python
directives = [
    Concurrent(
        MinionStart(...),
        MinionStart(...),
    ),
    WaitWorkflowCompletions(),
    MinionStop(name_or_instance_id="m1", expect_success=True),
    MinionStop(name_or_instance_id="m2", expect_success=True),
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
- `MinionStart(...)` starts a minion with a pipeline.
  - `minion` and `pipeline` are module path strings in the canonical DSL.
- `MinionStop(...)` stops a minion by name or instance id.
- `Concurrent(...)` runs child directives concurrently.
- `WaitWorkflowCompletions(...)` waits for workflow completion.
- `AfterWorkflowStarts(expected, directive)` waits for workflow starts then immediately executes a wrapped directive.
  - Initial supported wrapped directive: `MinionStop(...)`.
- `ExpectRuntime(...)` evaluates runtime expectations at the current scenario checkpoint.
  - Initial supported section: `expect=RuntimeExpectSpec(persistence={minion_name: count})`.
  - Supported targets: `at="latest"` (directive checkpoint) or `at=<checkpoint_index>` (0-based).
- `GruShutdown(...)` shuts down the Gru runtime.

## Directive Effects
| Directive | Writes Receipts | Affects Waits | Affects Verification Counts |
|---|---|---|---|
| `MinionStart` | Yes (`success`, `instance_id`, `resolved_name`, directive index) | Yes (successful starts contribute expected workflow waits) | Yes (minion start counts, workflow-step counts, pipeline attempt/success bounds, state-store totals) |
| `MinionStop` | No | No | Indirect only (runtime state changes that may affect later call histories) |
| `Concurrent` | No (container only) | No (container only) | No (container only) |
| `WaitWorkflowCompletions` | No | Yes (waits by all started or named subset) | No direct counts; ensures async work is drained before assertions |
| `AfterWorkflowStarts` | No | Yes (waits for start targets before running wrapped directive) | Indirect only (wrapped directive effects) |
| `ExpectRuntime` | No | No | Yes (checkpoint-scoped runtime expectations such as persisted-context counts) |
| `GruShutdown` | No | No | Yes (`seen_shutdown` gates shutdown-related expectations) |

## Waiting for Workflows
- `WaitWorkflowCompletions()` waits for all workflows expected from successful starts so far.
- `WaitWorkflowCompletions(minion_names=set())` is a no-op.
- `WaitWorkflowCompletions(minion_names={...})` waits only for those names and fails on unknown names.
- `WaitWorkflowCompletions(workflow_steps_mode="at_least"|"exact")` controls checkpoint-window step-delta strictness.
  - default is `"at_least"` (compatibility mode).
  - use `"exact"` for deterministic windows where over-production should fail.
  - in `"exact"`:
    - workflow-id deltas must be exact (`expected`),
    - call-count deltas must stay within `expected..expected+successful_starts_in_window` when workflow-id evidence is available,
    - otherwise call-count deltas must be strict equality (`expected`).
  - mismatch diagnostics are deterministic:
    - if workflow-id evidence exists and workflow-id delta mismatches, that mismatch is reported first,
    - otherwise call-count mismatch is reported with explicit expected range and actual delta.
- The wait first blocks on workflow-step spy call counts, then drains minion tasks.
- `AfterWorkflowStarts(expected={...}, directive=MinionStop(...))` is the race-safe primitive for restart/resume stop points.

## Runtime Expectations
- `ExpectRuntime(at="latest", expect=RuntimeExpectSpec(...))` asserts runtime state at the current checkpoint.
- `ExpectRuntime(at=0, expect=RuntimeExpectSpec(...))` asserts runtime state at an explicit checkpoint index.
- Initial persistence section:
```python
ExpectRuntime(
    expect=RuntimeExpectSpec(
        persistence={"my-minion-name": 1},
    ),
)
```
- Initial resolutions section:
```python
ExpectRuntime(
    expect=RuntimeExpectSpec(
        resolutions={"my-minion-name": {"succeeded": 1, "failed": 0, "aborted": 0}},
    ),
)
```
- Workflow-step progression section (step workflow-id counts by minion name):
```python
ExpectRuntime(
    expect=RuntimeExpectSpec(
        workflow_steps={"my-minion-name": {"step_1": 1, "step_2": 1}},
        workflow_steps_mode="exact",  # "exact" or "at_least"
    ),
)
```
- Persistence counts are resolved by scenario-local minion names observed from successful starts.
- Resolution counts are evaluated from checkpoint metrics snapshots for scenario-local minion instances.
- Workflow-step progression counts are evaluated from checkpoint workflow step-start workflow IDs.
- `ExpectRuntime.workflow_steps_mode="exact"` is checkpoint-total exactness (strict equality for declared counts at target checkpoint), not window-tolerance mode.
- Strictness policy lock:
  - `ExpectRuntime.workflow_steps_mode="exact"` does not apply wait-window `+successful_starts_in_window` tolerance.
  - That bounded tolerance is intentionally scoped to `WaitWorkflowCompletions(workflow_steps_mode="exact")` window checks only.
- Restart/resume (strict) example:
```python
directives = [
    MinionStart(minion=minion_modpath, pipeline=pipeline_modpath),
    AfterWorkflowStarts(
        expected={"slow-step-minion": 1},
        directive=MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
    ),
    ExpectRuntime(
        expect=RuntimeExpectSpec(
            persistence={"slow-step-minion": 1},
            workflow_steps={"slow-step-minion": {"step_1": 1}},
            workflow_steps_mode="exact",
        ),
    ),
    MinionStart(minion=minion_modpath, pipeline=pipeline_modpath),
    WaitWorkflowCompletions(),
    ExpectRuntime(
        expect=RuntimeExpectSpec(
            resolutions={"slow-step-minion": {"succeeded": 2, "failed": 0, "aborted": 0}},
            workflow_steps={"slow-step-minion": {"step_1": 2}},
            workflow_steps_mode="exact",
        ),
    ),
]
```

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
- These counts drive `WaitWorkflowCompletions`, workflow-step expectations, and state store save/delete totals.
- Validation is strict:
  - Missing counts for started pipelines fail plan creation.
  - Unused pipeline keys in `pipeline_event_counts` fail plan creation.
  - Counts must be integers `>= 0`.

## Verification Tolerances
- Pipeline `produce_event` counts allow off-by-one: expected `N` or `N+1`.
- Minion workflow-step fanout counts are bounded by successful starts:
  expected workflow calls per class are `N..N+starts` (reflecting the same +1-per-start tolerance).
- Pipeline lifecycle counts are bounded by observed start attempts and successes.
  - `__init__`: between `0/1` (depending on successful starts) and start attempts.
  - `startup` and `run`: bounded similarly, with `run <= startup`.
- Minion `startup`/`run` expectations are derived from successful starts (failed starts are not treated as running minions).
- Resource classes allow unlisted calls when pinning counts.
- Unexpected extra calls during pinning are collected and reported after call-order checks.

## Verification Backlog
- Verification-specific TODOs live in `tests/support/gru_scenario/VERIFICATION_TODOS.md`.
- Usage tests may reference backlog IDs, but verification capability work should be implemented and tested in this DSL package.
- Backlog implementation must preserve the Official Contract and Synchronization Invariant above.

## Runtime-Dependent Notes
- Some tolerances intentionally reflect current runtime behavior under concurrency (attempt/success bounds), not idealized singleton assumptions.
- If runtime lifecycle locking/ordering changes, revisit these tolerances and related verifier tests together.
- Minion composition rule: workflow steps cannot call other workflow steps directly (`self.step_x(...)`); step sequencing is owned by the runtime workflow engine.
- Alternate backend note:
  - non-in-memory logger/metrics/state-store backends can be used with DSL scenarios if they implement the spied component contract above.
  - required capabilities include spy call-history/count surfaces plus snapshot surfaces consumed by runner/verifier (for example logs, metrics counters, persisted contexts).

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
