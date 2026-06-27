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
- Keep composition acceptance/rejection coverage in manual `gru.start_orchestration(...)` tests.

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
  - invalid scenario usage,
  - timeout/verification failures,
  - expectation mismatches between declared directives and observed runtime behavior.
- `ScenarioPlan(...)` raises `ValueError` for plan-construction validation errors
  (for example invalid `pipeline_event_counts` counts). Missing/unused durable
  pipeline IDs are scenario-contract failures reported by runner verification.
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
- For strict overlap-window assertions, prefer fixed-count deterministic fixtures:
  - base: `tests.assets.support.pipeline_fixed_event_count.FixedEventCountSpiedPipeline`,
  - overlap fixture: `tests.assets.support.pipeline_overlap_window`.
- Reason: base `Pipeline.run` loops indefinitely and may produce an additional `produce_event` call beyond target counts; fixed-count fixtures bound event emission so strict window mismatches fail for the intended reason.

## Quick Start
```python
start_1 = OrchestrationStart(...)
start_2 = OrchestrationStart(...)

directives = [
    Concurrent(
        start_1,
        start_2,
    ),
    WaitWorkflowCompletions(),
    OrchestrationStop(id=start_1, expect_success=True),
    OrchestrationStop(id=start_2, expect_success=True),
    GruShutdown(expect_success=True),
]

await run_gru_scenario(
    gru,
    logger,
    metrics,
    state_store,
    directives,
    pipeline_event_counts={pipeline_id: 1},
)
```

## Directives
Directives fall into two broad roles:
- Gru command directives execute runtime operations directly, such as `OrchestrationStart(...)`, `OrchestrationStop(...)`, and `GruShutdown(...)`.
- Condition-setup directives express when another directive should run, so tests can deterministically create runtime states that may expose lifecycle bugs.

- `OrchestrationStart(...)` starts an orchestration from a minion, pipeline, and optional minion config.
  - `minion` and `pipeline` are component refs, matching `Gru.start_orchestration(...)`.
  - String refs are module-path load addresses; class refs are Minion/Pipeline subclasses.
  - Gru resolves refs to component IDs. Decorated components use their explicit `@minion_id(...)`
    / `@pipeline_id(...)`; idless components use the module path as the fallback ID.
  - Use `minion_config_path` with module path strings.
  - Use `minion_config` with class-based starts.
- `OrchestrationStop(id=...)` stops an orchestration by orchestration ID.
- `OrchestrationStop(id=<OrchestrationStart>)` stops the orchestration started by that earlier `OrchestrationStart` directive.
- `Concurrent(...)` runs child directives concurrently.
- `WaitWorkflowCompletions(...)` waits for workflow completion.
- `AfterWorkflowStepStarts(expected, directive)` waits for explicit start-directive/step counts, then immediately executes a wrapped directive.
  - The wrapped directive must currently be `OrchestrationStop(...)`.
  - Use this for checkpoint/resume tests that need a specific workflow step boundary.
- `ExpectRuntime(...)` evaluates runtime expectations at the current scenario checkpoint.
  - Expectations are keyed by the exact `OrchestrationStart` directive whose runtime state should be checked.
  - Supported targets: `at="latest"` (directive checkpoint) or `at=<checkpoint_index>` (0-based).
- `GruShutdown(...)` shuts down the Gru runtime.

### Directive Identity
- Every directive object represents one distinct occurrence in a scenario plan, even when two directives have identical field values.
- Create a separate `OrchestrationStart(...)` object for each start or resume attempt.
- References in `OrchestrationStop`, `WaitWorkflowCompletions`, `AfterWorkflowStepStarts`, and `RuntimeExpectSpec` must use the exact `OrchestrationStart` object included earlier in the plan.
- Do not reuse the same directive object in multiple plan positions; `ScenarioPlan` rejects duplicate object references.
- Identity-based keys remain hashable when `minion_config` contains unhashable inline values such as dictionaries or lists.

## Directive Effects
| Directive | Writes Receipts | Affects Waits | Affects Verification Counts |
|---|---|---|---|
| `OrchestrationStart` | Yes (`success`, `orchestration_id`, directive index) | Yes (successful starts contribute expected workflow waits) | Yes (minion start counts, workflow-step counts, pipeline attempt/success bounds, state-store totals) |
| `OrchestrationStop` | No | No | Indirect only (runtime state changes that may affect later call histories) |
| `Concurrent` | No (container only) | No (container only) | No (container only) |
| `WaitWorkflowCompletions` | No | Yes (waits by all started or explicit start-directive subset) | No direct counts; ensures async work is drained before assertions |
| `AfterWorkflowStepStarts` | No | Yes (waits for start-directive step targets before running wrapped directive) | Indirect only (wrapped directive effects and checkpoint snapshots) |
| `ExpectRuntime` | No | No | Yes (checkpoint-scoped runtime expectations and internal persisted-context integrity checks) |
| `GruShutdown` | No | No | Yes (`seen_shutdown` gates shutdown-related expectations) |

## Waiting for Workflows
- `WaitWorkflowCompletions()` waits for all workflows expected from successful starts so far.
- `WaitWorkflowCompletions(orchestrations=())` is a no-op.
- `WaitWorkflowCompletions(orchestrations=(start_a, start_b))` waits only for those starts.
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
- `AfterWorkflowStepStarts(expected={...}, directive=OrchestrationStop(...))` is the checkpoint condition primitive for stopping at a specific step boundary.

## Runtime Expectations
- `ExpectRuntime(at="latest", expect=RuntimeExpectSpec(...))` asserts runtime state at the current checkpoint.
- `ExpectRuntime(at=0, expect=RuntimeExpectSpec(...))` asserts runtime state at an explicit checkpoint index.
- Persistence section:
```python
start = OrchestrationStart(minion=minion_ref, pipeline=pipeline_ref)

ExpectRuntime(
    expect=RuntimeExpectSpec(
        persistence={start: 1},
    ),
)
```
- When persisted context snapshots are available, the verifier also validates persisted context integrity internally: matching started minion/orchestration, declared event/context types, `context_cls`, and `next_step_index` bounds.
- Resolutions section:
```python
ExpectRuntime(
    expect=RuntimeExpectSpec(
        resolutions={start: {"succeeded": 1, "failed": 0, "aborted": 0}},
    ),
)
```
- Workflow-step progression section (step workflow-ID counts for a start):
```python
ExpectRuntime(
    expect=RuntimeExpectSpec(
        workflow_steps={start: {"step_1": 1, "step_2": 1}},
        workflow_steps_mode="exact",  # "exact" or "at_least"
    ),
)
```
- Runtime expectation mappings are keyed by the exact successful `OrchestrationStart` directive being asserted.
- Persisted context content/type checks are evaluated against decoded StateStore snapshots, using the started minion type to decode event/context payloads when available.
- Resolution counts are evaluated from checkpoint metrics snapshots for the start's orchestration.
- Workflow-step progression counts are evaluated from checkpoint workflow step-start workflow IDs.
- `ExpectRuntime.workflow_steps_mode="exact"` is checkpoint-total exactness (strict equality for declared counts at target checkpoint), not window-tolerance mode.
- Strictness policy lock:
  - `ExpectRuntime.workflow_steps_mode="exact"` does not apply wait-window `+successful_starts_in_window` tolerance.
  - That bounded tolerance is intentionally scoped to `WaitWorkflowCompletions(workflow_steps_mode="exact")` window checks only.
- Restart/resume (strict) example:
```python
start = OrchestrationStart(minion=minion_ref, pipeline=pipeline_ref)
resume = OrchestrationStart(minion=minion_ref, pipeline=pipeline_ref)
directives = [
    start,
    AfterWorkflowStepStarts(
        expected={start: {"step_2": 1}},
        directive=OrchestrationStop(id=start, expect_success=True),
    ),
    ExpectRuntime(
        expect=RuntimeExpectSpec(
            persistence={start: 1},
            workflow_steps={start: {"step_1": 1, "step_2": 1}},
            workflow_steps_mode="exact",
        ),
    ),
    resume,
    WaitWorkflowCompletions(),
    ExpectRuntime(
        expect=RuntimeExpectSpec(
            resolutions={resume: {"succeeded": 2, "failed": 0, "aborted": 0}},
            workflow_steps={resume: {"step_1": 2, "step_2": 2}},
            workflow_steps_mode="exact",
        ),
    ),
    OrchestrationStop(id=resume, expect_success=True),
]
```

## Per-Run Expectations
Provide `pipeline_event_counts` for exactly the resolved pipeline IDs started by successful `OrchestrationStart(...)` directives. Keys may be resolved pipeline ID strings, module-path strings, or `Pipeline` subclasses:
```python
await run_gru_scenario(
    ...,
    pipeline_event_counts={pipeline_id: 3},
)
```
Expected workflow counts are derived from `pipeline_event_counts`:
- For each successful `OrchestrationStart`, expected workflows = `pipeline_event_counts[pipeline_id]`.
- For idless pipelines, `pipeline_id` is the module-path fallback ID. For decorated pipelines,
  it is the explicit `@pipeline_id(...)` value, even when the start ref is a module path.
- Pipeline subclass keys are normalized to the same resolved ID before validation.
- Counts are summed per minion class.
- These counts drive `WaitWorkflowCompletions`, workflow-step expectations, and state store save/delete totals.
- Validation is strict:
  - Missing counts for started pipeline IDs fail runner verification.
  - Unused pipeline IDs in `pipeline_event_counts` fail runner verification.
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

## Automatic Lifecycle Verification
- Scenario authors declare lifecycle actions; they do not declare Gru registry or task counts.
- The runner records stable lifecycle observations after top-level start, stop, concurrent, wrapped-stop, and shutdown boundaries.
- The verifier derives expected active orchestration, minion, pipeline, resource, and task identities from successful start receipts, resolved dependencies, and stop history.
- It also verifies minion/pipeline resource ownership maps, resource dependency edges, reverse-dependent edges, and total resource refcounts without scenario authors declaring those values.
- This verifies shared pipeline/resource retention, per-start tracking, cleanup after the last owner stops, and empty runtime state after successful shutdown.

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
