### Priority Refs:
- ref: "todo: stabilize orchestration lifecycle and identity"


### Test Suite:

- todo: review the latest StateStore blob-contract refactor
  - review order:
    - `minions/_internal/_framework/state_store.py`: review the new contract surface
      - `StoredWorkflowContext`
      - blob-based `save_context(...)`
      - required `get_contexts_for_orchestration(...)`
      - framework-owned `_save_context(...)` and decode helpers
    - `minions/_internal/_framework/minion_workflow_context_codec.py`: review the runtime serialization boundary
      - `serialize_persisted_workflow_context(...)`
      - `deserialize_workflow_context_blob(...)`
      - persisted-blob schema-version validation
    - `minions/_internal/_framework/state_store_sqlite.py`: review the main backend rewrite
      - `workflows(workflow_id, orchestration_id, context)` schema
      - orchestration index
      - blob batching
      - orchestration-scoped reads
    - `minions/_internal/_domain/minion.py`: review startup replay integration
      - `_get_contexts_for_orchestration(...)` replaces the old minion-scoped helper path
    - `minions/_internal/_framework/state_store_noop.py`: review the noop contract adaptation
    - `tests/assets/support/state_store_inmemory.py`: review the test-store move to the stored-row/blob model
    - `tests/minions/_internal/_framework/test_state_store_contract.py`: review the end-to-end contract expectations
    - `tests/minions/_internal/_framework/state_store_sqlite/`: review SQLite-specific backend expectations
    - `tests/minions/_internal/_domain/test_minion_states.py`: review the replay test update for the new persisted payload shape
    - `tests/support/gru_scenario/verify.py`: review the verifier rename from minion-context lookup to orchestration-context lookup
    - `tests/support/gru_scenario/tests/test_verify.py`: review the matching verifier test updates
    - `benchmarks/minion_workflow_context_persistence.py`: review the canonical blob-path benchmark coverage
    - `WORKFLOW_CONTEXT_PERF_PLAN.md`: review the running log and benchmark results for the refactor

- todo: audit the test suite to align with the default-backend + contract-test strategy
  - for each system component base class, ensure there is a contract test file
  - check higher-level/domain tests to ensure they use the in-memory system component by default
  - only keep non-default backends in a test when the backend-specific choice is intentional and clearly justified
  - document this test suite design in a test suite design doc

- todo: add first-class resume support and resume testing to the Gru scenario DSL
  - current coverage:
    - the DSL verifier can now validate persisted context snapshot integrity internally
    - verifier call-count expectations now account for replayed persisted workflow steps after restart
    - user-guarantee scenarios cover dict, dataclass, and msgspec.Struct persistence through stop, restart, and resume
  - gap:
    - scenarios still use coarse orchestration primitives like `AfterWorkflowStarts(..., OrchestrationStop(...))` to create a persisted checkpoint
    - there is no explicit DSL directive for stopping/resuming at a specific workflow checkpoint or step boundary
    - `OrchestrationStart` scenarios do not yet cover class-based orchestration starts or inline `minion_config`, so the newer `Gru.start_orchestration(...)` modes still rely on direct/manual tests
  - possible DSL shape:
    - `AfterWorkflowStepStarts(expected={minion_name: {"step_1": 1}}, directive=OrchestrationStop(...))`
    - `ExpectRuntime(..., expect=RuntimeExpectSpec(replayed_workflow_steps={...}))` only if the existing automatic replay counting is not enough
    - optional checkpoint selectors for asserting before/after restart behavior without relying on checkpoint indexes
  - tests:
    - prove restart resumes from `next_step_index` without replaying already-completed steps
    - prove replayed step counts and save/delete counts are deterministic across checkpoint windows
    - cover pause/drain/cutover stop-mode semantics once those modes exist
  - why it matters:
    - Gru's operational guarantee includes not losing in-flight workflow state across restart, and the DSL should be able to express that guarantee directly rather than only validating uninterrupted happy-path workflows.

- todo: harden test_gru.py:
  - steps:
    - complete the robust reusable Gru testing routine
    - rewrite Gru tests to use the routine
    - integrate Gru tests from other files:
      - test_minion_states.py is basically a Gru test where checking for counters should be handled by the reusable testing routine

- todo: ensure immediate user-facing domain objects (minion, pipeline, resource) and non-immediate user-facing domain objects (like StateStore, logger, metrics)...
  - 1: validate composition at class definition time and raise user friendly exception msg (good onboarding DX)
    - ex: tests/minions/_internal/_domain/test_minion.py
  - 2: validate usage at orchestration time (like in test_gru.py or one of its subdirectories if it got reorganized)
    - ex: tests/minions/_internal/_domain/test_gru.py::TestValidUsage.test_gru_accepts_none_logger_metrics_state_store
  - note: for non-immediate user-facing domain objects, validating composition at class definition time may not always be possible, so it's fine to only validate at orchestration time if that's the case

- todo: add "crash testing" to test suite to ensure the Minions runtime preserves its crash guarantees
  - status:
    - deterministic "boom user code" coverage has been added for the primary in-process user-code surfaces
    - `Gru.start_orchestration(...)` failed-start partial registration cleanup has been implemented and tested
  - remaining follow-ups:
    - add explicit mid-operation invariant tests for remaining public Gru methods, especially `stop_orchestration(...)` and `shutdown(...)`
    - decide and document whether pipeline/resource shutdown-hook failures during cancellation should make `stop_orchestration(...)` fail or remain log-only
    - add separate subprocess kill/restart tests for process-death resume guarantees; keep this separate from ordinary in-process exception containment
  - todo: add deterministic "boom user code" testing across every user-code runtime surface
    - goal:
      - pass intentionally exploding user code into every user-code entry point and prove the runtime contains the blast radius consistently
    - coverage:
      - minion user hooks / steps
      - pipeline user hooks / emit paths
      - resource user-defined methods
      - any other runtime surface that invokes user code directly or indirectly
    - assertions:
      - failures are routed through the safe user-code wrapper path
      - no call site bypasses the wrapper by reaching private implementation methods directly
      - runtime behavior is deterministic and preserves crash guarantees / error reporting
    - example bug to guard against:
      - accidentally calling `self._mn_state_store._save_context(ctx)` instead of the safe wrapper like `self._mn_state_store.save_context(ctx)`
    - implementation note:
      - audit every user-code invocation site and add tests that would "go boom" immediately if the runtime forgets to use the guarded path


### Features:
- todo: decide whether resource method metrics should use method names or stable method identities
  - problem:
    - `resource_method` currently uses method names, so renaming a method creates a new Prometheus series even when the logical operation did not change
  - options:
    - keep method names because they are readable and match source code
    - add an explicit stable method identity through a decorator or metadata field
    - keep both readable and stable labels only if the cardinality/compatibility tradeoff is acceptable
  - why it matters:
    - stable metric identities make long-lived dashboards and alerts less fragile during refactors

- todo: review `StoredWorkflowContext` naming and representation
  - problem:
    - the name embeds "state", but the object represents a stored workflow context row/blob that may be used across different lifecycle states
    - it is currently a dataclass; it is worth deciding whether a `msgspec.Struct` provides meaningful performance or serialization benefits here
  - options:
    - keep `StoredWorkflowContext` as-is because it is clear enough and simple
    - rename it to emphasize persisted row/blob semantics
    - convert to `msgspec.Struct` only if profiling or serialization boundaries justify it
  - why it matters:
    - this type is part of the custom StateStore contract, so naming and representation should be deliberate before the contract settles

- todo: simplify persisted workflow context decode path once payload typing guarantees are finalized
  - problem:
    - `deserialize_workflow_context_blob(...)` still supports untyped/fallback decode paths, but persisted payloads may reasonably be expected to have both event and context classes available
  - decision:
    - confirm whether all runtime persistence reads should always provide `event_cls` and `context_cls`
    - if yes, simplify the fallback logic and make missing type information an explicit compatibility path or error
  - why it matters:
    - narrowing the decode contract would make persistence behavior easier to reason about and reduce legacy codec branching

- todo: spec out opt-in freshness / staleness handling for time-sensitive events
  - problem:
    - startup gating now correctly delays live event handling until a minion is ready, but some domains may need to reject events that become stale before handling begins
  - open question:
    - Is this a runtime policy, or should users handle freshness inside workflow steps where domain-specific staleness rules are clearer?
  - goal:
    - keep the default runtime behavior as durable "handle when ready"
    - support opt-in freshness policies for users whose events carry deadlines / max-age requirements
  - design constraints:
    - do not silently drop delayed events by default
    - freshness must be explicit and user-controlled, not inferred from runtime timing
    - the runtime should not guess what "too late" means for arbitrary event types
  - design options to evaluate:
    - minion-level policy like `max_event_age_seconds`
    - event-carried deadline / created-at timestamp checked at handle time
    - user hook for stale-event handling (`drop` / `abort` / custom callback)
  - tests:
    - add coverage that default behavior still handles delayed events
    - add coverage for opt-in stale-event rejection once semantics are chosen
  - docs:
    - document delivery-vs-freshness tradeoffs clearly so users do not assume immediate-on-create handling guarantees

- todo: add first-class logger system telemetry (`logger_*`)
  - goal:
    - expose logger health and performance as system telemetry, separate from domain/business metrics
    - treat logger as a pluggable infrastructure component like StateStore, while starting with a smaller generic metric surface
    - keep metric names backend-agnostic and labels low-cardinality
  - metrics to add:
    - `logger_writes_total` (counter)
    - `logger_write_failures_total` (counter)
    - `logger_write_duration_seconds` (histogram)
    - `logger_payload_bytes` (histogram)
  - labels/policy:
    - allowed labels:
      - `backend` (e.g. `file`, `prometheus`, `noop`)
      - `operation` (`write`)
      - `error_type` (for `logger_write_failures_total` only)
    - do not include high-cardinality labels (no workflow ids/minion ids/payload content)
    - telemetry emission must be best-effort and never block runtime paths
    - avoid a `framework_` prefix because user-provided loggers are still first-class infrastructure inside a Minions system
  - implementation steps:
    - add constants + label mappings in `metrics_constants.py`
    - instrument logger write path(s)
    - defer deeper backend-specific metrics such as queue depth, flush duration, and dropped records until logger implementations need them
  - tests:
    - add focused tests that metrics are emitted for logger success paths
    - add focused tests that logger error counters increment on failure paths
    - ensure no unexpected labels are emitted
  - docs:
    - update docs to explain domain telemetry vs system/infrastructure telemetry split
    - document the new generic logger metric names and label policy
    - note that dashboard authors can ignore exposed metrics visually, but scraped metrics still cost storage unless scrape config or relabeling drops them

- todo: add first-class StateStore backend/system telemetry (`state_store_*`)
  - goal:
    - expose StateStore health and performance as backend/system telemetry, separate from workflow durability/guarantee telemetry
    - use `minion_workflow_persistence_*` metrics for workflow checkpoint impact and `state_store_*` metrics for backend cause/health
    - keep labels low-cardinality and backend-oriented
  - metrics to add:
    - `state_store_operations_total`
    - `state_store_operation_failures_total`
    - `state_store_operation_duration_seconds`
    - `state_store_payload_bytes`
    - `state_store_queue_depth`
    - `state_store_batch_flush_duration_seconds`
    - `state_store_commit_duration_seconds`
  - labels/policy:
    - allowed labels:
      - `backend` (ex: `sqlite`, `noop`, `inmemory`)
      - `operation` (ex: `save` | `delete` | `load` | `flush` | `commit`)
      - `error_type` (for failure counters only)
    - do not include workflow ids, minion ids, orchestration ids, checkpoint names, or payload contents
    - metrics emission must be best-effort and must not block persistence paths
  - implementation steps:
    - add metric constants and label names in `metrics_constants.py`
    - instrument generic StateStore wrapper paths for operations, failures, duration, and payload size
    - instrument SQLite-specific queue depth, batch flush duration, and commit duration
    - keep workflow-level persistence telemetry in `minion_workflow_persistence_*`, not `state_store_*`
    - do not add parallel `framework_state_store_*` metrics; this todo owns the generic StateStore telemetry surface
  - tests:
    - verify generic save/delete/load operation counters and durations are emitted
    - verify failure counters include `error_type`
    - verify SQLite queue/batch/commit metrics are emitted without workflow-level labels
    - ensure metric emission failures do not affect StateStore behavior
  - docs:
    - document the split between workflow persistence impact metrics and StateStore backend/system metrics
    - document recommended operator usage:
      - alert on `minion_workflow_persistence_*` for workflow durability impact
      - inspect `state_store_*` for backend health and root cause

- todo: wire `MINION_WORKFLOW_STEP_INFLIGHT_GAUGE` into `minion.py`
  - keep the step-level gauge as a live execution-state complement to step duration and outcome counters
  - update the gauge in the step start/finally paths once the wiring is ready

- todo: add family-level metrics exposure controls
  - goal:
    - let operators disable or exclude whole metric families when they do not want Prometheus to ingest that runtime surface
    - keep the default behavior observability-first so users get the full low-cardinality runtime picture without extra setup
  - families to consider:
    - `logger_*`
    - `state_store_*`
    - `minion_workflow_persistence_*`
    - other future framework/system families
  - design constraints:
    - prefer family-level include/exclude controls over per-metric toggles to avoid excessive configuration surface
    - disabling a family should prevent exposing those series at the metrics endpoint, not merely hide them from dashboards
    - defaults should expose all stable low-cardinality metric families
    - telemetry emission must remain best-effort and must not block runtime paths
  - implementation steps:
    - decide where metrics exposure config belongs (global runtime config, Prometheus exporter config, or both)
    - add allow/deny family filtering before metrics are exported
    - document how exporter-side filtering differs from Prometheus scrape/relabel filtering
  - tests:
    - verify excluded families are not exposed on the Prometheus endpoint
    - verify included families continue to emit with their normal label policy
    - verify unknown family names are rejected or ignored consistently
  - docs:
    - recommend exposing all stable low-cardinality runtime metrics by default
    - explain that family-level controls are mostly for operators with storage, policy, or relevance constraints

- todo: stabilize workflow persistence and recovery semantics
  - implementation order:
    - refine configurable workflow persistence failure policy after first implementation
    - decide state-store availability semantics for Gru start/stop commands
    - add first-class recovery handling for undecodable persisted workflow contexts

  - todo: refine configurable workflow persistence failure policy after first implementation
    - problem:
      - the first pass added explicit persistence failure policies, retry behavior, logs, and metrics
      - remaining design work is about API placement, stop-mode interaction, and operational clarity
    - follow-ups:
      - decide whether policies should be configured globally on Gru, per Minion, or both
      - clarify how `continue-on-failure` should report unresolved failed checkpoints when stop/redeploy modes are added
      - keep policy interaction with durable vs eventual StateStore semantics explicit
    - linked follow-up:
      - pair this with explicit workflow step skip results so idempotent step guards are visible in logs and runtime receipts
    - tests:
      - add stop-mode-specific tests once `drain`/`cutover` exists
      - add tests for any future per-minion policy override behavior
    - docs:
      - keep documenting the policy tradeoff between availability and durable progress guarantees

  - todo: decide state-store availability semantics for Gru start/stop commands
    - problem:
      - state-store read failures during resume now correctly fail closed instead of returning an empty resume set
      - workflow checkpoint write behavior is now explicitly operator-configurable via `WorkflowPersistenceFailurePolicy`, but startup/replay behavior still has fixed semantics
      - the remaining design gap is to decide whether startup component availability, persisted-context replay reads, and degraded recovery behavior should live under a separate recovery policy or under a broader persistence/durability policy surface
      - this is correct as a safety baseline, but the broader command semantics are not fully designed:
        - should `start_orchestration` fail when persisted resume state cannot be read, or start in a degraded recovery-pending mode?
        - should `stop_orchestration` always remain live-process control, or block/warn/require force when persistence health makes durable recovery unsafe?
      - `WorkflowPersistenceFailurePolicy` currently describes checkpoint/write behavior for live workflows, not recovery reads or command safety
    - design principle:
      - commands can control the live process without the state store, but they must not claim durable recovery safety unless the state store has confirmed it
      - never collapse "state store unavailable" into "no persisted workflows to resume"
      - distinguish command application from durability/recovery guarantees in user-facing results
    - start_orchestration design questions:
      - current conservative behavior: a resume read failure causes start to fail and Gru cleans up partial runtime state
      - decide whether state-store startup/bootstrap failure should remain unconditionally fatal, or become part of the same operator-facing durability/recovery policy model as persisted replay reads
      - possible future behavior: start the orchestration in a degraded state with recovery pending, retry persisted-context reads in the background, and allow new workflows according to `WorkflowPersistenceFailurePolicy`
      - if degraded start is supported, add explicit runtime state such as `recovery_pending` / `state_store_unavailable` / `partial_recovery`
      - decide whether new workflow admission should be allowed before persisted recovery completes, and how to avoid duplicate or out-of-order work if recovery later succeeds
    - stop_orchestration design questions:
      - make stop modes persistence-aware once `pause` / `drain` / `cutover` exist
      - `drain` can usually proceed with warnings because live in-memory workflows are allowed to finish
      - `pause` / persisted handoff should require durable checkpoint confidence or report degraded persistence risk
      - `cutover` / immediate interruption should warn strongly or require `force=True` when state-store health is bad or unpersisted workflows exist
      - decide how stop results should report unresolved failed checkpoints under `continue-on-failure`
    - result/API shape:
      - consider adding structured warning/status fields to start/stop results rather than overloading `reason`
      - possible fields:
        - `applied: bool`
        - `state_store_available: bool`
        - `recovery_status: Literal["complete", "pending", "not_required", "failed", "partial"]`
        - `persistence_risk: Literal["none", "degraded", "possible_workflow_loss"]`
        - `warnings: tuple[str, ...]`
      - keep string `reason` for failure summaries, but make degraded success machine-readable
    - observability:
      - log state-store availability transitions and command-level persistence risk with stable kwargs
      - add metrics for recovery-pending/degraded starts and stop commands that proceed with persistence risk
      - make GruShell surface warnings before dangerous operations and require explicit confirmation/force for high-risk cutover paths
    - tests:
      - keep the current regression that read failure during `start_orchestration` fails closed and cleans up runtime state
      - if degraded start is added, prove start returns applied-with-warning, recovery retries continue, and persisted workflows are not silently abandoned
      - prove stop `drain` / `pause` / `cutover` report different persistence risks when the state store is unavailable
      - prove forced cutover is explicit and observable when persistence risk is present
    - docs:
      - explain the distinction between live-process control and durable recovery guarantees
      - document how `WorkflowPersistenceFailurePolicy` relates to command behavior without overloading it to mean recovery-read policy
      - document the remaining semantic split clearly until it is unified: checkpoint save/delete policy is configurable, while startup bootstrap and replay currently remain stricter/fixed

  - todo: add first-class recovery handling for undecodable persisted workflow contexts
    - problem:
      - StateStore read failures during resume must fail closed because an empty result means "no persisted in-flight workflows", while a failed read means "unknown persisted state"
      - decode failures are more nuanced: one bad persisted workflow context should not necessarily prevent unrelated decodable workflows from resuming
      - current best-effort decode behavior only logs bad contexts, so an undecodable in-flight workflow can become stranded without a first-class blocked/recovery-failed state for operators to inspect or alert on
    - desired default:
      - long-term default should be best-effort recovery once it is implemented as a real operational feature
      - decodable workflow contexts resume independently
      - undecodable workflow contexts are marked as blocked/recovery-failed/quarantined instead of being silently skipped
      - strict/all-or-nothing recovery remains available for users who need orchestration-wide atomic resume semantics
    - policy shape:
      - consider a recovery/decode policy separate from `WorkflowPersistenceFailurePolicy`, since persistence failures happen while a live workflow is checkpointing and decode failures happen during recovery
      - possible policy names:
        - `WorkflowRecoveryDecodeFailurePolicy = Literal["resume-decodable", "fail-orchestration"]`
      - do not add public configurability until the blocked/recovery-failed operational surface exists; otherwise `resume-decodable` would mostly mean "log and skip"
    - observability:
      - add a durable/operator-visible representation for undecodable workflow contexts
        - include `workflow_id`, `orchestration_id`, state store backend, error type/message, and first-seen timestamp
        - avoid logging or storing raw payload contents
      - add metrics such as `minion_workflow_recovery_decode_failed_total`
      - ensure Gru/GruShell can list recovery-failed workflows and provide an operator action path to drop, retry, or migrate them
      - make startup/resume logs clearly distinguish full recovery from partial recovery
    - tests:
      - prove read failures still fail closed and do not look like an empty resume set
      - prove best-effort recovery resumes decodable contexts while recording undecodable contexts as recovery-failed
      - prove strict recovery fails the affected orchestration when any persisted context for that orchestration cannot decode
      - prove recovery-failed contexts are visible through logs, metrics, and the chosen runtime/operator inspection surface
      - prove unrelated orchestrations are not blocked by a recovery failure outside their scope
    - docs:
      - explain the difference between state-store read failure, decode failure, and no persisted contexts
      - document why best-effort recovery is the intended default once recovery-failed workflows are first-class state
      - document when users should choose strict recovery instead

- todo: add explicit workflow step skipped result for idempotent early returns
  - problem:
    - workflow steps must be idempotent, so authors often need early-return guards when a step has already run or no work is required
    - returning nothing makes an intentional skip hard to distinguish from a silent no-op in logs and runtime inspection
  - target:
    - introduce `MinionWorkflowStepSkipped(reason: str)` as an explicit step result
    - allow workflow steps to return `MinionWorkflowStepSkipped("...")` when an idempotency guard skips the step
    - record the skip reason in logs and any workflow receipts/step outcome surfaces
    - keep skip semantics distinct from failure, abort, and successful side-effectful completion
  - linked follow-up:
    - use this as the observable companion to workflow persistence failure retries, since replayed steps may intentionally skip already-completed work
  - tests:
    - verify a skipped step advances workflow progress according to normal step sequencing
    - verify the skip reason appears in logs/receipts
    - verify skipped steps remain compatible with retry/resume behavior
  - docs:
    - update idempotent workflow step guidance with an early-return example that returns `MinionWorkflowStepSkipped("already processed")`

- todo: add memory-pressure guard to manage OOM risk (high-utilization defaults)
  - goal:
    - avoid hard OOM kills while still letting a single-node minions system run “hot” (stable high RAM usage)
    - prevent flapping/thrashing when crossing thresholds
  - design:
    - runtime maintains a small state machine driven by sampled memory usage:
      - OK: normal
      - SHED: memory pressure is sustained; runtime sheds load
    - detection:
      - poll memory usage at a fixed interval (e.g. 250ms) and smooth across a short window (N samples / EMA)
      - enter SHED only after `enter_consecutive` samples at/above `shed_ratio`
      - exit SHED only after:
        - a minimum `cooldown_s` elapsed since entering SHED, and
        - `exit_consecutive` samples at/below `resume_ratio`
      - defaults are intentionally “peg high”:
        - shed late (e.g. 0.94) and resume slightly below (e.g. 0.92) so system naturally stays near the top without bouncing down to 80%
    - actions while in SHED:
      - reject new `start_orchestration` requests (return a typed failure / reason = `memory_pressure`)
      - suspend pipelines from admitting new work (soft suspend: don’t kill in-flight work; just stop new scheduling/emit)
      - log pressure state with rate limiting (avoid spam)
    - container vs host measurement:
      - support `mode="auto"` that prefers cgroup limits when available, otherwise host memory
      - if measurement fails, degrade safely (disable guard + log once), do not brick runtime
    - config surface (minimal, user-overridable):
      - `MemoryPressureCfg(shed_ratio, resume_ratio, enter_consecutive, exit_consecutive, cooldown_s, poll_interval_s, enabled=True, mode="auto")`
      - keep defaults aggressive/high-util; users can set conservative ratios if they want more headroom
  - implementation notes:
    - wire guard state into `Gru.start_orchestration(...)` admission path
    - wire guard state into pipeline scheduling / emit admission path (one choke point; no scattered checks)
    - keep all internal fields in `_mn_` attrspace
    - need to log/notify the user when state changes (and what that means) but not too often
  - tests:
    - test: sustained high memory enters SHED and rejects new `start_orchestration`
    - test: pipelines stop admitting new work in SHED (no new tasks/events created), but in-flight work continues
    - test: anti-flap works (bouncy samples around shed threshold do not repeatedly enter/exit)
    - test: cooldown is respected (drops below resume_ratio immediately still stays SHED until cooldown elapsed)
    - test: recovery resumes admission after sustained low memory (exit_consecutive satisfied)
    - test: measurement failure degrades safely (guard disabled, runtime continues)
  - docs:
    - explain the philosophy: “single-node, maximize useful work; default runs hot; sheds only when sustained pressure risks OOM”
    - document config knobs and recommended conservative profile for container/shared-host deployments
  - convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/6945f1d2-cdcc-832e-9ce6-a12dfd906992
  - other convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/6939c2bf-5d7c-8332-b9a7-6baa836491f8

- todo: add event-loop lag monitor & notification (spec decently enough to polish & implement later)
  - goal:
    - detect sustained asyncio loop stalls early; surface precise blame (minion/workflow/step + top frame); auto-pause intake on severe lag; never force users to change their plain `await` code
  - design:
    - sampling loop (0.5–1.0s) computes lag: `lag_ms = max(0, (now - target) - 1000)`
    - thresholds & hysteresis:
      - WARN when `lag_ms >= 100` (log offenders once per sample with rate limiting)
      - CRIT when `lag_ms >= 500` sustained for ≥5s → set intake state to `PAUSE`
      - RESUME when `lag_ms < 100` sustained for ≥10s → set intake state to `OK`
    - attribution:
      - track INFLIGHT tasks created by `@minion_step` wrapper; set `task.set_name(f"{minion}:{wf}:{step}")`
      - on WARN/CRIT, capture up to N offenders by longest `elapsed_s` and include top stack frame (`filename:lineno func`)
    - actions:
      - WARN → structured log + metrics only
      - CRIT → call existing intake pause path (same choke point used by memory guard); do not cancel tasks
      - RESUME → resume intake; drain any on-disk spool first (if present)
    - config surface (minimal, env/kwargs):
      - `LagCfg(sample_s=0.5, warn_ms=100, crit_ms=500, crit_sustain_s=5, resume_ms=100, resume_sustain_s=10, max_dump=5, enabled=True)`
    - DX stance:
      - no per-step timeouts by default; users keep plain asyncio
      - docs instruct offloading CPU/blocking work to `ProcessPoolExecutor` / `asyncio.to_thread`
  - implementation notes:
    - add lightweight `loop_lag_monitor()` task started with Gru; keep state in `_mn_` attrspace
    - extend `@minion_step` wrapper to register/unregister tasks in an `_mn_inflight` set + `started_at`
    - logging:
      - single structured line per sample at WARN/CRIT with `lag_ms`, `intake_state`, and `offenders=[{task,elapsed_s,top}]`
      - rate-limit identical WARN lines (e.g., suppress duplicates within 1s)
    - metrics (Prometheus):
      - `event_loop_lag_ms` (gauge)
      - `loop_lag_warnings_total` (counter)
      - `intake_state{state="ok|pause"}` (gauge 0/1)
      - optional: `step_runtime_seconds` (histogram, per minion/step labels) if not already present
    - intake integration:
      - reuse existing memory-pressure intake controller; add a “lag” reason to enter/exit `PAUSE`
      - ensure PAUSE/RESUME are idempotent and reason-agnostic (memory or lag can trigger)
  - tests:
    - unit: simulate lag by sleeping the event loop in a helper; assert WARN then CRIT transitions with hysteresis
    - unit: register fake INFLIGHT tasks with names/stacks; assert offender dump ordering by `elapsed_s`
    - integration: CRIT → intake paused; sustained recovery → intake resumed; verify no new tasks created during pause
    - logging: snapshot one WARN line (redact paths if needed), assert fields present and rate-limited
  - docs:
    - section “Event-loop lag”: what it means, how it’s surfaced, why bots may stall; how to fix (offload CPU/blocking I/O)
    - clarify policy: signal-only by default; no auto-cancels; intake pauses on severe sustained lag to protect the system
  - convo: https://chatgpt.com/g/g-p-6854f3157968819196393751e67bd218-minions-python-oss-framework/c/689e81a1-4f78-832f-8a7b-6d2f4fb5973d

- todo: revisit convo https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/68f6c4f3-888c-8333-964d-be052cd06ea1

- todo: stabilize orchestration lifecycle and identity
  - done: make orchestration the canonical Gru lifecycle API surface
    - problem:
      - the old minion-named start API read as if Gru only started a minion, but the operation materializes a live orchestration from a pipeline, minion, and minion config
      - starting that orchestration also validates the declared composition, starts or reuses the required pipeline and resources, subscribes the minion, and lets workflows be produced as events arrive
      - naming the public lifecycle operation after only the minion hides the runtime unit users need to reason about when they inspect, stop, resume, or operate the system
    - direction:
      - make `Gru.start_orchestration(...)` and `Gru.stop_orchestration(...)` the canonical lifecycle verbs
      - alias `Gru.start(...)` and `Gru.stop(...)` to the canonical orchestration lifecycle operations for convenience
      - prefer the start shape `pipeline, minion, minion_config` for the orchestration API, with docs using keyword arguments so the relationship is explicit
      - keep Minion terminology focused on the user-authored workflow factory/behavior definition while orchestration names the live binding Gru owns
      - done: replaced the public minion-named lifecycle API with orchestration-named lifecycle methods rather than keeping both concepts indefinitely
      - implementation choice:
        - done: removed compatibility wrappers in the canonical API pass because call-site and doc churn was straightforward enough that wrappers only prolonged a misleading API
    - update together:
      - README, docs, examples, tests, shell/scenario helpers, result naming, and lifecycle terminology should teach orchestration as the live runtime unit consistently
      - align this with the orchestration identity terminology cleanup so lifecycle names, ids, logs, metrics, and persistence concepts do not drift
    - why it matters:
      - an accurate lifecycle name gives users the right mental model without requiring them to manage component startup details themselves

  - done: settle official terminology for orchestration identity
    - problem:
      - the codebase previously used `minion_composite_key` and `orchestration_id` in closely related or interchangeable ways
      - persistence rows stored `orchestration_id`, while telemetry and runtime labels exposed `minion_composite_key`
      - this creates ambiguity for API docs, metric interpretation, and future migration work
    - decision:
      - `orchestration_id` is the official concept name for the stable minion/config/pipeline orchestration identity
      - `minion_composite_key` should not remain a public, persisted, metric, log, or long-lived internal identity term
      - code, docs, logs, metrics labels, workflow context, persistence blobs, and test helpers have been aligned to `orchestration_id`
    - why it matters:
      - unclear identity terminology can become a compatibility problem for users who build alerts, dashboards, stored state tools, or operational docs around these names

  - todo: decouple orchestration addressing from stable runtime identity with source-anchored ids
    - context:
      - current identity spine is “project-root relative path/modpath”
      - this breaks resumability + Prometheus continuity on directory refactors
      - long-term guarantee requires stable ids not derived from paths
    - goals:
      - preserve current DX through the orchestration API: orchestration starts accept component classes or string refs
      - make inflight workflow resume + reuse + metrics stable across refactors
      - let normal component/config renames and relocations preserve identity without forcing users to maintain a registry/manifest by hand
    - decisions:
      - make durable component identity source-anchored:
        - `@pipeline_id(...)`, `@minion_id(...)`, and `@resource_id(...)` carry stable ids with user classes across module renames and relocations
        - a small CLI/source-editing helper can stamp ids so the user pays a one-time explicit cost instead of hand-writing UUIDs
      - make durable file config identity artifact-anchored:
        - stable config slot id lives in a reserved `_mn_config_id` entry prepended to the config file
        - config file moves preserve the slot id because the id moves with the config artifact
        - `_mn_config_id` must not accidentally leak into the config mapping exposed to user minion code
          - _mn_config_id is the right config name or should be changed? `minion_config_id` maybe looks less internal?
      - refs and paths remain for loading / addressing / diagnostics:
        - string refs continue to work as “how to import the class”
        - config paths continue to work as “where to load the config file now”
        - source/config ids become “how the runtime persists and labels the durable thing”
      - registry/manifest support is secondary, not the primary refactor-survival mechanism:
        - it may provide aliases, CLI inspection, duplicate-id diagnostics, discovery/indexing, and last-seen refs
        - durable identity must survive the common move/rename path without requiring a registry relink
      - keep method-level observability separate from durable component identity:
        - minion step names and resource method names may remain readable metric labels by default
        - if dashboard continuity across method renames warrants it, prefer an optional stable semantic metric name over default method-level UUIDs
    - steps:
      - define the identity vocabulary before changing persistence/API surfaces:
        - `component_id` = stable identity for each durable domain component class
        - `config_slot_id` = stable identity for the durable config slot/artifact, independent of config path or config content revision
        - `orchestration_id` = stable identity for the current orchestration definition built from stable component/config ids
        - `minion_composite_key` has been replaced by `orchestration_id` in the current runtime terminology
        - keep a distinct name for one live process-local minion start (`runtime_instance_id` / `run_id` / final chosen term)
        - do not overload current `instance_id` terminology with both a durable orchestration identity and a live runtime start identity
      - add source-level component id metadata:
        - expose public id decorators for durable component classes (`@pipeline_id(...)`, `@minion_id(...)`, `@resource_id(...)`)
        - validate id shape and component kind at class definition time
        - detect duplicate/conflicting loaded ids clearly
        - keep components without explicit ids available for non-durable/prototyping usage, but do not promise refactor-stable resume identity for them
      - add config slot id metadata:
        - define `_mn_config_id` parsing and prepending behavior for supported config file formats
        - define the identity extraction boundary before normal minion config loading, including how `_mn_config_id` works when users override `Minion.load_config(...)`
        - define whether id-less file configs are rejected for durable mode, assigned by a CLI helper, or retain only refactor-fragile fallback identity
        - record a normalized config content digest separately from the stable config slot id
      - define canonical “component ref” format for addresses and diagnostics:
        - prefer `module:qualname` (plus `kind` prefix internally if needed)
        - store refs/paths where needed for loading and debugging, but never make them the durable persistence identity when ids are available
      - update Gru resolution pipeline:
        - align this work with the orchestration lifecycle API todo above so `start_orchestration(...)` is the canonical identity entrypoint
        - when orchestration start receives the current component classes/refs/config again after a restart, resolve current addresses and stable ids from the loaded artifacts
        - moved source/config artifacts with unchanged ids compute the same durable orchestration identity without registry maintenance
        - make durable reuse/resume keys use stable ids, not relpaths/modpaths/config paths
        - decide id behavior for inline `minion_config` mappings: content digest, explicitly non-durable identity, or an explicit config slot id API
      - define orchestration identity from the current orchestration model:
        - orchestration is currently the stable wiring of pipeline + minion + minion config slot
        - `orchestration_id = hash(pipeline_component_id + minion_component_id + config_slot_id)` unless the public model chooses an explicit orchestration id later
        - add resource binding ids only if/when resource binding choices become orchestration identity inputs
        - reuse policy:
          - pipelines reuse on stable pipeline identity unless pipeline configuration later makes that model insufficient
          - minion orchestration reuse/resume follows the durable `orchestration_id`
          - live start ids remain distinct from durable orchestration identity
      - define config revision behavior for Model B before relying on it:
        - stable `config_slot_id` preserves deployment-slot identity across file moves and content updates
        - normalized config digest records the content revision
        - decide whether in-flight workflows resume under the latest config slot contents, require explicit cutover/migration handling, or retain another revision contract
      - update persistence + resume:
        - change workflow state keys to use durable ids (never component refs or config paths when stable ids exist)
        - keep baseline resume semantics aligned with current architecture: recovery occurs when the orchestration is declared/started again with the moved artifacts
        - decide what extra index/discovery support is required only if Minions later promises auto-resume of persisted workflows before orchestration redeclaration
        - add migration shim:
          - if old state keys exist (path-based), attempt best-effort mapping to new ids or mark orphaned with a clear error
      - update prometheus labeling:
        - stable labels:
          - durable `component_id` / `orchestration_id` fields chosen by the terminology cleanup
        - human/debug labels:
          - `component_ref`
          - current readable minion/pipeline/config labels where cardinality and compatibility permit
        - ensure dashboards can be written against stable labels
      - define type / class persistence as part of this identity model:
        - this work also decides how persisted Python `type` references are serialized and resolved
        - `MinionWorkflowContext.context_cls` is the current concrete example
        - keep runtime/domain objects Python-native; persistence adaptation belongs at the framework boundary
        - persisted state must not rely solely on current import path stability
        - decide whether persisted type references store `component_id`, current `component_ref`, or both
        - support migration after old path-based persisted state without making persisted workflows unusable
        - UX should remain simple while file structure changes are happening, not only after everything is stable again
      - add CLI/tooling support (minimal but sufficient):
        - stamp missing component/config ids intentionally instead of asking users to hand-write UUIDs
        - inspect/validate durable ids and show duplicate/conflicting ids
        - expose optional alias/index/listing support only where it reduces operational friction
      - document the new contract:
        - “refs and config paths can change; ids persist with source/config artifacts”
        - “normal component/config moves keep durable orchestration identity when the same id-bearing artifacts are started again”
        - “id-less/prototype artifacts do not receive the same refactor-stable durability guarantee”
        - “durable observability uses stable identity labels; step/resource method labels remain readable operation labels unless an explicit semantic metric name is added”
    - convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/69446e9e-053c-832c-abfb-ba40b5123693
    - other convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/694725cf-a5c4-8326-bdfc-b95f1b289f14
    - note: consider how cross env (dev,qa,prod) comparison will work: like with grushell snapshot/redeploy, discussed in "other convo"
    - note: `orchestration_id` is now the current runtime identity term; future durable-id work should preserve this name while changing how the value is derived

  - done: audit the todos in todo.md to align them to the new Gru.start / Gru.stop API
  
  - todo: optimize SQLiteStateStore orchestration-scoped context reads
    - shape:
      - current schema shape:
        CREATE TABLE workflows(
            workflow_id TEXT PRIMARY KEY,
            orchestration_id TEXT NOT NULL,
            context BLOB NOT NULL
        )
        CREATE INDEX idx_workflows_orchestration_id
        ON workflows(orchestration_id);
    - dependency:
      - do this only after the stable identity migration above is finalized (`component_id` / `config_slot_id` / `orchestration_id`)
    - problem:
      - current path effectively scans all stored workflow contexts, then filters in memory
      - this is unnecessary read amplification once stable ids are in place
    - target:
      - query by minion identity/workflow key directly from sqlite (no full table scan)
      - keep current public behavior unchanged
    - tests:
      - add/adjust tests to prove behavior matches current semantics while using indexed lookup

- todo: mature Gru lifecycle failure management
  - context:
    - `docs/concepts/gru-lifecycle-failure-management.md` captures the current cleanup model and the boundary between Gru-owned framework state and user-owned process state
    - the current implementation hardens failed start, stop, and shutdown cleanup, but the API/result surface and long-term diagnostics still need deliberate design
  - implementation order:
    - formalize Gru stop commit/fail-closed semantics
    - refine lifecycle result contracts
    - design lifecycle leak-check tooling and cookbook
    - add optional lifecycle diagnostics

  - todo: formalize Gru stop commit/fail-closed semantics
    - define the point where `stop_orchestration(...)` becomes committed and cannot be rolled back to a running orchestration
    - keep tests proving committed stop failures remove active routing and framework-owned runtime state
    - document which failures are primary operation failures versus secondary cleanup/finalization failures
    - why it matters:
      - stop is not transactional once detachment begins, so the code and tests need explicit fail-closed semantics

  - todo: refine lifecycle result contracts
    - distinguish operation failure from framework cleanup status and possible user-owned process pollution
    - consider structured fields for cleanup phases, cancelled tasks, degraded cleanup, and restart recommendations
    - preserve the simple success/failure path while making degraded cleanup machine-readable for operators and callers
    - apply the result model consistently to `start_orchestration(...)`, `stop_orchestration(...)`, and `shutdown()`
    - why it matters:
      - current `success=False` results are too coarse for lifecycle failures and make it hard for callers/operators to react correctly

  - todo: design lifecycle leak-check tooling and cookbook
    - goal:
      - give users a practical way to find likely lifecycle cleanup leaks in the single-process runtime model without pretending Gru can prove process cleanliness
    - utility flow:
      - start an orchestration or component lifecycle
      - optionally exercise it briefly
      - stop it
      - let the event loop settle and force garbage collection
      - measure process memory, live tasks, and retained component objects
      - repeat for many iterations
      - report suspicious monotonic growth or retained runtime objects
    - layers:
      - individual component checks that repeatedly exercise minion, pipeline, or resource startup/shutdown with the least framework context needed
      - minimal orchestration checks that use the smallest valid Gru orchestration containing the target component
      - scenario checks that repeatedly start, exercise, and stop realistic orchestrations to catch workload-dependent leaks
    - likely report fields:
      - iteration count and configured settle/gc behavior
      - process memory delta and monotonic-growth signal
      - live asyncio tasks introduced during the run
      - retained weakref-tracked component instances after stop
      - top `tracemalloc` allocation deltas when enabled
      - recommended component/hook areas to inspect
    - cookbook:
      - show how to run the check against a resource with a background task
      - show how to run the check against a full orchestration scenario
      - explain that this detects likely leaks, not proof of leak freedom
      - explain when process restart is still the only hard cleanup boundary
    - tests:
      - leak-check utility passes for known-good component/orchestration assets
      - utility flags retained component references
      - utility flags un-cancelled background tasks
      - utility flags monotonic memory growth when `tracemalloc`/memory measurement is enabled
      - utility result remains stable enough for CI without relying on exact byte counts
    - why it matters:
      - Minions runs in-process, so users need practical diagnostics for cooperative lifecycle cleanup issues

  - todo: add optional lifecycle diagnostics
    - consider memory deltas, `tracemalloc` snapshots, live asyncio task inspection, and weakref collectability checks
    - decide which diagnostics belong in one-off leak-check tooling versus runtime debug logging
    - keep diagnostics opt-in so normal lifecycle guarantees do not depend on heavyweight inspection
    - why it matters:
      - diagnostics help debug user-owned cleanup leaks without changing Gru's core cleanup guarantee

- todo: add stop modes to gru.stop_orchestration and map to GruShell redeploy strategies
  - default mode: pause (current behavior)
  - modes and behavior:
    - pause: stop new workflows; persist in-flight workflows for resume on restart
    - drain: stop new workflows; await in-flight workflows to finish; then stop
    - cutover: stop new workflows; abort in-flight workflows; then stop
  - interruptibility:
    - if drain is in progress and caller requests cutover or pause, override drain immediately
    - if drain is requested again, treat as idempotent
    - if orchestration already stopped, return success with reason
  - api sketch:
    - gru.stop_orchestration(orchestration_id, mode="pause"|"drain"|"cutover")
    - grushell redeploy maps:
      - redeploy drain -> stop_orchestration(mode="drain")
      - redeploy cutover -> stop_orchestration(mode="cutover")
  - tests:
    - add orchestration helper directives for mode="drain"/"cutover"
    - add runtime tests for in-flight workflow behavior per mode
  - open questions:
    - Should `continue-on-failure` track outstanding failed checkpoints instead of only retrying when the workflow reaches the next checkpoint?
    - Should `WorkflowPersistenceFailurePolicy` remain Gru-level, become Minion-level, or support both global defaults and per-minion overrides?
    - The desired guarantee is that `Gru.stop_orchestration(...)` should not lose workflow state in any stop mode unless the process is force-interrupted.
  - why it matters:
    - failed checkpoint state affects the exact semantics of `drain` and `cutover`, especially when a workflow has progressed beyond its last durable checkpoint.

- todo: add Minion `max_inflight_workflows` class attr for bounded lossy workflow admission control
  - goal:
    - protect the process from a noisy minion/orchestration creating unbounded workflow tasks
    - keep the default runtime greedy and backwards-compatible
    - avoid introducing queueing/event-backlog semantics in this pass
  - api:
    - unlimited/default minions do not need to declare anything:
      - `max_inflight_workflows: int | None = None`
      - `overflow_policy: Literal["reject"] | None = None`
    - bounded minions must declare both attrs explicitly:
      - `max_inflight_workflows = 100`
      - `overflow_policy = "reject"`
  - validation:
    - `max_inflight_workflows is None` means unlimited/current behavior
    - if `max_inflight_workflows` is set, it must be a positive int
    - if `max_inflight_workflows` is set, `overflow_policy` is required
    - if `overflow_policy` is set while `max_inflight_workflows is None`, raise a user-friendly class/usage error
    - for now, the only valid overflow policy is `"reject"`
  - behavior:
    - pipelines keep producing and fanning out normally
    - each minion enforces its own admission limit independently
    - if the minion is at/above `max_inflight_workflows`, reject the event for that minion
    - do not create a workflow context for rejected events
    - do not persist rejected events
    - do not queue rejected events
    - do not treat rejection as a workflow failure, because no workflow started
    - do not backpressure the pipeline or affect other minions subscribed to the same pipeline
  - observability:
    - add a rejected-event/workflow-admission metric, e.g. `minion_workflow_rejected_total`
    - include minion/orchestration labels consistent with existing minion workflow metrics
    - emit structured logs for rejected events with the configured cap and current inflight count
  - docs:
    - document this as bounded lossy admission control / overload protection
    - explicitly say it is not fairness, event delivery, backpressure, or durable queueing
    - explain that resource semaphores still protect dependencies, while this cap protects the runtime from task growth
  - tests:
    - unlimited minion preserves current greedy behavior
    - bounded minion accepts events below the threshold
    - bounded minion rejects events at threshold without creating/persisting workflow context
    - rejection increments the metric and logs useful structured context
    - rejection does not affect another minion subscribed to the same pipeline
    - invalid class attrs raise clear errors
  - codex://threads/019ca819-0afe-7591-b59f-53d06718a48b

- todo: add bounded start concurrency to Gru (`max_concurrent_starts`)
  - goal:
    - prevent starts storms (ex: 50+ concurrent calls to Gru.start_orchestration) from overloading event loop / NAS / host I/O
    - keep starts throughput high but controlled and predictable
  - api:
    - add kwarg on `Gru.create(...)` (and ctor path) like:
      - `max_concurrent_starts: int | None = None`
    - behavior:
      - `None` = unbounded (current behavior; preserve backwards compatibility)
      - `>=1` = bounded concurrent starts via semaphore gate
      - `<=0` should raise `ValueError`
  - implementation:
    - create a starts semaphore on Gru init when bounded:
      - `_mn_starts_sem: asyncio.Semaphore | None`
    - wrap the full `start_orchestration(...)` orchestration body in the semaphore scope when enabled
      - (not just config read) so the whole start lifecycle is bounded consistently
    - keep `Minion.load_config()` using async read (`await asyncio.to_thread(path.read_text)`)
      because bounded start concurrency is the global throttle
    - ensure release on all code paths (`async with`), including failures and early returns
  - policy:
    - do not add separate config-read semaphores yet
    - if needed later, derive read cap from starts cap rather than adding another public knob
  - tests:
    - usage test: bounded starts allow at most N in flight (instrument with barrier/gate pipeline/minion assets)
    - usage test: with cap=1, concurrent start requests serialize deterministically
    - usage test: failures inside bounded section still release permit (no deadlock/leak)
    - usage test: `None` keeps current behavior
    - invalid usage test: cap `0` / negative raises clear error
  - docs:
    - document as start-storm/backpressure control
    - include guidance:
      - small local systems: `4-8`
      - slow NAS / shared hosts: start lower and tune up


- todo: add support for resourced pipelines and resourced resources (currently partially implemented)
  - requires implementation, testing, and documentation for each
  - before testing, the test asset structure will probably need a small refactor
  - todo: test that resourced domain objects can have multiple resource dependencies
    - current test assets only test 1 dependency per asset
  - justification:
    - consider a system like as follows
      ```python
      # WSClientResource
      # PriceOracleResource (depends on WSClientResource)
      # NewTokenCreatedPipeline (depends on WSClientResource)
      # TradingMinion (
      #   depends on NewTokenCreatedPipeline and PriceOracleResource
      #   and it's possible that you want to expose the "raw" WSClientResource
      #   to TradingMinion too
      # )
      ```
      so in other words, you compose your system at a high level using "raw" resources, "higher level" resources, pipelines, and minions.
    - docs follow-up:
      - write a "composing/designing your Minions system" guide
      - consider guidance around committing observability definitions to the repo

- todo: implement `minions gru serve` / `minions gru attach`
  - goal:
    - replace `GruShell` as the canonical runtime controller/operator UX
    - `minions gru serve` owns the long-lived `Gru` runtime lifecycle
    - `minions gru attach` connects to an already-running runtime, sends control-plane commands, and exits without stopping it
  - architecture:
    - split process model: work process = `minions gru serve`; control-plane client = `minions gru attach`
    - runtime remains single-process for Minions work; IPC is only for low-volume operator commands/responses
    - default transport: localhost TCP + JSONL request/response + per-runtime auth token
    - do not start with shared memory, Unix sockets, Windows named pipes, HTTP, gRPC, or a browser-facing API
  - serve:
    - command: `minions gru serve [--runtime default]`
    - create and own the long-lived `Gru` instance
    - bind TCP server to `127.0.0.1` on an ephemeral port by default
    - generate auth token with `secrets.token_urlsafe(32)`
    - write runtime metadata to a user-private state file
    - accept/authenticate JSONL requests, dispatch supported commands, return structured JSONL responses
    - clean up metadata file on graceful shutdown
  - runtime metadata:
    - path: `platformdirs.user_state_dir("minions")` + `/runtimes/<runtime>.json`
    - shape:
      ```json
      {
        "runtime": "default",
        "pid": 12345,
        "endpoint": "tcp://127.0.0.1:49152",
        "token": "...",
        "created_at": "..."
      }
      ```
    - on Unix, create parent dirs privately where practical and set metadata file mode to `0600`
    - on Windows, rely on user profile/app-data ACLs for v1
  - attach:
    - command: `minions gru attach [--runtime default]`
    - read runtime metadata, connect to endpoint, include token on every request
    - support interactive command loop or command passthrough
    - `exit`, `quit`, and Ctrl-D close attach only; they must not stop the served runtime
  - protocol:
    - JSON lines, one JSON object per request/response
    - request:
      ```json
      {"id": "request-id", "token": "...", "cmd": "status", "args": {}}
      ```
    - success response:
      ```json
      {"id": "request-id", "ok": true, "result": {}}
      ```
    - error response:
      ```json
      {"id": "request-id", "ok": false, "error": {"code": "unauthorized", "message": "unauthorized"}}
      ```
    - use `secrets.compare_digest(...)`; reject malformed/unauthenticated requests before command dispatch
  - initial commands:
    - migrate useful `GruShell` semantics into serve/attach instead of expanding `cmd.Cmd`
    - commands: `status`, `stop`, `redeploy`, `snapshot`, `deps`, `shutdown`, `help`
    - naming: `stop` stops work/component; `shutdown` stops serve daemon/runtime; `exit` closes attach only
  - security posture:
    - trusted single-user developer/operator environment; not internet-facing or hostile multi-tenant
    - not defending against root/admin or malware already running as the same OS user
    - bind `127.0.0.1` only by default; never bind `0.0.0.0` by default
    - require random token auth for every request
    - store token only in user-private runtime metadata
    - do not log token; do not print token unless explicitly requested for debugging
    - constant-time token comparison; reject malformed requests safely
    - v1 narrows access from "anything on localhost" to "anything that can read user-private runtime metadata"
  - non-goals for v1:
    - TLS, user accounts, RBAC, remote attach, public interface binding, Unix sockets, Windows named pipes, gRPC, shared memory, HTTP server, web UI, browser-facing API, multi-tenant isolation
  - future extensions:
    - one-shot commands: `minions gru status`, `minions gru stop <id>`, `minions gru redeploy <id>`, `minions gru snapshot`
    - optional transports: `--listen tcp://127.0.0.1:<port>`, `--listen unix://...`
    - possible future Go/polyglot attach client using the same protocol
    - remote attach requires a separate security design before implementation
  - GruShell deprecation:
    - treat `GruShell` as legacy design material, temporary local demo/helper, and source command semantics to migrate
    - do not expand `cmd.Cmd` behavior
    - transitional stability note:
      - fixed obvious exported-helper crashes: `wait` no longer references undefined helpers; `start` calls the current lifecycle start signature; successful starts are tracked by `StartResult.instance_id`
      - continue fixing obvious `GruShell` crashes only as needed; do not add new product semantics there
      - remove the temporary `pyproject.toml` Pyright exclude for `minions/_internal/_domain/gru_shell*.py` once `GruShell` is phased out
  - suggested implementation order:
    - add runtime metadata path helpers
    - add runtime metadata read/write/delete helpers
    - add JSONL request/response models
    - add token generation and auth validation
    - add `serve` TCP listener
    - add minimal `status` command
    - add `attach` client with one-shot request support
    - add interactive attach loop
    - migrate remaining commands
    - add graceful shutdown behavior
    - add tests for auth, malformed requests, attach exit behavior, and metadata cleanup
  - testing checklist:
    - serve writes metadata with endpoint, pid, token, runtime name
    - token is not logged
    - attach can connect using metadata
    - invalid/missing token is rejected
    - malformed JSON is rejected
    - unknown command returns structured error
    - `exit` closes attach only
    - `shutdown` stops served runtime
    - server binds only to `127.0.0.1`
    - stale metadata is handled cleanly
    - second `serve --runtime same-name` fails or handles conflict explicitly
    - metadata is cleaned up on graceful shutdown
    - Unix metadata file mode is `0600` where applicable
  - convo: https://chatgpt.com/c/69406c80-f478-8327-85b2-e3fb54d89796

- todo: implement and lock in two-stage Ctrl-C shutdown semantics for the runtime controller
  - scope:
    - implement for `minions gru serve` / `minions gru attach`; only backport to GruShell if it remains useful before replacement
  - behavior:
    - first Ctrl-C triggers graceful runtime shutdown and logs "press Ctrl-C again to force stop"
    - additional Ctrl-C during graceful shutdown forces immediate hard stop
  - tests:
    - requires test coverage in the test suite
    - first Ctrl-C path starts graceful shutdown (no immediate hard exit)
    - second Ctrl-C path hard-exits while graceful shutdown is in progress
    - process-level behavior is reasonable to validate by spawning the shell as a subprocess and asserting exit behavior/log output

- todo: provide uvloop support for better performance on *nix systems (maybe 2-4x more)
  - design:
    - user does "pip install minions[perf]" and gets uvloop if not on windows
      - in pyproject.toml add to objs
        ```python
        [project.optional-dependencies]
        perf = [
          "uvloop>=0.22,<0.23; platform_system != 'Windows'",
        ]
        dev = [
          ...
          "uvloop>=0.22,<0.23",
        ]
        ```
    - Gru supports uvloop; user sets the asyncio event loop policy before running Gru with `asyncio.run(...)`
  - implications:
    - the test suite may need to run each relevant test twice (once with uvloop and once with the default asyncio loop)
      - pytest could be configured as follows:
        - test both backends: "pytest tests/minions/_internal/_domain"
        - test only asyncio:  "pytest tests/minions/_internal/_domain --loop-policy asyncio"
        - test only uvloop:   "pytest tests/minions/_internal/_domain --loop-policy uvloop"
      - steps:
        - update tests/conftest.py
          ```python
          import pytest

          def pytest_addoption(parser):
              parser.addoption(
                  "--loop-backend",
                  action="append",
                  choices=["asyncio", "uvloop"],
                  help="Limit loop backends; default runs both",
              )

          def pytest_generate_tests(metafunc):
              if "loop_backend" in metafunc.fixturenames:
                  backends = metafunc.config.getoption("--loop-backend") or ["asyncio", "uvloop"]
                  metafunc.parametrize(
                      "loop_backend",
                      [pytest.param(b, id=b, marks=pytest.mark.loop_uvloop if b == "uvloop" else ()) for b in backends],
                      scope="session",
                  )
          ```
        - update tests/minions/_internal/_domain/conftest.py
          ```python
          import asyncio
          import pytest
          import pytest_asyncio
          import minions._internal._domain.gru as grumod
          from minions._internal._domain.gru import Gru

          @pytest.fixture(scope="session")
          def loop_policy(loop_backend):
              if loop_backend == "uvloop":
                  uvloop = pytest.importorskip("uvloop")
                  return uvloop.EventLoopPolicy()
              return asyncio.DefaultEventLoopPolicy()

          @pytest.fixture
          def event_loop(loop_policy):
              asyncio.set_event_loop_policy(loop_policy)
              loop = asyncio.new_event_loop()
              try:
                  yield loop
              finally:
                  loop.run_until_complete(loop.shutdown_asyncgens())
                  loop.close()
                  asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())

          @pytest_asyncio.fixture
          async def gru(loop_backend, logger, metrics, state_store):
              grumod._GRU_SINGLETON = None
              g = await Gru.create(logger=logger, metrics=metrics, state_store=state_store)
              try:
                  yield g
              finally:
                  await g.shutdown()
                  grumod._GRU_SINGLETON = None
          ```
    - it's the same deal with future benchmarks in a benchmarks dir, run each twice (once per loop)
    - in test suite, if uvloop not available, abort the suite and inform the dev
    - in my contributing.md
      - says requires Linux/macOS or WSL2
  - convo: https://chatgpt.com/c/693a8a64-55a0-8326-b383-881b36874aec

### Benchmarks:
- todo: build a benchmark harness in `benchmarks/`
  - define repeatable workload profiles:
    - `io_light_many` (many pipelines/minions, mostly waiting on I/O)
    - `io_mixed` (I/O + moderate transformation)
    - `startup_storm` (cold start of many minions at once)
  - write run outputs to `benchmarks/results/` as json/csv
  - capture metadata per run:
    - git sha
    - python version
    - loop backend (asyncio/uvloop)
    - machine specs
    - timestamp

- todo: measure max sustained concurrency
  - max concurrent pipelines
  - max concurrent minions
  - define sustained as stable operation for N minutes without errors/backlog explosion

- todo: measure throughput and latency
  - events/sec at steady state
  - end-to-end latency p50/p95/p99
  - startup latency for `Gru.create` and `start_orchestration`

- todo: measure memory footprint and stability over time
  - rss over time (baseline + under load + long soak)
  - detect leaks/drift across 1h+ runs
  - include gc stats if practical

- todo: compare loop backends where applicable
  - run each benchmark with default asyncio loop
  - run same benchmark with uvloop (if available)
  - report deltas with same workload and hardware

- todo: add cloud cost comparison against equivalent minions workload
  - estimate monthly cost for equivalent sustained throughput/latency
  - include assumptions:
    - instance type
    - region
    - uptime model
    - storage/network estimates
  - include a simple break-even table
  - related chat: https://chatgpt.com/c/69fe1264-d0e0-83ea-bf1f-daf66098f551

- todo: benchmark an equivalent microservices baseline (narrow scope)
  - pick one representative workflow shape
  - implement minimal equivalent using queue + worker services
  - compare:
    - throughput
    - p95/p99 latency
    - memory
    - operational complexity
    - monthly cost

- todo: publish a benchmark methodology doc
  - workloads
  - assumptions
  - limitations
  - fairness constraints
  - exact commands to reproduce
  - include a "what this does not prove" section

- todo: define benchmark release gates
  - set thresholds for p95/p99, memory ceiling, and error rate
  - fail release checklist if regressions exceed threshold

### Docs:
- todo: autogenerate API tree for the minions module using Sphinx autodoc/autosummary/intersphinx

- todo: update my docs and readme with positioning surfaced in this thread (https://chatgpt.com/c/693b751c-bb18-8329-b2d5-b6ece864000b)
  - ctrl+f to read from the following text to end of thread:
    - "Short answer: the angle I suggested is stronger than this one as a primary positioning, but most of what you wrote here is still very good. The difference is where and how it’s used."
  - note: thread also contains additional todos and plans

- todo: the landing page doc and README are almost the same; decide whether to centralize them or keep them separate
  - diff: https://chatgpt.com/codex/tasks/task_e_694a7a586ea883299cf280a9bf7fc64a
  - current lean: do not centralize yet, because the optimal structure may only be clear when deploying the docs site

- todo: add version switcher to docs

- todo: add example of auto-generating and auto-running paper trading strategies with an LLM
  - goal:
    - demonstrate how to securely automate business logic within a Minions system using an LLM (in this case: trading strategy ideation and validation)
  - design:
    - the LLM generates StrategySpecs, not Python code directly
    - StrategySpec events are emitted by a pipeline (`StrategySpecPipeline(Pipeline[StrategySpec])`)
    - StrategySpec events are received, run, and managed by a minion `StrategySpecRunnerMinion(Minion[StrategySpec, Ctx])`
  - note:
    - ideally sample Minions system examples should be runnable and have tests to assert behavior
  - open question:
    - emitting StrategySpec events may take non-trivial time; should pipelines support resumability for event generation?
    - current pipeline event emission happens in a single `Pipeline.emit_event` step, so long-running event generation has no persisted resume point
    - one possible design is `@pipeline_step`, where the last pipeline step returns the event instance
    - tradeoff: the current emit API is simple, so resumable pipeline steps should only be added if the use case justifies the extra model
  - convo: https://chatgpt.com/c/694e3d91-2ae0-8328-b434-72d8a30af9e2

### Misc:
- todo: comb the codebase for any remaining TODO comments; resolve them or consolidate them into this file

- todo: define and enforce a runtime logging contract, then manually audit wording for operator/user framing
  - goal:
    - runtime logs should read as event statements with structured details in kwargs
    - operator/user-facing runtime logs should put the operator/user concern first unless the log is specifically a developer/runtime error
    - common structured kwargs should be present consistently across applicable logs, so monitoring, alerts, dashboards, and support workflows can depend on them
  - automated contract checks:
    - collect emitted logs from representative Gru scenario runs instead of relying only on manual review
    - assert event-message formatting mechanically:
      - lowercase event-style message strings
      - no important runtime details embedded only in the message when they should be kwargs
      - stable message strings where downstream monitoring may depend on them
    - assert expected log levels for important runtime events
    - assert common kwargs by log category / runtime surface rather than requiring every log to carry every field
      - define a small log category / runtime surface taxonomy before locking required kwargs
        - possible categories: lifecycle, workflow, pipeline, resource, persistence, logger, metrics, admission-control, dev/runtime-error
        - use the taxonomy to decide which kwargs are required, optional, or irrelevant for each category
      - possible common fields:
        - component / component_type / component_id
        - minion_name / minion_instance_id / orchestration_id
        - workflow_id / step_name
        - pipeline_name / pipeline_instance_id
        - resource_name / resource_instance_id / resource_method
        - error_type / error_message for failure logs
      - decide the final field names alongside the orchestration identity terminology cleanup so logs, metrics, docs, and tests do not drift
    - make intentional log contract changes visible in tests because message strings and kwargs can become monitoring compatibility surfaces
  - manual audit:
    - review collected runtime logs for operator/user-first wording:
      - can an operator tell what happened?
      - can they identify the affected runtime object?
      - can they tell whether action is needed?
      - is the message about the user-visible event first, with developer details reserved for kwargs or dev-error logs?
    - manually classify logs that are legitimately developer/runtime errors so they are not forced into operator-facing wording
  - testing strategy:
    - use narrow unit assertions when a single code path owns a specific log event
    - use broader Gru scenario / contract tests for runtime flows where message, level, kwargs, ordering, or rate-limiting behavior matter together
    - avoid snapshotting noisy or incidental logs unless they are part of the intended observability contract
  - example:
    - "async component started", {"component_type": "SQLiteStateStore", ...}

- todo: setup github repo so feature requests are surfaced thru "discussions" instead of "issues"
  - https://chatgpt.com/c/693f6fff-6bac-8333-9844-b1aade31a4d5

- todo: read the following docs for inspiration on how to structure this project's docs
  - https://fastapi.tiangolo.com/
  - https://microsoft.github.io/autogen/stable/
  - https://python-prompt-toolkit.readthedocs.io/en/master/index.html

- todo: after building all features for v0.1.0 release, read through the docs start to finish to see if anything needs adding/updating

- todo: dogfood the runtime and refine it based on findings

### Consider:
- consider: adopt a two-lane test execution model to surface intermittent runtime bugs without slowing normal dev loops
  - lane 1 (fast): run full suite once on every change (`pytest`)
  - lane 2 (flake/stress): run only concurrency/orchestration/lifecycle-sensitive tests in loops (`30x` for risky PRs, `100x` nightly/pre-release)
  - rationale:
    - recent GRU startup replay bug was only surfaced under repeated stress runs (single-pass was often green)
    - looping all tests is too expensive/noisy, but looping high-risk tests gives strong signal for race/timing bugs
    - policy-driven stress runs improve confidence before release while keeping day-to-day feedback fast
  - possible implementation:
    - maintain a small curated target list (or pytest marker like `@pytest.mark.flake`)
    - add scripts/commands for `fast` and `flake` lanes
    - fail-fast in flake lane with iteration index and failing test output
  - note:
    - evaluate whether the test suite can run safely in parallel; this could provide a large performance gain if shared resources and timing-sensitive tests are structured correctly

- consider: implement sharded sqlite state store for workflow context persistence (same could be said for sqlite logger)
  - goal:
    - increase persistence throughput by replacing the single SQLite file with `N` deterministically sharded SQLite files
    - preserve the current state store API and current batching/coalescing behavior
    - keep SQLite as embedded storage rather than introducing external infra
  - current fit:

    - current schema is already ideal for sharding because atomicity is naturally bounded by `workflow_id`
    - current store is basically KV-like persistence over:

      - `workflow_id TEXT PRIMARY KEY`
      - `context BLOB NOT NULL`
    - current implementation already batches/coalesces writes, so sharding composes naturally with the existing design
  - design:

    - split into two layers:

      - `SQLiteStateStoreShard`

        - owns one sqlite database file
        - owns one connection
        - owns one `_batch`
        - owns one `_lock`
        - owns one `_flush_task`
        - preserves current shard-local batching and flush behavior
      - `ShardedSQLiteStateStore`

        - owns `list[SQLiteStateStoreShard]`
        - routes each workflow operation to exactly one shard by deterministic hashing of `workflow_id`
    - deterministic shard routing:

      - use stable hash of `workflow_id`
      - route by:

        - `shard_idx = stable_hash(workflow_id) % shard_count`
      - do not use Python `hash()` because placement must remain stable across process restarts
      - use something simple/stable like truncated `blake2b` or `sha1`
    - shard file layout:

      - store files in a base dir like:

        - `workflows_00.sqlite3`
        - `workflows_01.sqlite3`
        - ...
    - api behavior:

      - `save_context(workflow_id, payload)`:

        - route to one shard
      - `get_context(workflow_id)`:

        - route to one shard only
        - do not scan all shards
      - `delete_context(workflow_id)`:

        - route to one shard only
      - `get_all_contexts()`:

        - fan out across all shards in parallel
        - concatenate results
  - constraints:

    - no placement/index database

      - deterministic routing removes the need for a metadata DB
    - no cross-shard transactions

      - acceptable because current persistence boundary is already per-workflow
    - no scatter/gather for point lookups

      - shard lookup must remain O(1)
    - no serialization format changes

      - keep existing payload encoding as-is
  - correctness work:

    - fix the existing delete-vs-flush race before or during sharding

      - current risk:

        - a pending flush can re-upsert a workflow after `delete_context()` has already removed it
      - this is a real correctness issue and becomes more important once multiple shard flushers exist
    - implementation options:

      - minimal fix:

        - track `_pending_deletes`
        - ensure pending flushes skip deleted workflow ids
      - cleaner fix:

        - serialize all shard writes/deletes through a single shard writer queue
        - batching becomes queue-drain based
      - preference:

        - minimal fix first unless queue-based rewrite feels clean enough to justify now
  - batching:

    - preserve current shard-local batching semantics
    - each shard should continue to:

      - coalesce multiple writes to the same `workflow_id`
      - flush when `batch_max_n` is reached
      - flush when `batch_max_ms` elapses
    - this keeps the current batching win while also unlocking parallel writes across shards
  - startup and shutdown:

    - startup:

      - initialize all shards in parallel
      - create schema on each shard
      - optionally calibrate batch defaults once and apply to all shards
    - shutdown:

      - flush all shards in parallel
      - close all shard connections cleanly
  - shard count:

    - start with small fixed shard counts

      - default: `4`
      - practical early range: `4-16`
    - do not over-shard initially

      - each shard adds file handles, WAL activity, caches, connection overhead, and one more flusher
  - stats and logging:

    - keep shard-local metrics similar to current implementation

      - commit latency
      - backlog size
      - rows/sec capacity
    - optionally add store-level summary logging:

      - per-shard backlog
      - hottest shard
      - shard skew ratio
  - expected throughput increase:

    - realistic estimate:

      - `4` shards: roughly `2x-3.5x` write throughput
      - `8` shards: roughly `3x-5x` write throughput
    - best case:

      - if current bottleneck is mostly single-writer lock contention and workflow ids distribute well, gains can approach shard-count scaling early on
    - weaker case:

      - if bottleneck is mostly serialization cost, oversized blobs, hot-key concentration, or raw disk latency, gains may be closer to `1.3x-2x`
    - practical read:

      - for this exact schema, sharded SQLite is very likely the highest-leverage next step before considering Postgres
  - implementation steps:

    - extract current single-file logic into `SQLiteStateStoreShard`
    - add stable hash helper for shard routing
    - add `ShardedSQLiteStateStore` wrapper
    - route `save_context`, `get_context`, and `delete_context` by shard
    - implement `get_all_contexts()` as parallel fanout across shards
    - fix delete-vs-flush resurrection behavior
    - add shard-aware startup and shutdown
    - add store-level shard stats
    - add benchmark script comparing:

      - single-file sqlite
      - sharded sqlite with 4 shards
      - sharded sqlite with 8 shards
  - tests:

    - correctness:

      - save/get roundtrip
      - overwrite same `workflow_id`
      - delete removes persisted row
      - repeated save/delete/save ordering
      - delete during pending flush does not resurrect row
      - restart preserves data
      - `get_all_contexts()` returns union of all shards
    - routing:

      - same `workflow_id` always maps to same shard across restarts
      - large sample of ids distributes roughly evenly
    - performance:

      - concurrent saves to many distinct workflow ids
      - hot-key workload
      - mixed save/delete workload
      - larger payload blobs
  - decision rule:

    - implement this if:

      - persistence is becoming a measurable bottleneck
      - writes are spread across many workflow ids
      - you want more throughput without infra burden
    - defer this if:

      - current batching is already sufficient
      - persistence is not near the top of the profile
      - the real bottleneck is serialization rather than sqlite writer contention
