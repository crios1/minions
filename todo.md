<!-- 
  Complete these todos from top to bottom.
  Highest Level Todo:
  - Consolidate (and spec out) my todos to establish a priority between them.
    First, consolidate codebase TODOs and relevant notes from design conversations into this file.
    Then, complete the test-suite refactor so future work can be implemented end to end with running tests.
    Next, complete partially finished efforts like GruShell to tidy the codebase and document/test their designs.
    Finally, work through this file end to end with running tests.
-->

### Test Suite:
- todo: add an explicit user-guarantee/versioning dimension to the test-suite strategy
  - problem:
    - `tests/README.md` explains confidence layers for runtime/component correctness, but it does not yet clearly separate tests that protect user-facing API and behavior guarantees
  - examples:
    - supported persisted event/context shapes such as `dict`, dataclass, and `msgspec.Struct`
    - backwards-compatible CLI/API behavior
    - guarantees that should only change across intentional major-version boundaries
  - why it matters:
    - separating implementation correctness from public guarantee tests makes breaking-change decisions more deliberate

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

- todo:
  - audit the test suite to align with the default-backend + contract-test strategy
    - for each system component base class, ensure there is a contract test file
    - check higher-level/domain tests to ensure they use the in-memory system component by default
    - only keep non-default backends in a test when the backend-specific choice is intentional and clearly justified
  - document this test suite design doc with this design info

- todo: harden test_gru.py:
  - steps:
    - complete the robust reusable Gru testing routine
    - rewrite Gru tests to use the routine
    - integrate Gru tests from other files:
      - test_minion_states.py is basically a Gru test where checking for counters should be handled by the reusable testing routine

- todo: add stop modes to gru.stop_minion and map to GruShell redeploy strategies
  - default mode: pause (current behavior)
  - modes and behavior:
    - pause: stop new workflows; persist in-flight workflows for resume on restart
    - drain: stop new workflows; await in-flight workflows to finish; then stop
    - cutover: stop new workflows; abort in-flight workflows; then stop
  - interruptibility:
    - if drain is in progress and caller requests cutover or pause, override drain immediately
    - if drain is requested again, treat as idempotent
    - if minion already stopped, return success with reason
  - api sketch:
    - gru.stop_minion(name_or_instance_id, mode="pause"|"drain"|"cutover")
    - grushell redeploy maps:
      - redeploy drain -> stop_minion(mode="drain")
      - redeploy cutover -> stop_minion(mode="cutover")
  - tests:
    - add orchestration helper directives for mode="drain"/"cutover"
    - add runtime tests for in-flight workflow behavior per mode
  - open questions:
    - Should `continue-on-failure` track outstanding failed checkpoints instead of only retrying when the workflow reaches the next checkpoint?
    - Should `WorkflowPersistenceFailurePolicy` remain Gru-level, become Minion-level, or support both global defaults and per-minion overrides?
    - The desired guarantee is that `Gru.stop_minion(...)` should not lose workflow state in any stop mode unless the process is force-interrupted.
  - why it matters:
    - failed checkpoint state affects the exact semantics of `drain` and `cutover`, especially when a workflow has progressed beyond its last durable checkpoint.

- todo: add robust tests for persisted event/context support across dict, dataclass, and msgspec.Struct shapes
  - ```python
    class MyEvent(msgspec.Struct):
      greeting: str = "hello world"
    ```
  - ```python
    class MyContext(msgspec.Struct):
      my_attribute: str | None = None
    ```

- todo: ensure immediate user-facing domain objects (minion, pipeline, resource) and non-immediate user-facing domain objects (like StateStore, logger, metrics)...
  - 1: validate composition at class definition time and raise user friendly exception msg (good onboarding DX)
    - ex: tests/minions/_internal/_domain/test_minion.py
  - 2: validate usage at orchestration time (like in test_gru.py or one of its subdirectories if it got reorganized)
    - ex: tests/minions/_internal/_domain/test_gru.py::TestValidUsage.test_gru_accepts_none_logger_metrics_state_store
  - note: for non-immediate user-facing domain objects, validating composition at class definition time may not always be possible, so it's fine to only validate at orchestration time if that's the case

- todo: clean up domain object attrspace usage
  - todo: move private domain object attrs to minions attrspace (`_mn_`)
    - steps:
      - temporarily replace async_lifecycle.AsyncLifecycle._mn_ensure_attrspace
        with the following to detect attrs to migrate
        ```python
        @classmethod
        def _mn_ensure_attrspace(cls):
            names = {**cls.__dict__, **getattr(cls, "__annotations__", {})}
            bad = set()
            for name in names:
                if not name or not isinstance(name, str):  # skip non-identifiers
                    continue
                if name[0].isalpha():  # skip public
                    continue
                if name.startswith('__'):  # skip dunder
                    continue
                if name.startswith('_mn_'):  # skip internal attrspace
                    continue
                bad.add(name)
            if bad:
                modpath = f"{cls.__module__}.{cls.__qualname__}"
                raise Exception(
                    f"found private attr(s) in {modpath}; change to '_mn_' attrspace; attrs="
                    + ", ".join(sorted(bad))
                )
        ```
      - run the test suite, failing on first fail, prefix the attrs until whole test suite passes

  - todo: write tests that prove user can not write to minions attrspace (`_mn_`)
    - make tests for each user-facing domain object that try to add `_mn_` prefix attrs in each space the user has the opportunity to do so:
      - lifecycle hooks
      - pipeline emit event
      - minion steps
      - resource user defined methods
    - ensure that Gru refuses to start those orchestrations

  - todo: ensure attrspace and user-submitted code (across minions, pipelines, resources) is validated at class definition time
    - reason:
      - doing so provides fast feedback to users new to the runtime as they go through the dev/playground mode of onboarding
      - they won't need to inspect StartMinionResult to know they violated a compositional rule or constraint; an exception will be raised at class definition time instead
      - this behavior is safe because production Minions systems should generally use string-based minion starts, so class-definition validation is mostly a development-time feedback path
    - implementation:
      - call the validation hook from each `__init_subclass__` implementation for Minion, Pipeline, and Resource
      - in the test suite, defer `__init_subclass__` the same way SpiedMinion does (this may already be done for all domain objects)

  - todo: in test suite, import all domain objects (minion, pipeline, resource, StateStore, logger, etc.) and assert that they don't have private attrs
    - it's a check that says the user's private attr space is clean / we don't accidentally ship classes where we use the private attrspace
    - don't ship the check at in the final code, just do the check in the test suite because it gives us assurance

- todo: add per-entity lifecycle locking to gru for concurrent-safe starts/stops.
  - guarantees:
    - no duplicate minion starts for the same composite key; shared pipelines/resources start once; concurrent stops don’t double-cancel.
  - failure handling:
    - if an error occurs inside a locked section, undo partial registry inserts before releasing the lock; log skipped work when another op already holds the lock (debug is enough).
  - tests to add:
    - concurrent same-key start_minion collapses to one instance; concurrent same-id stop_minion is idempotent; two minions sharing a pipeline/resource don’t double-start it.
  - note: use the (_minion_locks, _pipeline_locks, _resource_locks) gru attrs
  - convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/6910f9e9-d76c-8327-92b3-ea4b729b6288

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

- todo: add bounded startup concurrency to Gru (`max_concurrent_minion_starts`)
  - goal:
    - prevent startup storms (ex: 50+ minions on restart) from overloading event loop / NAS / host I/O
    - keep startup throughput high but controlled and predictable
  - api:
    - add kwarg on `Gru.create(...)` (and ctor path) like:
      - `max_concurrent_minion_starts: int | None = None`
    - behavior:
      - `None` = unbounded (current behavior; preserve backwards compatibility)
      - `>=1` = bounded concurrent starts via semaphore gate
      - `<=0` should raise `ValueError`
  - implementation:
    - create a startup semaphore on Gru init when bounded:
      - `_mn_start_minion_sem: asyncio.Semaphore | None`
    - wrap the full `start_minion(...)` orchestration body in the semaphore scope when enabled
      - (not just config read) so the whole start lifecycle is bounded consistently
    - keep `Minion.load_config()` using async read (`await asyncio.to_thread(path.read_text)`)
      because bounded start concurrency is the global throttle
    - ensure release on all code paths (`async with`), including failures and early returns
  - policy:
    - do not add separate config-read semaphores yet
    - if needed later, derive read cap from startup cap rather than adding another public knob
  - tests:
    - usage test: bounded starts allow at most N in flight (instrument with barrier/gate pipeline/minion assets)
    - usage test: with cap=1, concurrent start requests serialize deterministically
    - usage test: failures inside bounded section still release permit (no deadlock/leak)
    - usage test: `None` keeps current behavior
    - invalid usage test: cap `0` / negative raises clear error
  - docs:
    - document as restart-storm/backpressure control
    - include guidance:
      - small local systems: `4-8`
      - slow NAS / shared hosts: start lower and tune up

- todo: write tests for gru.start_minion to lock in that it works with class and string-based starts

- todo: add early (best-effort) serialization validation for user-provided event and workflow context types at Pipeline / Minion definition time
  - statically check that user type annotations are supported by gru's serialization, and raise when an annotation is not
  - this is an early feedback mechanism, full serializability can only be guaranteed at runtime
  - ensure no on-going validations; just validation once at class definition time  

- todo: add "crash testing" to test suite to ensure the Minions runtime preserves its crash guarantees
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

- todo: once the reusable Gru testing routine is ready, fill out coverage for the ways users interact with Gru, especially the file shapes they can pass to it
 - audit each test asset in the suite and ensure it is useful and covered by a reasonable Gru test; otherwise, add the test or delete the asset

### Features:
- todo: settle official terminology for orchestration identity vs minion composite key
  - problem:
    - the codebase currently uses `minion_composite_key` and `orchestration_id` in closely related or interchangeable ways
    - persistence rows store `orchestration_id`, while telemetry and runtime labels expose `minion_composite_key`
    - this creates ambiguity for API docs, metric interpretation, and future migration work
  - goal:
    - choose one official concept name for the stable minion/config/pipeline orchestration identity
    - define whether `minion_composite_key` remains an internal construction detail, becomes the official term, or is renamed to `orchestration_id`
    - update code, docs, logs, metrics labels, and test helpers consistently once the term is chosen
  - why it matters:
    - unclear identity terminology can become a compatibility problem for users who build alerts, dashboards, stored state tools, or operational docs around these names

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
      - reject new `start_minion` requests (return a typed failure / reason = `memory_pressure`)
      - suspend pipelines from admitting new work (soft suspend: don’t kill in-flight work; just stop new scheduling/emit)
      - log pressure state with rate limiting (avoid spam)
    - container vs host measurement:
      - support `mode="auto"` that prefers cgroup limits when available, otherwise host memory
      - if measurement fails, degrade safely (disable guard + log once), do not brick runtime
    - config surface (minimal, user-overridable):
      - `MemoryPressureCfg(shed_ratio, resume_ratio, enter_consecutive, exit_consecutive, cooldown_s, poll_interval_s, enabled=True, mode="auto")`
      - keep defaults aggressive/high-util; users can set conservative ratios if they want more headroom
  - implementation notes:
    - wire guard state into `Gru.start_minion(...)` admission path
    - wire guard state into pipeline scheduling / emit admission path (one choke point; no scattered checks)
    - keep all internal fields in `_mn_` attrspace
    - need to log/notify the user when state changes (and what that means) but not too often
  - tests:
    - test: sustained high memory enters SHED and rejects new `start_minion`
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

- todo: decouple orchestration addressing from stable runtime identity (component_id + instance_id)
  - context:
    - current identity spine is “project-root relative path/modpath”
    - this breaks resumability + Prometheus continuity on directory refactors
    - long-term guarantee requires stable ids not derived from paths
  - goals:
    - preserve current DX: `start_minion()` accepts classes or string refs
    - make inflight workflow resume + reuse + metrics stable across refactors
    - avoid forcing users to hand-maintain a full catalog unless they want nicer names
  - decisions:
    - introduce canonical stable ids:
      - `component_id` = stable identity for each domain component class (minion/pipeline/resource)
      - `instance_id` = stable identity for a running wiring of components + config (minion instance)
    - refs remain for loading / addressing:
      - string refs continue to work as “how to import the class”
      - ids become “how the runtime persists and labels the thing”
    - ids are immutable; “renames” are aliases, not id mutation
  - steps:
    - introduce registry storage under project root:
      - `component_registry`: maps `component_id -> component_ref` (+ kind, timestamps, optional aliases)
      - `alias_index`: maps `alias -> component_id` (human ids)
      - persist in `.minions/` (toml/json/sqlite; pick one and standardize)
    - define canonical “component ref” format:
      - `module:qualname` (plus `kind` prefix internally if needed)
      - store both `component_ref` and `component_id` everywhere you currently store “relpath”
    - update Gru resolution pipeline:
      - when `start_minion(minion=<class|ref|id>, pipeline=<class|ref|id>, ...)` is called:
        - resolve each input to `(component_id, component_ref, cls)`
        - if unknown component, register and assign `component_id` automatically
        - if input is alias, resolve via alias_index
        - if input is component_id, load via registry’s current ref
      - make reuse keys based on `component_id` (not relpath)
    - define config identity and instance identity:
      - treat config as identity input, not a “component class”
      - `config_id`:
        - if `minion_config_path` provided: compute a stable hash of normalized config content (and store optional friendly alias)
        - if `minion_config` mapping provided: compute hash from normalized serialization
      - `instance_id = hash(minion_component_id + pipeline_component_id + config_id + resource_binding_ids)`
      - reuse policy:
        - pipelines reuse on `pipeline_component_id` (unless pipeline itself is config-parameterized; if so, fold pipeline config into its instance identity too)
        - minion instances are distinct on `instance_id`
    - update persistence + resume:
      - change workflow state keys to use `instance_id` + `component_id` (never `component_ref`)
      - ensure any “resume in-flight” logic loads classes via `component_id -> component_ref -> import`
      - add migration shim:
        - if old state keys exist (path-based), attempt best-effort mapping to new ids or mark orphaned with a clear error
    - update prometheus labeling:
      - stable labels:
        - `component_id`, `instance_id`
      - human/debug labels:
        - `component_ref`
        - optional `alias` if user assigned
        - optional `config_alias` if provided
      - ensure dashboards can be written against stable labels
    - define type / class persistence as part of this identity model:
      - this work also decides how persisted Python `type` references are serialized and resolved
      - `MinionWorkflowContext.context_cls` is the current concrete example
      - keep runtime/domain objects Python-native; persistence adaptation belongs at the framework boundary
      - persisted state must not rely solely on current import path stability
      - decide whether persisted type references store `component_id`, current `component_ref`, or both
      - support relink / migration after file moves or refactors without making persisted workflows unusable
      - UX should remain simple while file structure changes are happening, not only after everything is stable again
    - add CLI support (minimal but sufficient):
      - `minions ls` (show component_id, kind, alias(es), current ref)
      - `minions resolve <alias|id|ref>` (print canonical ids + refs)
      - `minions alias set <alias> <component_id>` (alias only; never mutate id)
      - `minions ref set <component_id> <new_ref>` (relink after refactor)
      - `minions instance ls` (show instance_id composition: minion/pipeline/config)
    - document the new contract:
      - “refs can change; ids persist”
      - “refactors require either a relink (ref set) or alias mapping; inflight workflows remain resumable”
      - “durable observability requires stable labels; use alias for readable dashboards”
  - convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/69446e9e-053c-832c-abfb-ba40b5123693
  - other convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/694725cf-a5c4-8326-bdfc-b95f1b289f14
  - note: consider how cross env (dev,qa,prod) comparison will work: like with grushell snapshot/redeploy, discussed in "other convo"
  - note: `minion_composite_key` currently appears to overlap with the runtime meaning of `orchestration_id`; this should be resolved by the terminology todo above

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
    - do this only after the identity migration above is finalized (`component_id` + `instance_id`)
  - problem:
    - current path effectively scans all stored workflow contexts, then filters in memory
    - this is unnecessary read amplification once stable ids are in place
  - target:
    - query by minion identity/workflow key directly from sqlite (no full table scan)
    - keep current public behavior unchanged
  - tests:
    - add/adjust tests to prove behavior matches current semantics while using indexed lookup

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

- todo: implement "minions gru serve" and "minions gru attach"
  - basically a redesign of the runtime controller
  - GruShell can remain as a demo or embedded helper, while the official operational flow may become a separate serve/attach model
  - convo: https://chatgpt.com/c/69406c80-f478-8327-85b2-e3fb54d89796
  - should consider creating a golang cli for a cli option of 'minions gru attach'

- todo: complete GruShell (~90% implemented, needs documentation / user onboarding flow)
  - users may embed GruShell into deployment scripts or use a cookbook script
  - open question:
    - should `python -m minions shell` be the primary onboarding path, or should it be framed as an experimental/development helper?

- todo: implement and lock in two-stage Ctrl-C shutdown semantics for GruShell (for Gru too if not implemented)
  - scope:
    - implement in GruShell / shell entrypoint (`minions/shell.py`), not in `Gru` core runtime
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
  - startup latency for `Gru.create` and `start_minion`

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

- todo: manually audit runtime logs and ensure they read as events w/ details in kwargs (also that event msgs are lowercase)
  - ex: "async component started" , {'component': SQLiteStateStore}
  - note: it would be useful to enforce that quality when running Gru scenarios by asserting from a set of log message / log kwargs pairs
  - note: changing log messages or kwargs can be a breaking change for monitoring, so tests should make intentional log changes visible
  - note: decide on a broader logging-test strategy for the suite, not just isolated log unit tests
    - decide when to use narrow unit assertions on a single emitted log vs broader scenario/contract tests that lock in log messages, levels, kwargs, ordering, and rate-limiting behavior across runtime flows

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
