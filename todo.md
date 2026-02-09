<!-- 
  Complete these todos from top to bottom.
  Highest Level Todo:
  - Consolidate (and spec out) my todos to establish a priority between them.
    First, I will take the todos throughout the codebase and consolidate them in this file. (there are some todos in my brower tabs as chat gpt convos too)
    Then, I will complete the test suite refactor so following todos can be implemented end to end w/ running tests.
    Next, I'll complete partially completed endeavors like GruShell to tidy the codebase. (document grushell design and test)
    Finally, I'll complete the todos in this file end to end w/ running tests.
-->

### Test Suite:
- todo: harden test_gru.py:
  - steps:
    - complete the robust reuseable gru testing routine
    - rewrite gru tests to use the routine
    - integrate gru tests from other files:
      - test_minion_states.py is basically a gru test where checking for counters should be handled by the reuseable testing routine

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

- todo: refactor tests/assets naming + organization for long-term maintainability
  - decision:
    - directory-first taxonomy; no hand-maintained manifest
  - goals:
    - scale to ~100–200 assets without filename bloat or collisions
    - encode intent in paths + semantic names; avoid “mystery variants”
    - preserve ability for tests to stay “pure Python imports” of assets
  - conventions:
    - layout is the primary index:
      - `tests/assets/<kind>/<scenario_family>/...`
      - kinds: `pipelines/`, `minions/`, `resources/`, `workflow_files/`, `config/`, `support/`
      - `scenario_family` = the human-meaningful scenario bucket (e.g. `emit1`, `emit_loop`, `on_simple_event`, `invalid_explicit`, etc.)
    - filenames are short + semantic:
      - `tests/assets/<...>/<case>.py` (snake_case)
      - `case` describes purpose (e.g. `hang_after_first_event`, `invalid_missing_resource`, etc.)
    - identity clones use letters, not numbers:
      - when identical behavior but distinct runtime identity is required: `<case>_a.py`, `<case>_b.py`, `<case>_c.py`
      - class names inside match suffix (e.g. `MyPipelineA`, `MyPipelineB`) to satisfy “unique identity per class path”
    - skip vNN by default:
      - don’t version fixtures; edit in place
      - when you need “old behavior kept alongside new”, create a new semantic case asset instead of vNN
  - docs:
    - add `tests/assets/README.md` defining:
      - taxonomy + naming rules
      - examples for each kind
      - when to use `_a/_b/_c` clones
      - where invalid fixtures live and how to name them
  - convo:
    - https://chatgpt.com/c/693f9bc9-c46c-8327-bd5a-c9b38f45859b

- todo: write robust tests that i support persisting state for dict, dataclass, and msgspec.Struct as event and context types (implement and say you support those types in the docs)
  - ```python
    class MyEvent(msgspec.Struct):
      greeting: str = "hello world"
    ```
  - ```python
    class MyContext(msgspec.Struct):
      my_attribute: str | None = None
    ```

- todo: ensure immediate user facing domain objects (minion, pipeline, resource) and non-immediate user facing domain objects (like statestore, logger, metrics)...
  - 1: validate composition at class definition time and raise user friendly exception msg (good onboarding DX)
    - ex: tests/minions/_internal/_domain/test_minion.py
  - 2: validate usage at orchestration time (like in test_gru.py or one it's subdirs if it got reorganized)
    - ex: tests/minions/_internal/_domain/test_gru.py::TestValidUsage.test_gru_accepts_none_logger_metrics_state_store
  - note: for non-immediate user facing domain objects, validating composition at class definition time may not always be possible, so it's fine to only validate at orchestration time if that's the case ... will have to further spec out, i have to test the abstractions and actual implementations of these objects robustly

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
    - make tests for each user facing domain object that try to add `_mn_` prefix attr in each space the user has the opportunity to do so:
      - lifecycle hooks
      - pipeline emit event
      - minion steps
      - resource user defined methods
    - ensure that gru refuses to start those orchestrations

  - todo: ensure attrspace and user submitted code (across minions, pipelines, resources) is validated at class definition time
    - reason:
      - doing so provides fast feedback to users new to the runtime as they go thru the dev/playground mode of onboarding
      - they won't need to inspect StartMinionResult to know they violated a compositional rule or constraint, an exception will be raise at class definition time instead
      - this behavior is safe because in production minions systems, the user will be using str style minion starts so there is no risk of messing up thier prod launch cuz they won't by passing classes to gru
    - implementation:
      - basically just call ... in each the `__init_subclass__` of Minion, Pipeline, Resource
      - and then in the test suite i'll defer the `__init_subclass__` like i do to make SpiedMinion (might already be done for all domain objects in test suite)

  - todo: in test suite, import all domain objects (minion, pipeline, resource, statestore, logger, etc.) and assert that they don't have private attrs
    - it's a check that says the user's private attr space is clean / we don't accidently ship classes where we use the private attrspace
    - don't ship the check at in the final code, just do the check in the test suite because it gives us assurance

- todo: robustify / clean up minions/_internal/_utils/serialization.py and it's respective test file
  - diff: https://chatgpt.com/codex/tasks/task_e_694a8fe5a8a883299f9aa2c9fc0294af
  - (just make sure to copy the comment in my current serialization code to the diff since it wasn't present when the diff was created)

- todo: add per-entity lifecycle locking to gru for concurrent-safe starts/stops.
  - guarantees:
    - no duplicate minion starts for the same composite key; shared pipelines/resources start once; concurrent stops don’t double-cancel.
  - failure handling:
    - if an error occurs inside a locked section, undo partial registry inserts before releasing the lock; log skipped work when another op already holds the lock (debug is enough).
  - tests to add:
    - concurrent same-key start_minion collapses to one instance; concurrent same-id stop_minion is idempotent; two minions sharing a pipeline/resource don’t double-start it.
  - note: use the (_minion_locks, _pipeline_locks, _resource_locks) gru attrs
  - convo: https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/6910f9e9-d76c-8327-92b3-ea4b729b6288

- todo: write tests for gru.start_minion to lock in that it works with class and str based starts

- todo: write a gru test that ensures a minion has access to self.event and self.context
in all steps include when resuming a workflow from statestore

- todo: add early (best-effort) serialization validation for user-provided event and workflow context types at Pipeline / Minion definition time
  - statically check that user type annotations are supported by gru's serialization, and raise when an annotation is not
  - this is an early feedback mechanism, full serializability can only be guaranteed at runtime

- todo: add "crash testing" to test suite to ensure that minions runtime does the runtime crash guarentees

- todo: now w/ the robust reuseable testing routine (from the harden/repair test_gru.py todo) fill out the test suite to cover all the ways the user will interact with gru... mainly the kinds of files they can pass to it.
 - audit each of the test asset in the test suite and ensure they are useful and being used by gru with a reasonable test. otherwise, add the test(s) or delete the asset

### Features:
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
    - this breaks resumeability + prometheus continuity on directory refactors
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

- todo: add support for resourced pipelines and resourced resources (currenlty partially implemented)
  - requires implementation, testing, and documentation for each
  - before testing, i'll probably have to refactor the structure of test assets
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
      so in other words, you compose your system at a high level using "raw" resources, "higher level" resources, pipelines, and minions. (todo: i'll flesh this out in the docs as something like a "composing / designing your minion system") ... also maybe talk about commiting your observeability to a repo.

- todo: implement "minions gru serve" and "minions gru attach"
  - basically a redesign of the controller of the runtime, GruShell will remain as perhaps a demo thing or something maybe but the official and best way to use gru and the shell is in a serve-attach model as seperate
  - convo: https://chatgpt.com/c/69406c80-f478-8327-85b2-e3fb54d89796

- todo: complete GruShell (~90% implemented, needs documentation / user onboarding flow)
  - users will embed GruShell into thier deployment scripts / use the cookbook to make the script
  - but maybe it makes sense to let the user experiment with the shell by calling "python -m minions shell"? i need to consider the user onboarding flow further.

- todo: implement and lock in two-stage Ctrl-C shutdown semantics for GruShell
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
    - gru supports uvloop; user sets the asyncio event loop policy before running gru with `asyncio.run(...)`
  - implications:
    - my test suite needs to run each test twice (once w/ uvloop and again w/ asyncio loop)
      - i can configure pytest to behave as follows:
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

### Docs:
- todo: autogenerate api tree for minions module using like sphinx autodoc, autosummary, intersphinx

- todo: update my docs and readme with positioning surfaced in this thread (https://chatgpt.com/c/693b751c-bb18-8329-b2d5-b6ece864000b)
  - ctrl+f to read from the following text to end of thread:
    - "Short answer: the angle I suggested is stronger than this one as a primary positioning, but most of what you wrote here is still very good. The difference is where and how it’s used."
  - note: thread also contains additional todos and plans

- todo: my landing page doc and readme are almost the same, i should consider centralizing them to some extent or better to maintain them seperately?
  - diff: https://chatgpt.com/codex/tasks/task_e_694a7a586ea883299cf280a9bf7fc64a

- todo: add version switcher to docs

- todo: add example of auto-generating and auto-running paper trading strategies with an LLM
  - goal:
    - demonstrate how to secuerly automate business logic within a minions system using an a LLM (in this case: trading strategy ideation and validation)
  - design:
    - the llm generates StrategySpecs not python code directly
    - StrategySpec events are emited by a pipeline (`StrategySpecPipeline(Pipeline[StrategySpec])`)
    - StrategySpec events are recieved, run, and managed by a minion `StrategySpecRunnerMinion(Minion[StrategySpec, Ctx])`
  - note:
    - ideally sample minion system examples should be runable / have tests to assert behaviour 
  - note (discuss with gpt): consider that emiting StrategySpec events can take a non-trivial amount of time. currenlty, emitting pipeline events has no resumeability because it happens in a single step (Pipeline.emit_event). it might be worth adding resumeablity support for pipeline events. it would basically be implemented in the same way that minions have resumeability (like @pipeline_step and last @pipeline_step must return event type instance). i don't love it because the current emit_event api is simple but maybe it's something to add support for because it seems like the only way to get resumeability for generating events. it's really something to think about.
  - convo: https://chatgpt.com/c/694e3d91-2ae0-8328-b434-72d8a30af9e2

### Misc:
- todo: comb the codebase for any remaining todo comments, they shold all be resolved by now, if not consolidate/complete them

- todo: manually audit runtime logs and ensure they read as events w/ details in kwargs (also that event msgs are lowercase)
  - ex: "async component started" , {'component': SQLiteStateStore}
  - note: would be great to enfore that quality when running gru scenarios, can be done by asserting from a set of log-msg-log-kwargs pairs, than as logs or thier kwargs are changed, test suite will catch them and will suggest to dev that changing logs msg and kwargs is a big deal - which it is since it could be a breaking change for monitoring and such

- todo: setup github repo so feature requests are surfaced thru "discussions" instead of "issues"
  - https://chatgpt.com/c/693f6fff-6bac-8333-9844-b1aade31a4d5

- todo: read the following docs for inspo on how to structure mine
  - https://fastapi.tiangolo.com/
  - https://microsoft.github.io/autogen/stable/
  - https://python-prompt-toolkit.readthedocs.io/en/master/index.html

- todo: after building all features for v0.1.0 release, read thru the docs start to finish to see if anything needs adding/updating

- todo: dogfood the runtime and refine it based on findings
