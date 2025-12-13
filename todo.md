<!-- 
  Highest Level Todo:
  - Consolidate (and spec out) my todos to establish a priority between them.
    First, I will take the todos throughout the codebase and consolidate them in this file. (there are some todos in my brower tabs as chat gpt convos too)
    Then, I will complete the test suite refactor so following todos can be implemented end to end w/ running tests.
    Next, I'll complete partially completed endeavors like GruShell to tidy the codebase. (document grushell design and test)
    Finally, I'll complete the todos in this file end to end w/ running tests.
-->

- todo: update my docs and readme to the approach laid out in index_new.md 

- todo(needs spec-ing?): complete test suite refactor

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

- todo(needs spec-ing): manage OOM risk... how should the runtime react to / manage maximal memory demands? (is probably partially implemented)
  - design:
    - when demand is high:
      - gru rejects new start_minion requests
      - gru suspends pipelines
  - implementation / verify w/ tests:
    - ...

- todo: add support for resourced pipelines and resourced resources (currenlty partially implemented)

- todo: add "crash testing" to test suite to ensure that minions runtime does the runtime crash guarentees

- todo: add gru config so users can optin to use uvloop for better performance on *nix systems (maybe 2-4x more)
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

- todo: read the following docs for inspo on how to structure mine
  - https://fastapi.tiangolo.com/
  - https://microsoft.github.io/autogen/stable/
  - https://python-prompt-toolkit.readthedocs.io/en/master/index.html

- todo: after building all features for v0.1.0 release, read thru the docs start to finish to see if anything needs adding