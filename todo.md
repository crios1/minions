<!-- 
  Highest Level Todo:
  - Consolidate (and spec out) my todos to establish a priority between them.
    First, I will take the todos throughout the codebase and consolidate them in this file.
    Then, I will complete the test suite refactor so following todos can be implemented end to end w/ running tests.
    Next, I'll complete partially completed endeavors like GruShell to tidy the codebase. (document grushell design and test)
    Finally, I'll complete the todos in this file end to end w/ running tests.
-->

- todo(needs spec-ing): manage OOM risk... how should the runtime react to / manage maximal memory demands?
  - design:
    - when demand is high:
      - gru rejects new start_minion requests
      - gru suspends pipelines
  - implementation / verify w/ tests:
    - ...

- todo: add gru config so users can optin to use uvloop for better performance on *nix systems (maybe 2-4x more)
  - design: 
    - user does "pip install minions[perf]" and get uvloop if not on windows
        - in pyproject.toml add to objs
          ```python
          [project.optional-dependencies]
          perf = [
            "uvloop>=0.22.1,<1.0; platform_system != 'Windows'",
          ]
          dev = [
            ...
            "uvloop>=0.22.1,<1.0",
          ]
          ```
    - gru uses uvloop if available and on a *nix system;
      gru exposed loop option with a kwarg config like <loop_config> = "auto" | "uvloop" | "asyncio"
  - implications:
    - my test suite needs to run each test twice (once w/ uvloop and again w/ asyncio loop)
    - it's the same deal with future benchmarks in a benchmarks dir, run each twice (once per loop)
    - in test suite, if uvloop not available, abort the suite and inform the user
    - in my contributing.md
      - says requires Linux/macOS or WSL2
  - convo: https://chatgpt.com/c/693a8a64-55a0-8326-b383-881b36874aec

- todo: read the following docs for inspo on how to structure mine
  - https://fastapi.tiangolo.com/
  - https://microsoft.github.io/autogen/stable/
  - https://python-prompt-toolkit.readthedocs.io/en/master/index.html

- todo: after building all features for v0.1.0 release, read thru the docs start to finish to see if anything needs adding