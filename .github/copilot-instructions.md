# Copilot / AI Agent Instructions for the Minions repository

This file gives focused, actionable guidance to an AI coding agent working on this repository so it can be productive immediately.

Keep suggestions and code changes minimal, follow existing style, and run tests locally before proposing large changes.

1) Big-picture architecture
- Core domain: `minions._internal._domain` contains the runtime primitives: `Gru`, `Minion`, `Pipeline`, and workflow/context types. Key file: `minions/_internal/_domain/gru.py` (singleton process manager).
- Framework-style helpers: `minions._internal._framework` supplies pluggable implementations for `logger`, `metrics`, and `state_store` (console, noop, in-memory, sqlite). Tests instantiate these directly (see `tests/minions/_internal/_domain/test_gru.py`).
- Tests and examples: `tests/assets` contains minimal example minion/pipeline/resource modules used by tests. Use these to prototype changes and to understand runtime loading patterns.

2) Important conventions and patterns
- Singleton Gru: `Gru.create(...)` is the factory; direct construction of `Gru(...)` raises. Only one Gru is expected per process. Tests reset `_GRU_SINGLETON` via fixture.
- Pluggable dependencies: `logger`, `metrics`, `state_store` are passed as instances to `Gru.create`. Many tests pass `NoOp*` or `InMemory*` fixtures. When adding features, preserve instance-based injection.
- Dynamic module loading: Minion and Pipeline implementations are discovered by importing modules under paths like `tests.assets.*` (dot-path strings). When testing, prefer the example assets under `tests/assets`.
- Resource sharing: Resources are keyed and shared across Minion instances by type and config; tests assert that `gru._resources` and `gru._pipelines` track sharing and cleanup.
- Testing style: tests are `pytest` + `pytest-asyncio` with `async def` tests. Use `monkeypatch` to stub `psutil` and asyncio behaviors when needed (see `test_gru.py`).

3) Typical developer workflows / commands
- Run unit tests: `pytest -q` from repository root. Tests use `pytest-asyncio`; ensure your Python >= 3.10 and dependencies from `requirements.txt` installed.
- Run a single test file: `pytest tests/minions/_internal/_domain/test_gru.py::TestValidUsage::test_gru_start_stop_minion -q`
- Run coverage: `pytest --cov=minions` (project already uses `pytest-cov` in `requirements.txt`).
- Install deps into a virtualenv: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.

4) Project-specific gotchas for code changes
- Avoid creating multiple Gru instances; tests and code rely on `_GRU_SINGLETON`. If you modify initialization logic, update tests that reset `_GRU_SINGLETON`.
- Many tests call private attributes like `gru._resources`, `_pipelines`, `_minions_by_id`, and `_minion_tasks`. Keep these structures stable or update tests accordingly.
- The codebase uses explicit NoOp/InMemory implementations for deterministic tests — prefer adding new test helpers in `minions/_internal/_framework` and referencing them from `tests` when needed.
- Use the example modules in `tests/assets` to validate dynamic-loading behavior instead of creating new files outside tests.

5) Integration points & external dependencies
- `psutil` is used for system/process metrics collection in `Gru._monitor_process_resources`. Tests monkeypatch `psutil` calls heavily. When modifying resource-monitoring, verify behavior via `tests/minions/_internal/_domain/test_gru.py`.
- `prometheus_client` appears in requirements; metrics implementations live inside `minions/_internal/_framework/metrics_*` and expose snapshots used in tests.
- `aiosqlite` is declared in `pyproject.toml` for persistence/state-store implementations.

6) Helpful example snippets (copyable)
- Create a test Gru with in-memory helpers:
```py
from minions._internal._framework.logger_inmemory import InMemoryLogger
from minions._internal._framework.state_store_inmemory import InMemoryStateStore
from minions._internal._framework.metrics_inmemory import InMemoryMetrics

gru = await Gru.create(state_store=InMemoryStateStore(logger=logger), logger=logger, metrics=InMemoryMetrics())
```
- Patch `psutil` in async tests:
```py
monkeypatch.setattr('psutil.virtual_memory', lambda: types.SimpleNamespace(percent=55, total=10_000))
monkeypatch.setattr('psutil.cpu_percent', lambda interval=None: 20)
```

7) When to update these instructions
- Update this file when adding new top-level components, changing `Gru.create` signature/semantics, or when introducing new runtime plugins (logger/metrics/state_store).

8) Preferred Python interpreter
- When running repository Python tooling (tests, scripts, linters), prefer the project's virtual environment Python at `venv/bin/python`.
- Use the full path to the venv Python when invoking commands in scripts or terminal automation to avoid ambiguity, for example:
	- `/Users/chrisrios/Documents/Code/Minions/venv/bin/python -m pytest -q`
	- `/Users/chrisrios/Documents/Code/Minions/venv/bin/python scripts/origin_demo.py`
- This repository uses a project-local venv located at `venv/` by convention; if a different path is used, update these instructions accordingly.

If anything in this document is unclear or you want more detail about a specific area (storage, metrics, or dynamic loading), tell me which area and I'll expand the instructions or merge content from an existing file.

NOTE FOR AI AGENTS

- When proposing or making changes that affect test execution (for example module reloads), explain the reason and expected outcome first, then request or perform the change. Prefer opt-in for reloads when the user hasn't explicitly asked for them.
- Keep changes minimal and local to tests/helpers when diagnosing test flakiness. Run the affected tests locally and report the exact commands and output.
- If the user says "do not change X", do not modify X. Instead propose alternatives and get explicit approval before modifying other areas.
- When in doubt, ask one clarifying question rather than performing a potentially risky global change.
