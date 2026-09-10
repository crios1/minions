# Contributing to Minions

Thanks for your interest in contributing to **Minions**.

Minions is early-stage, opinionated, and evolving quickly. Contributions are welcome, but the bar for acceptance is correctness, simplicity, and alignment with the project’s core goals.

## Scope

Good candidates for contributions:

* Bug fixes
* Documentation improvements
* Small, well-scoped enhancements
* Performance or correctness improvements

For larger changes, new features, or architectural ideas, **please open an issue first** to discuss direction before writing code.

## Design Philosophy

Minions prioritizes:

* Simplicity over abstraction
* Clear lifecycle and ownership of state
* Async-native, single-process systems
* Practical correctness over theoretical completeness

If a change adds complexity without clear benefit, it is unlikely to be accepted.

## Code Expectations

* Keep changes focused and minimal
* Follow existing patterns and structure
* Avoid over-engineering
* Tests (where applicable) should pass

## Commit Messages

Commit subjects should stand on their own. They should identify the affected scope,
state the actual behavior or contract change, and include the triggering condition or
reason when that distinction matters. Explicitly distinguish production code, test
code, test support, benchmarks, documentation, and tooling when the scope could
otherwise be ambiguous.

Do not make the body carry information that is necessary to understand the subject.
Avoid vague subjects built around words such as *update*, *improve*, *handle*,
*support*, or *align* unless their object and resulting behavior are explicit. Use the
body for supporting detail, implementation context, or evidence—not to rescue an
underspecified subject.

Before finalizing a commit message, check that a reader could understand the change
without seeing the diff and could not reasonably confuse a test-only change with a
production change.

For example, prefer:

```text
Use StartResult.cause instead of its generic reason in the subprocess recovery test runner
```

over:

```text
Check StartResult.cause in subprocess recovery
```

## Local Setup

For development, use a fresh virtual environment and install Minions in editable mode so
`python -m minions` and the test suite resolve the package from `src/`:

```bash
python3.12 -m venv .venv
./.venv/bin/python -m pip install -U pip setuptools wheel
./.venv/bin/python -m pip install -e ".[dev]"
./.venv/bin/python -m pytest
```

Run `pytest` from the repository root. The test suite relies on shared fixtures under
`tests/assets`, so the repo root must be on the import path during test runs.

There is no strict style guide beyond consistency with the existing codebase.

## Process

1. Fork the repository
2. Create a focused branch
3. Open a PR with a clear explanation of *what* and *why*

PRs may be declined if they don’t align with the project’s direction. This isn’t personal — Minions is intentionally curated.

## Questions

If you’re unsure whether a contribution makes sense, opening an issue to ask is always welcome.

Thanks for taking the time to contribute.
