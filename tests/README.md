# Test Suite Scope

This suite is intentionally layered. Each layer has a different purpose and confidence target.

## 1) Runtime Contract Layer (DSL, deterministic, in-memory/spied)
- Purpose: prove orchestration semantics are correct.
- This is the strongest correctness proof for lifecycle, fanout, and verification guarantees.
- Canonical areas:
  - `tests/support/gru_scenario/*` (DSL internals)
  - `tests/minions/_internal/_domain/gru/test_usage_*.py` (DSL usage scenarios)
- Default test components here are in-memory + spy-capable because deterministic verification is required.

## 2) Component Contract Layer (unit tests per implementation)
- Purpose: prove each backend/component is correct in isolation.
- Covers shipped implementations directly (for example sqlite state store, metrics, logger, utility/framework modules).
- Canonical areas:
  - `tests/minions/_internal/_framework/*`
  - `tests/minions/_internal/_utils/*`
  - domain-focused unit tests outside DSL usage files.

## 3) Integration Fit Layer (thin orchestration smoke with real components)
- Purpose: prove concrete backends satisfy runtime contracts when plugged into orchestration.
- Scope is intentionally small and focused.
- Not a full backend-by-backend parity copy of the DSL matrix.

## 4) Resumability Layer (state/restart semantics)
- Purpose: prove interrupted/in-flight work resumes correctly and safely.
- This is explicit coverage, not incidental behavior hidden inside broad orchestration tests.
- Includes replay scoping, resume ordering, and persistence interaction guarantees.

## 5) Workload / Blackbox Layer (predictable business outcome)
- Purpose: prove end-to-end “runtime gets work done” with predictable workloads.
- This is top-layer confidence coverage, not the sole correctness mechanism.
- Use to validate outcome realism after lower-layer contracts are already enforced.

## 6) Stress / Soak Layer (flake/race/resource behavior)
- Purpose: detect intermittent timing/race issues and long-running safety regressions.
- Scope: loop targeted high-risk tests and selected workload scenarios.
- This layer complements deterministic checks; it does not replace them.

## Scope Policy

- Core orchestration confidence comes from Layer 1 (DSL deterministic tests with in-memory/spied components).
- Backend correctness comes from Layer 2 (implementation unit tests).
- Backend orchestration fit is Layer 3 (small smoke coverage).
- Resume safety is explicitly covered in Layer 4.
- Outcome realism is Layer 5.
- Intermittent bug detection is Layer 6.

## Test Componentry Policy

- To test the runtime in `minions/` well, this repository includes non-trivial testing componentry (for example spies, in-memory stores, and scenario helpers) under `tests/`.
- The runtime itself is tested in a mirrored layout under `tests/minions/` as the primary test mirror of the `minions/` package.
- Testing componentry modules are also first-class code and must have their own tests.
- When a test componentry directory contains multiple helper modules, prefer a localized `tests/` subdirectory in that same directory to validate those helpers.
- This localized helper-test pattern is an accepted tradeoff to keep related test infrastructure and its validation close together.

## Contributor Placement Guide

1. Orchestration semantics changed:
- Add/modify Layer 1 tests first.

2. Component/backend behavior changed:
- Add/modify Layer 2 tests first.

3. Backend plug-in orchestration fit changed:
- Add/modify Layer 3 smoke tests.

4. Resume/restart behavior changed:
- Add/modify Layer 4 tests.

5. End-to-end workload behavior changed:
- Add/modify Layer 5 tests.

6. Concurrency/timing-sensitive paths changed:
- Run/expand Layer 6 stress loops.
