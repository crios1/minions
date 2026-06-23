# tests/assets

Directory-first taxonomy for test assets. A component's full module path should
provide the information needed to select and compose it without inspecting its
implementation.

- `support/`: shared support types used by assets.
- `config/`: TOML config fixtures.
- `resources/`: resource fixtures.
- `minions/`: minion fixtures.
- `pipelines/`: pipeline fixtures.
- `entrypoints/`: module-entrypoint composition fixtures.
- `events/`: shared event types used by simple-event assets.

## Component layouts

Use one taxonomy dimension per path segment.

- Minions:
  `tests/assets/minions/<workflow_shape>/<event_family>/<variant>.py`
- Pipelines:
  `tests/assets/pipelines/<production_shape>/<event_family>/<variant>.py`
- Resources:
  `tests/assets/resources/<behavior>/<variant>.py`

Contract-specific collections such as `invalid`, `entrypoints`, and
`user_guarantees` may use a purpose-first layout when composability is not the
fixture's primary concern.

The full module path should answer:

1. What component kind is this?
2. What event or data shape can it compose with?
3. What is its primary workflow, production, or resource behavior?
4. What special behavior distinguishes it from the baseline asset?

Event-family policy:
- `counter` is the default event family for orchestration scenarios.
- `simple` is a secondary event family for generic type-shape coverage.

## Path naming

- Encode selection-level traits in the path: component kind, compatibility,
  primary shape, dependencies, identity behavior, and deliberate timing,
  failure, persistence, or concurrency behavior.
- Do not encode incidental implementation details such as exact durations,
  UUID values, returned constants, or internal mechanisms.
- Name dependencies when they matter. Prefer `with_fixed_resource` over
  `resourced`, and `with_fixed_and_other_resource` over `multi_resources`.
- Order filename modifiers as `<identity>_<dependencies>_<behavior>`.
- Use `default.py` only for the clearly documented baseline of a directory.
- Use `_a/_b/_c` clones only for identity-separated equivalents, and do not
  reuse those suffixes to describe dependency selection or sharing behavior.
- Invalid-definition fixtures live under `entrypoints/invalid` and `minions/invalid` / `pipelines/invalid` as applicable.

Example:

```text
tests/assets/minions/two_steps/counter/
    identified_with_fixed_resource_slow_second_step.py
```

## Class naming

The module path owns the complete fixture description. The primary component
class inside the module uses a deliberately plain, stable role name:

- `AssetMinion`
- `AssetPipeline`
- `AssetResource`

This avoids duplicating the path in long class names and prevents a short,
partially descriptive class name from being mistaken for the fixture's full
contract.

When a test uses the class directly rather than referring to its module path,
import it with a concise descriptive alias:

```python
from tests.assets.minions.one_step.counter.with_inline_config import (
    AssetMinion as InlineConfigMinion,
)

result = await gru.start_orchestration(minion=InlineConfigMinion, ...)
```

The alias should restore the important path context that disappears at the
call site. Include structural and compatibility information when the filename
alone is generic:

```python
from tests.assets.minions.two_steps.counter.default import (
    AssetMinion as TwoStepCounterMinion,
)
```

Do not use opaque aliases such as `DefaultMinion`; `default` is meaningful only
inside its containing path. String-path references already provide the complete
context and need no alias.

Each component module should normally expose one primary framework entrypoint:

```python
class AssetMinion(...):
    ...


minion = AssetMinion
```

Prefer one primary component per asset module. Related variants should normally
be separate modules so their full paths remain unambiguous.
