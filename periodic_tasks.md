# Periodic Tasks

These are recurring maintenance passes worth doing occasionally. They are not one-time todos; keep them around as project hygiene routines.

### Type Hygiene

- Task: audit unreasonable `Any` usage across the codebase
  - Goal:
    - make sure the project is not opting out of type checking where concrete types, protocols, generics, or targeted casts would preserve type safety
  - Review:
    - find explicit `Any` imports/usages, implicit untyped containers, broad `dict[str, Any]` / `Mapping[str, Any]` surfaces, and casts that hide real typing gaps
    - separate justified dynamic/plugin/serialization boundaries from avoidable internal `Any` usage
    - tighten annotations incrementally where the concrete type is clear and low risk
    - add narrow helper types, protocols, type aliases, or generic parameters when they reduce repeated `Any` use without overcomplicating the API
    - document or locally justify remaining `Any` usage at true dynamic boundaries
  - Verification:
    - run the project's Python type-check and test commands from the project venv after meaningful annotation changes
  - Why it matters:
    - excessive `Any` can silently bypass Pyright coverage and let type regressions reach runtime paths that should be checked statically

- Task: audit unreasonable mutable collection type annotations across the codebase
  - Goal:
    - make sure annotations do not imply mutation or concrete collection requirements when callers only need to provide/read a stable collection interface
  - Review:
    - find parameters and return surfaces typed as mutable concrete collections like `list`, `dict`, `set`, or `tuple` where a read-only or abstract interface would be more accurate
    - prefer `Sequence[T]` over `list[T]` when order/indexing is needed but mutation is not
    - prefer `Iterable[T]` or `Collection[T]` when callers only need iteration or membership/length semantics
    - prefer `Mapping[K, V]` over `dict[K, V]` when key/value lookup is needed but mutation is not
    - prefer `AbstractSet[T]` / `Set[T]`-style read-only interfaces over `set[T]` where membership semantics are enough
    - keep concrete mutable types where the function mutates the object, depends on concrete behavior, constructs a returned mutable value for callers to own, or intentionally documents mutability as part of the contract
    - check dataclass/msgspec fields separately from function parameters, because stored mutable fields may need concrete container types, `tuple`, defaults, or defensive copies depending on ownership semantics
    - document or locally justify mutable collection annotations at true mutation/ownership boundaries
  - Verification:
    - run the project's Python type-check and test commands from the project venv after meaningful annotation changes
  - Why it matters:
    - overusing mutable concrete collection types narrows valid callers, obscures ownership boundaries, and can hide accidental mutation that read-only interfaces would make explicit
