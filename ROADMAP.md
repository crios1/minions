# Roadmap

Minions emphasizes correctness, stability, and real-world validation over rapid feature churn.

## Versioning posture

Minions Core is the active product surface. Compose and Cluster describe the long-term
execution ladder, but they are future work until Core has been exercised enough to justify
new execution layers.

Before `v0.1.0`, Core is still settling: APIs may move, semantics may sharpen, and docs may
describe the intended direction as well as the current implementation. `v0.1.0` is the first
evaluation-ready Core release. The `v0.1.x` line should remain focused on Core hardening,
compatibility, examples, and bug fixes rather than adding major architectural scope.

New execution layers, including meaningful Compose or Cluster work, should start in a later
minor line only after Core usage reveals real deployment pressure.

## v0.1.0
- Ready for evaluation and adoption
- Core model implemented, tested, and exercised in real systems

## v0.1.x
- Maintenance and hardening releases
- Bug fixes, dependency updates, and security patches
- No semantic or architectural changes

## v0.x.0
- New capabilities or core model changes
- Changes are driven by user adoption and feedback
- Breaking changes are rare, batched intentionally, and accompanied by migration guides

## v1.0.0
- Released after sustained real-world production use
- Core abstractions proven stable in practice
- Long-term API stability commitment
