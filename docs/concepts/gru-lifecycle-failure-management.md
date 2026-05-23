# Gru Lifecycle Failure Management

This note captures the design deliberation around failed lifecycle operations in Gru, especially failed `start_orchestration` and `stop_orchestration` semantics.

## Core Runtime Constraint

Gru is an in-process orchestration runtime. That is the point of the model: components can share memory, resources, event loops, metrics, and state without the cost and operational overhead of microservice boundaries.

The tradeoff is that user code is trusted and cooperative inside the Python process. User code can leak memory, create untracked tasks, hold global references, suppress cancellation, or fail to release external handles from any hook or method it controls, including startup, shutdown, run loops, workflow steps, and resource methods.

Gru can deterministically clean up framework-owned state. Gru cannot deterministically clean up arbitrary user-owned side effects after user code fails or leaks inside the same process.

## Cleanup Boundary

Gru should distinguish framework cleanup from user cleanup.

Framework cleanup includes:

- removing minions, pipelines, resources, tasks, ownership maps, and refcounts from Gru's active runtime state;
- cancelling framework-created tasks;
- stopping or discarding components from normal orchestration routing;
- preserving Gru's ability to continue operating after failed lifecycle paths.

User cleanup includes:

- code inside component `shutdown` hooks;
- raw sockets, files, clients, caches, module globals, background tasks, native handles, or other objects created by user code;
- any resource that Gru did not create, track, or own.

If user cleanup fails, Gru cannot prove the process is clean. The honest contract is that Gru cleaned its own orchestration state, but the process may contain leftover user-owned state.

## Start Failure Semantics

Starting an orchestration is speculative until all required components have started and wiring is complete.

If startup fails, Gru should roll back framework-owned state so the attempted start is treated as not active. Existing pre-start components should remain available. Newly-created components should be stopped or discarded from Gru's active state.

The user-facing result should report the original startup failure. If cleanup also encountered problems, those should be surfaced or logged as cleanup failures rather than replacing the primary startup error.

Important nuance: even when Gru's rollback succeeds, user startup code may have left process side effects before it failed. Gru should not claim hard process cleanliness after arbitrary user startup failure.

## Stop Failure Semantics

Stopping is not fully transactional. Once Gru begins detaching a minion from its pipeline, cancelling tasks, decrementing refs, or stopping dependencies, it cannot reliably roll the system back to the previous running state.

The right model is fail-closed:

- remove the orchestration from active routing and ownership state;
- cancel or stop known framework tasks/components as far as Gru can;
- report failure if any user lifecycle hook or framework cleanup step failed;
- keep Gru's internal state healthy enough to continue running other work;
- make clear that the process may be polluted by user-owned leftovers.

The operator should not be expected to manage orphaned internal components as a normal workflow. If cleanup fails, the useful operator actions are to inspect logs, continue if acceptable, retry broader shutdown, or restart the Gru process for a hard cleanup boundary.

## Shutdown Failure Semantics

`Gru.shutdown()` is the final in-process cleanup boundary. It should attempt to cancel all known runtime tasks and shut down framework-owned components. It should clear Gru's active runtime state even if some cleanup steps fail.

If shutdown reports errors, that means cleanup was incomplete or uncertain. It does not necessarily mean Gru still has valid active orchestration state. After shutdown, the Gru instance should not route work.

## Result Contract Direction

The current `success=False` result is too coarse for lifecycle failures. It conflates:

- the requested operation failed;
- Gru's internal state is or is not healthy;
- user cleanup did or did not run successfully;
- the process may contain leftover user-owned state.

Future result types should distinguish these ideas without making the normal operator experience complicated. A likely direction is to preserve simple success/failure while adding structured details for degraded cleanup paths.

Useful fields may include:

- whether Gru framework state was cleaned;
- whether active orchestration routing was removed;
- cleanup errors by phase/component;
- whether process restart is recommended for a hard cleanup boundary;
- whether known framework tasks were cancelled, done, or still pending.

Example wording for a failed stop:

```text
Stop failed during component shutdown. Gru removed the orchestration from active state and cleaned framework-owned runtime state, but user cleanup did not complete. The process may contain leftover user-owned state; restart Gru for a hard cleanup boundary.
```

## Diagnostics Direction

Gru cannot perfectly identify arbitrary user memory leaks, but it can provide useful breadcrumbs in optional debug modes:

- process memory deltas before and after lifecycle operations;
- `tracemalloc` snapshot comparisons for likely allocation sites;
- still-live asyncio task inspection;
- weakref checks to see whether component objects are collectible after stop;
- managed task/resource helper APIs that make common cleanup mistakes easier to detect and avoid.

These diagnostics should not be required for the core lifecycle guarantee. They are operational aids for cooperative cleanup bugs.

## Lifecycle Leak-Check Utility Direction

Because Minions intentionally feels script-like while giving users service-like
lifecycle control, it inherits a script-like cleanup risk: if user code leaves
memory, tasks, clients, or globals behind, those leftovers remain in the shared process.

That risk becomes more visible when users start and stop orchestrations at runtime. A component with poor lifecycle cleanup may look fine during one run but leak steadily across repeated start/stop cycles.

A useful mitigation is a runtime-supported utility and cookbook for testing user-defined components for likely lifecycle leaks.

The basic utility flow should be:

1. Start an orchestration.
2. Optionally exercise it briefly.
3. Stop the orchestration.
4. Let the event loop settle and force garbage collection.
5. Measure process memory, live tasks, and retained component objects.
6. Repeat for many iterations.
7. Report suspicious monotonic growth or retained runtime objects.

This should be framed as likely leak detection, not proof of leak freedom. It can catch common lifecycle leaks in startup/shutdown paths, but it cannot prove that production workflow logic never leaks under all possible inputs or runtime conditions. (Really that's the responsibility of users not the runtime.)

Three layers are worth supporting:

- individual component lifecycle checks, where a minion, pipeline, or resource has its lifecycle hooks exercised repeatedly with the least framework context needed to instantiate it;
- minimal orchestration checks, where a minion, pipeline, or resource is tested in the smallest valid orchestration that uses it;
- scenario checks, where a realistic orchestration is repeatedly started, exercised, and stopped to catch leaks that only appear during normal workflow execution.

Individual component checks are the first diagnostic layer because they can catch direct startup/shutdown leaks without involving orchestration wiring. Minimal orchestration checks help catch leaks introduced by Gru's lifecycle wiring and component composition. Scenario checks are where responsibility begins to shift toward the application developer, because leaks may depend on system-specific workload behavior that the framework cannot infer.

Example report direction:

```text
Lifecycle leak check failed: memory rose 96 MB over 25 start/stop cycles.

Likely retained runtime objects:
- 25 instances of MyResource still alive after stop
- 25 live tasks named MyResource.background_loop
- top allocation delta: app/cache.py:42 +88 MB

Recommendation:
Check MyResource.shutdown() and ensure background tasks and caches are cleared.
```

This kind of tool reinforces the single-process Minions model. Instead of pretending Gru has hard microservice-style cleanup isolation, it gives users a practical way to validate that their cooperative lifecycle code is healthy.

## Working Design Summary

Gru should guarantee cleanup of Gru-owned orchestration state, not arbitrary Python process state.

Failed start should be treated as a rolled-back orchestration attempt, with the process possibly polluted if user startup code left side effects.

Failed stop should be treated as a fail-closed orchestration removal, with Gru remaining internally healthy and the process possibly polluted if user shutdown code failed.

Failed shutdown should release Gru's active runtime state and report cleanup errors honestly; process restart is the only hard boundary for arbitrary user-owned leftovers.
