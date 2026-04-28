# Operating with Metrics

Minions exposes Prometheus-style metrics for runtime health, workflow progress, resource activity, and persistence behavior. This guide focuses on the metrics operators can use to understand whether the runtime is healthy and whether workflows are preserving their durability guarantees.

## Start with persistence metrics

If you only watch one group of metrics for workflow correctness, watch the workflow persistence metrics:

- `minion_workflow_persistence_attempts_total`
- `minion_workflow_persistence_succeeded_total`
- `minion_workflow_persistence_failures_total`
- `minion_workflow_persistence_duration_seconds`
- `minion_workflow_persistence_blocked_gauge`

These metrics describe what workflows are experiencing, not just whether the backend store itself looks healthy.

## Read the important labels first

When investigating persistence behavior, start with these labels:

- `minion_workflow_persistence_operation`: `save` or `delete`
- `minion_workflow_persistence_checkpoint_type`: `workflow_start`, `before_step`, or `workflow_resolve`
- `minion_workflow_persistence_failure_stage`: `serialize`, `save`, or `delete`
- `minion_workflow_persistence_retryable`: `true` or `false`
- `minion_workflow_persistence_policy`: `continue-on-failure` or `idle-until-persisted`
- `state_store`: backend implementation name
- `minion_composite_key`: affected minion instance/config/pipeline orchestration

Those labels tell you whether you are looking at application data that cannot be persisted, an operational store outage, or a workflow that is already done with user code and is now waiting to resolve checkpoint cleanup.

## What normal behavior looks like

Healthy systems usually show:

- `attempts_total` and `succeeded_total` increasing together
- low `duration_seconds` for both `operation="save"` and `operation="delete"`
- `failures_total` near zero or spiky but brief
- `blocked_gauge` usually at zero

Short-lived retries are expected during transient store problems. What matters is whether blocked workflows recover promptly and whether failures cluster around `save`, `delete`, or `serialize`.

## How to interpret blocked workflows

`minion_workflow_persistence_blocked_gauge` is the clearest signal that persistence is affecting workflow progress.

- `operation="save"`:
  workflows are paused before the next step because Minions cannot durably checkpoint progress.

- `operation="delete"`:
  user code has already reached a terminal outcome, but Minions is still resolving the workflow because checkpoint deletion has not succeeded yet.

- `checkpoint_type="before_step"`:
  the workflow is blocked between steps.

- `checkpoint_type="workflow_resolve"`:
  the workflow is blocked during terminal cleanup, not during user step execution.

In practice, a sustained non-zero blocked gauge for `operation="save"` is the higher-severity condition because it directly stalls workflow advancement.

## Distinguish application bugs from store outages

Use `minion_workflow_persistence_failures_total` with `failure_stage` and `retryable`:

- `failure_stage="serialize"` and `retryable="false"`:
  workflow event or context data cannot be encoded by the Minions persistence codec. This is an application/data-shape bug. Retrying will not fix it.

- `failure_stage="save"` and `retryable="true"`:
  Minions could serialize the workflow, but the StateStore failed to persist it. This usually indicates store unavailability, latency, lock contention, or another backend issue.

- `failure_stage="delete"` and `retryable="true"`:
  user code already finished, but workflow resolution is waiting on checkpoint cleanup. This is usually less urgent than blocked saves, but it still means workflows are not reaching a clean terminal state.

## Suggested alerting priorities

The exact thresholds depend on your workload, but the alert order should usually be:

1. Page on sustained blocked workflows for `operation="save"`.
2. Page on repeated non-retryable persistence failures.
3. Warn on sustained blocked workflows for `operation="delete"`.
4. Watch for rising persistence latency or failure ratios by `state_store`.

If you use `continue-on-failure`, you should also watch `failures_total` even when `blocked_gauge` stays at zero, because workflows may still be progressing while checkpoint durability is degraded.

## Match logs with metrics

The persistence logs are designed to line up with the metrics:

- `"Workflow continuing after persistence failure"`
- `"Workflow idled waiting for persistence"`
- `"Workflow persistence resumed"`
- `"Workflow idled waiting for checkpoint delete"`
- `"Workflow checkpoint delete resumed"`
- `"Workflow persistence failed with non-retryable error"`

Use the shared fields in the log payloads to correlate with metrics:

- `workflow_id`
- `checkpoint`
- `persistence_operation`
- `persistence_failure_stage`
- `persistence_retryable`
- `persistence_failure_policy`
- `state_store`

## Practical triage flow

When an operator sees persistence trouble:

1. Check whether `minion_workflow_persistence_blocked_gauge` is non-zero.
2. Split by `minion_workflow_persistence_operation` to see whether the issue is blocking workflow progress or terminal cleanup.
3. Inspect `minion_workflow_persistence_failures_total` by `minion_workflow_persistence_failure_stage` and `minion_workflow_persistence_retryable`.
4. Narrow by `state_store` and `minion_composite_key` to see whether the issue is backend-wide or isolated.
5. Use the persistence logs to confirm whether the runtime is retrying, resuming, or hitting a deterministic serialization failure.

For the underlying persistence semantics, see {doc}`/concepts/state-and-persistence`.
