(concurrency-backpressure)=
# Concurrency and Backpressure

The runtime prefers to be **greedy**: pipelines keep producing, minions fan out, and workflows run concurrently. Rate limiting and backpressure live at the resource edge, not in the core scheduler. This keeps the core simple and pushes policy to where it belongs—the dependency that needs protection.

## Philosophy

- Let the orchestrator maximize throughput; avoid global limits unless you have clear evidence they help.
- If a dependency cannot handle the load, enforce limits inside the relevant `Resource` (semaphores, queues, backoffs).
- Metrics and logs surface high in-flight counts so you can spot when you need to apply brakes.

## Practical patterns

- Wrap outbound calls in resource-level semaphores or connection pools sized for the dependency.
- Add per-resource retries/backoff rather than pausing the whole pipeline.
- Emit warnings when resuming a large number of workflows after a crash; then rely on resources to smooth the recovery.
- When you truly need a hard cap, put it inside a Resource so fanout continues but individual calls queue at the edge.

## Monitoring hooks

Gru exports Prometheus gauges and counters for workflows, pipeline fanout, and resource latency/errors. Pair them with alerts for runaway concurrency or error spikes.
