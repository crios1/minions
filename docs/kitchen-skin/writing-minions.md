# Writing Idempotent Minions

Minions persist workflow context and step index before every step, then resume from that index after restarts or failures. That gives you durability and ordering, but it **does not make your steps idempotent**—the same step can be invoked again if a run is retried. You own guarding against duplicate work.

This is intentional: the runtime has “at least once” semantics for step execution. A long-running step can be interrupted (process crash, deploy, network blip), so guaranteeing “exactly once” would be fake safety. Many real steps do I/O, HTTP calls, or wait for external state changes—any of those can be cut short. Designing for idempotency keeps your workflow correct even when a step is replayed, like when resuming inflight workflows after a power failure.

One easy pattern is to stash completion markers in your context:

```python
from minions import Minion, minion_step

class PriceMinion(Minion[Heartbeat, WorkflowCtx]):
    simple_resource: SimpleResource

    @minion_step
    async def fetch_price(self):
        if self.context.get("price"):
            return  # already handled; safe to skip
        self.context["price"] = await self.simple_resource.get_price("BTC")
```

You can also persist this state in your own database, cache, or ledger if that fits your workflow better. The goal is the same: make each step safe to re-run so retries, resumptions, and crashes do not produce side effects you did not intend.

## Reality check: contracts and limits

Minions’ honest contract is: **ordered, durable, at-least-once step execution**. Business-level safety is yours: idempotency, compensation, invariants. That mirrors microservices land—platforms can retry and queue, but only the domain owner can make side effects safe.

You can’t encode “idempotent” in an abstract base class without lying or forcing a one-size-fits-none pattern. Guidance beats inheritance here.

## Patterns to copy

- ETL / ingestion: last-seen IDs or timestamps in context/state; skip repeats.  
- Trading / order placement: idempotency keys and dedupe tables when calling exchanges.  
- Messaging / notifications: outbox tables or “already sent” markers keyed by recipient + message.  
- External APIs: request hashes and short-lived caches; ensure retries are safe.

<!-- TODO: add a reference to cookbook examples of implementing idempotency to inspire the dev -->
<!-- one of the main things to consider is data staleness -->
<!-- so each step is guarding to check that data wasn't collected already -->
<!-- but then in your execution step you need to check staleness of the data to make sure that your edge is still valid / not stale -->
<!-- at least in the case of trading bots, because you system could have been down for a while -->
<!-- and you want to resume all workflows and have them take the best actions considering thier situations -->
<!-- and checking for data staleness to ensure you still have edge is important -->
<!-- if you don't have edge, then you just cleanly abort the workflow with reason of like "edge expired" or something -->

## Safety checklist

- For every step with side effects, where is the idempotency guard or compensation logic?  
- Are your markers stored somewhere durable (context, DB, cache) that survives a restart?  
- If a step runs twice, does it double-charge, double-send, or double-book? If so, add a guard.  
- Do you have a dedupe key you can use (order_id, message_id, file checksum)?  
- Are long-running I/O steps resilient to being interrupted and replayed?
