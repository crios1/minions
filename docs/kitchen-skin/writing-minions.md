# Writing Idempotent Minions

Minions persist workflow context and step index before every step, then resume from that index after restarts or failures. That gives you durability and ordering, but it **does not make your steps idempotent**—the same step can be invoked again if a run is retried. You own guarding against duplicate work.

One easy pattern is to stash completion markers in your context:

```python
from minions import Minion, minion_step

class PriceMinion(Minion[Heartbeat, WorkflowCtx]):
    simple_resource: SimpleResource

    @minion_step
    async def fetch_price(self):
        if self.context.get("price"):
            return  # already handled; safe to skip
        self.context["price"] = await self.simple_resource.get_price(self.event.timestamp)
```

You can also persist this state in your own database, cache, or ledger if that fits your workflow better. The goal is the same: make each step safe to re-run so retries, resumptions, and crashes do not produce side effects you did not intend.
