import asyncio

from minions import minion_step
from tests.assets.support.minion_spied import SpiedMinion


class DictPersistenceGuaranteeMinion(SpiedMinion[dict, dict]):
    name = "dict-persistence-guarantee-minion"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        self.context["seen_kind"] = self.event["kind"]
        self.context["seen_value"] = self.event["payload"]["value"]
        if self.context != {"seen_kind": "dict-event", "seen_value": 10}:
            raise RuntimeError("dict event/context fields were not restored")
        await asyncio.sleep(0.2)


minion = DictPersistenceGuaranteeMinion
