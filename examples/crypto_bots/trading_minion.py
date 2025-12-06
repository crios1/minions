from minions import Minion, minion_step

from .prices_resource import PricesResource

class MyMinion(Minion):
    prices_resource: PricesResource

    @minion_step
    async def fetch_data(self, context):
        symbol = context["event"]["symbol"]
        context["data"] = await self.prices_resource.get_price(symbol)

    @minion_step
    async def maybe_trade(self, context):
        if context["decision"] == "buy":
            ...
