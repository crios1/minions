from minions import Minion, minion_step

from .binance_api import BinanceAPI

class MyMinion(Minion):
    binance_api: BinanceAPI

    @minion_step
    async def fetch_data(self, ctx: dict):
        symbol = ctx["event"]["symbol"]
        ctx["data"] = await self.binance_api.get_price(symbol)

    @minion_step
    async def maybe_trade(self, ctx: dict):
        if ctx["decision"] == "buy":
            await self.binance_api.buy(ctx["event"]["symbol"])
