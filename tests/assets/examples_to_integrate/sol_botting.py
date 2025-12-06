from minions import Resource, Minion, minion_step

class WebSocketProvider(Resource):
    ...

class PriceOracle(Resource):
    "caches prices to save on api calls"
    wsp: WebSocketProvider
    ...

class SkimmerBot(Minion):
    wsp: WebSocketProvider
    price_oracle: PriceOracle

    @minion_step
    async def step1(self):
        ...
