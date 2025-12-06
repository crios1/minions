from minions import Resource

class BinanceAPI(Resource):
    
    async def startup(self):
        self.api = dict()

    async def _private_helper(self):
        pass

    @Resource.untracked
    async def public_helper(self) -> str:
        return "not tracked"

    async def get_price(self, symbol: str):
        return self.api.get(f"/price?symbol={symbol}")
    
    async def get_volume(self, symbol: str):
        return self.api.get(f"/volume?symbol={symbol}")

    async def buy(self, symbol: str):
        return