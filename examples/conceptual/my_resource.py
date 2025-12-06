from minions import Resource

class MyResource(Resource):
    name = f"my-resource" # can add env to name with like os.getenv('ENV')
    
    async def startup(self): # startup lifecyle hook
        self.api = dict()
    
    async def shutdown(self): # shutdown lifecycle hook
        return
    
    async def run(self): # can be used for long running background tasks
        return

    @Resource.untracked
    async def public_helper_method(self): # user can't use private methods cuz i don't want name collisions with my framework internals
        return "not tracked for latency and errors"

    async def get_price(self, symbol: str):
        return self.api.get(f"/price?symbol={symbol}")
    
    async def get_volume(self, symbol: str):
        return self.api.get(f"/volume?symbol={symbol}")