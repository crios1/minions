from minions import Resource

from .support.mixin_spy import SpyMixin

class SimpleResource2(SpyMixin, Resource):
    name = f"simple-resource-2"
    
    async def startup(self):
        self.api = {'/price?symbol=AMD': 110.0}
    
    async def shutdown(self):
        return
    
    async def run(self):
        return

    # not tracked for latency and errors
    # user can't use private methods cuz i don't want name collisions with my framework internals
    @Resource.untracked
    async def public_helper_method1(self):
        return

    async def get_price(self):
        await self.public_helper_method1()
        return self.api.get('/price?symbol=AMD', None)
