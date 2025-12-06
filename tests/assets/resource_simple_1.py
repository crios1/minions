from minions import Resource

from .support.mixin_spy import SpyMixin

class SimpleResource1(SpyMixin, Resource):
    name = f"simple-resource"
    
    async def startup(self):
        self.api = {
            '/price?symbol=NVDA': 182.06
        }
    
    async def shutdown(self):
        return
    
    async def run(self):
        return

    # not tracked for latency and errors
    # user can't use private methods cuz i don't want name collisions with my framework internals
    @Resource.untracked 
    async def public_helper_method1(self):
        return

    @Resource.untracked()
    async def public_helper_method2(self):
        return

    @Resource.untracked(kwarg='kwargs')
    async def public_helper_method3(self):
        return

    async def get_price(self):
        await self.public_helper_method1()
        return self.api.get('/price?symbol=NVDA', None)