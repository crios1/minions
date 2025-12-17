from minions import Resource

from .support.mixin_spy import SpyMixin

class InvalidResource(SpyMixin, Resource):
    name = f"invalid-resource"
    
    async def startup(self):
        self.api = {
            '/price?symbol=NVDA': 182.06
        }
    
    async def shutdown(self):
        return
    
    async def run(self):
        return

    #should raise
    @Resource.untracked
    async def _private_helper_method_invalid(self):
        return
    
    #should raise
    @Resource.untracked # type: ignore
    def sync_helper_method_invalid(self):
        return 
    
    #should raise
    async def create_asyncio_task_invalid(self):
        import asyncio
        async def _(): ...
        asyncio.create_task(_())
