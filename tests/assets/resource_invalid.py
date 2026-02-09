from .support.resource_spied import SpiedResource

class InvalidResource(SpiedResource):
    name = "invalid-resource"
    
    async def startup(self):
        self.api = {
            '/price?symbol=NVDA': 182.06
        }
    
    async def shutdown(self):
        return
    
    async def run(self):
        return

    # TODO: should probably have seperate invalid resource assets for each of these invalid cases

    #should raise
    @SpiedResource.untracked
    async def _mn_attrspace_method_invalid(self):
        return
    
    #should raise
    @SpiedResource.untracked # type: ignore
    def sync_method_invalid(self):
        return 
    
    #should raise
    async def create_asyncio_task_invalid(self):
        import asyncio
        async def _(): ...
        asyncio.create_task(_())
