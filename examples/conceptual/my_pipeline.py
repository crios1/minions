import asyncio
import time
from minions import Pipeline

# startup, shutdown methods are optional
# but will probably always be used because pipelines
# need startup, shutdown, and running logic
# (running logic held in produce_event)

class MyPipeline(Pipeline):
    name = f"my-pipeline" # can add env to name with like os.getenv('ENV')

    async def startup(self):
        # setup some resource(s)
        self.interval = 60
    
    async def shutdown(self):
        # teardown some resource(s)
        pass

    async def produce_event(self):
        # long running procude some events
        await asyncio.sleep(self.interval)
        return {"timestamp": time.time()}
