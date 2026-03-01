import asyncio

from minions._internal._domain.types import T_Event
from .pipeline_spied import SpiedPipeline


class ScriptedSpiedPipeline(SpiedPipeline[T_Event], defer_pipeline_setup=True):
    """Test-only bounded runner for deterministic pipeline fixtures.

    Subclasses implement `produce_event` and can set `total_events`.
    The base `run` loop emits exactly `total_events` events, then blocks.
    """

    total_events = 1

    async def run(self):
        for _ in range(type(self).total_events):
            await self._mn_produce_and_handle_event()

        while True:
            await asyncio.sleep(3600)
