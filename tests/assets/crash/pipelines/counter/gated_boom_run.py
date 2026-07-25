import asyncio

from minions._internal._domain.types import T_Event
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class GatedBoomRunPipeline(
    SpiedPipeline[T_Event],
    defer_pipeline_setup=True,
):
    """Intermediate base: defer setup to override run; the concrete subclass binds T_Event."""

    def __init__(
        self,
        pipeline_id: str,
        pipeline_module_path: str,
        metrics: Metrics,
        logger: Logger,
    ) -> None:
        super().__init__(pipeline_id, pipeline_module_path, metrics, logger)
        self._release_run_failure = asyncio.Event()

    def trigger_run_failure(self) -> None:
        self._release_run_failure.set()

    async def run(self) -> None:
        await self._release_run_failure.wait()
        boom()


class AssetPipeline(GatedBoomRunPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        raise AssertionError("custom run does not call produce_event")


pipeline = AssetPipeline
