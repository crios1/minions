import asyncio
import sys
from typing import Any, ClassVar, NoReturn

from minions._internal._domain.types import T_Event
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics

from .pipeline_subscriber_ready_fixed_events import SubscriberReadyFixedEventsPipeline


class SubscriberReadyEventsByEmitPipeline(
    SubscriberReadyFixedEventsPipeline[T_Event],
    defer_pipeline_setup=True,
):
    """Test pipeline that configures the subscriber gate per emitted event."""

    expected_subs_by_emit: ClassVar[tuple[int, ...]] = (1,)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if "expected_subs" in cls.__dict__:
            raise TypeError(
                "SubscriberReadyEventsByEmitPipeline subclasses should set "
                "expected_subs_by_emit, not expected_subs."
            )
        if "total_events" in cls.__dict__:
            raise TypeError(
                "SubscriberReadyEventsByEmitPipeline derives total_events from "
                "expected_subs_by_emit."
            )

        cls.total_events = len(cls.expected_subs_by_emit)

    def __init__(
        self,
        pipeline_id: str,
        pipeline_module_path: str,
        metrics: Metrics,
        logger: Logger,
    ) -> None:
        super().__init__(pipeline_id, pipeline_module_path, metrics, logger)
        self._emit_index = 0

    @classmethod
    def configure_gate_by_emit(cls, *, expected_subs_by_emit: tuple[int, ...]) -> None:
        cls.expected_subs_by_emit = expected_subs_by_emit
        cls.total_events = len(expected_subs_by_emit)

    async def wait_for_expected_subscribers(self) -> None:
        expected_subs = type(self).expected_subs_by_emit[self._emit_index]
        await self.wait_for_subscribers(expected_subs)

    async def run(self) -> NoReturn:
        for _ in range(type(self).total_events):
            await self.wait_for_expected_subscribers()
            await self._mn_produce_and_handle_event()
            self._emit_index += 1

        while True:
            await asyncio.sleep(sys.maxsize)
