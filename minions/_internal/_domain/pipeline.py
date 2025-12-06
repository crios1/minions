import asyncio

from abc import abstractmethod
from typing import Awaitable, Callable, Generic, Mapping, Any, get_args, get_origin

from .types import T_Event
from .minion import Minion
from .._framework.async_service import AsyncService
from .._framework.logger import Logger, DEBUG, INFO, WARNING, ERROR, CRITICAL
from .._framework.metrics import Metrics
from .._framework.metrics_constants import (
    LABEL_MINION_INSTANCE_ID, LABEL_PIPELINE,
    PIPELINE_ERROR_TOTAL, PIPELINE_EVENT_PRODUCED_TOTAL,
    PIPELINE_EVENT_FANOUT_TOTAL
)
from .._utils.serialization import is_type_serializable

class Pipeline(AsyncService, Generic[T_Event]):
    def __init_subclass__(cls, *, defer_pipeline_setup=False, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._mn_validate_user_annotations()

        if defer_pipeline_setup:
            return

        err = TypeError(
            f"{cls.__name__} must declare an event type.\n"
            f"Example: class MyPipeline(Pipeline[MyEvent])"
        )

        for base in getattr(cls, "__orig_bases__", []):
            if get_origin(base) is Pipeline:
                args = get_args(base)
                if len(args) < 1:
                    raise err
                cls._mn_event_cls = args[0]
                break
        
        if not getattr(cls, '_mn_event_cls', None):
            raise err

        if not is_type_serializable(cls._mn_event_cls):
            raise TypeError(
                f"{cls.__name__}: event type is not JSON-serializable. "
                "Only JSON-safe types are supported (str, int, float, bool, None, list, tuple, dict[str, V], dataclass, TypedDict)."
            )

    def __init__(
        self,
        pipeline_id: str,
        pipeline_modpath: str,
        metrics: Metrics,
        logger: Logger
    ):
        super().__init__(logger)

        self._mn_pipeline_id = pipeline_id
        self._mn_pipeline_modpath = pipeline_modpath
        self._mn_metrics = metrics
        self._mn_logger = logger
        self._mn_subs: set[Minion] = set()
        self._mn_subs_lock = asyncio.Lock()
        self._mn_event_cls = type(self)._mn_event_cls
    
    async def _mn_startup(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ):
        return await super()._mn_startup(
            log_kwargs={'pipeline_id': self._mn_pipeline_id},
            pre=self._mn_validate_user_code,
            pre_args=[self.produce_event, self._mn_pipeline_modpath]
        )

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ):
        return await super()._mn_shutdown(
            log_kwargs={'pipeline_id': self._mn_pipeline_id}
        )

    async def _mn_run(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ):
        return await super()._mn_run(
            log_kwargs={'pipeline_id': self._mn_pipeline_id}
        )

    async def run(self):
        while True:
            try:
                event = await self.produce_event()
                
                await self._mn_logger._log(
                    DEBUG,
                    "Pipeline produced event",
                    pipeline_id=self._mn_pipeline_id,
                    pipeline_modpath=self._mn_pipeline_modpath,
                    event=repr(event)
                )
            except Exception:
                await self._mn_metrics._inc(
                    metric_name=PIPELINE_ERROR_TOTAL,
                    LABEL_PIPELINE=self._mn_pipeline_id
                )
                raise
            else:
                await self._mn_metrics._inc(
                    metric_name=PIPELINE_EVENT_PRODUCED_TOTAL,
                    LABEL_PIPELINE=self._mn_pipeline_id
                )
                async with self._mn_subs_lock:
                    for minion in self._mn_subs:
                        self.safe_create_task(minion._mn_handle_event(event))
                        await self._mn_metrics._inc(
                            metric_name=PIPELINE_EVENT_FANOUT_TOTAL,
                            LABEL_PIPELINE=self._mn_pipeline_id,
                            LABEL_MINION_INSTANCE_ID=minion._mn_minion_instance_id
                        )
                    await asyncio.gather(*[
                        self._mn_logger._log(
                            DEBUG,
                            "Pipeline Fanout: dispatched event to minion",
                            pipeline_id=self._mn_pipeline_id,
                            minion_name=minion._mn_name,
                            minion_instance_id=minion._mn_minion_instance_id,
                            minion_composite_key=minion._mn_minion_composite_key,
                            minion_modpath=minion._mn_minion_modpath
                        )
                        for minion in self._mn_subs
                    ], return_exceptions=True)

    async def _mn_subscribe(self, minion: Minion):
        async with self._mn_subs_lock:
            self._mn_subs.add(minion)

    async def _mn_unsubscribe(self, minion: Minion):
        async with self._mn_subs_lock:
            self._mn_subs.discard(minion)

    @abstractmethod
    async def produce_event(self) -> T_Event:
        """override to create your own Pipeline"""
        # TODO: will be long running like websocket event listener or polling?
