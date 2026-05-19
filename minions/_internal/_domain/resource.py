
import inspect
import time
from typing import Awaitable, Callable, Coroutine, ParamSpec, TypeGuard, TypeVar, Any, overload

from .._framework.async_service import AsyncService
from .._framework.async_lifecycle import LifecycleCallback
from .._framework.logger import Logger, WARNING
from .._framework.metrics import Metrics
from .._framework.metrics_constants import (
    LABEL_RESOURCE, LABEL_RESOURCE_METHOD, LABEL_ERROR_TYPE,
    RESOURCE_SERVES_TOTAL, RESOURCE_LATENCY_SECONDS, RESOURCE_ERROR_TOTAL
)


T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")


class Resource(AsyncService):
    """
    A Resource is a shared async service used by multiple minions.
    Subclasses typically expose async methods like `get_price()` or `fetch_data()`.
    All public async methods are wrapped with latency/error tracking by default.
    - Use `@Resource.untracked` as a decorator to opt out of tracking for specific methods.
    """
    _mn_user_facing = True

    def __init__(self, logger: Logger, metrics: Metrics, resource_modpath: str):
        super().__init__(logger)
        self._mn_metrics = metrics
        self._mn_resource_modpath = resource_modpath

    async def _mn_startup(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None
    ) -> None:
        return await super()._mn_startup(
            log_kwargs={'resource_id': self._mn_resource_modpath},
            pre=self._mn_validate_and_wrap_public_async_methods
        )

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None
    ) -> None:
        return await super()._mn_shutdown(
            log_kwargs={'resource_id': self._mn_resource_modpath}
        )

    async def _mn_run(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None
    ) -> None:
        return await super()._mn_run(log_kwargs={'resource_id': self._mn_resource_modpath})

    def _mn_validate_and_wrap_public_async_methods(self) -> None:
        def _is_async_callable(obj: object) -> TypeGuard[Callable[..., Coroutine[Any, Any, object]]]:
            return inspect.iscoroutinefunction(obj)

        for attr_name in dir(self):
            if attr_name.startswith("_"):
                continue
            method = getattr(self, attr_name)
            if not _is_async_callable(method):
                continue
            if getattr(method, "__untracked__", False):
                continue

            self._mn_validate_user_code(method, self._mn_resource_modpath)

            def make_wrapper(
                attr_name: str,
                method: Callable[..., Coroutine[Any, Any, object]],
            ) -> Callable[..., Coroutine[Any, Any, object]]:
                async def wrapper(*args: object, **kwargs: object) -> object:
                    return await self._mn_run_with_tracking(
                        type(self).__name__,
                        attr_name,
                        method,
                        *args,
                        **kwargs
                    )
                return wrapper

            setattr(self, attr_name, make_wrapper(attr_name, method))

    async def _mn_run_with_tracking(
        self,
        resource: str,
        resource_method: str,
        method: Callable[..., Coroutine[Any, Any, T]],
        *args: object,
        **kwargs: object
    ) -> T:
        start = time.monotonic()
        try:
            result = await method(*args, **kwargs)
        except Exception as e:
            await self._mn_metrics._mn_inc(
                metric_name=RESOURCE_ERROR_TOTAL,
                labels={
                    LABEL_RESOURCE: resource,
                    LABEL_RESOURCE_METHOD: resource_method,
                    LABEL_ERROR_TYPE: type(e).__name__,
                },
            )
            await self._mn_logger._mn_log_exception(
                WARNING,
                "Resource method failed",
                e,
                resource=resource,
                resource_method=resource_method,
                args=args,
                kwargs=kwargs
            )
            raise
        else:
            duration = time.monotonic() - start
            await self._mn_metrics._mn_observe(
                metric_name=RESOURCE_LATENCY_SECONDS,
                value=duration,
                labels={
                    LABEL_RESOURCE: resource,
                    LABEL_RESOURCE_METHOD: resource_method,
                },
            )
            await self._mn_metrics._mn_inc(
                metric_name=RESOURCE_SERVES_TOTAL,
                labels={
                    LABEL_RESOURCE: resource,
                    LABEL_RESOURCE_METHOD: resource_method,
                },
            )
            return result
    
    @staticmethod
    @overload
    def untracked(func: Callable[P, Awaitable[R]], /) -> Callable[P, Awaitable[R]]:
        ... # pragma: no cover

    @staticmethod
    @overload
    def untracked(**kwargs: Any) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        ... # pragma: no cover

    @staticmethod
    def untracked(
        func: Callable[P, Awaitable[R]] | None = None, /, **kwargs: Any
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]] | Callable[P, Awaitable[R]]:
        """
        A decorator used to opt out of latency and error tracking on a Resource method.
        """

        def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            if not inspect.iscoroutinefunction(fn):
                raise TypeError(
                    f"@untracked must be used on async functions, got: {fn.__name__}"
                )
            setattr(fn, "__untracked__", True)
            return fn

        # Support @untracked
        if func is not None:
            return decorator(func)
    
        # Support @untracked() and @untracked(**kwargs) in future
        return decorator
