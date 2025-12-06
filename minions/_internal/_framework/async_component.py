import inspect
import traceback
from typing import ParamSpec, TypeVar, Callable, Awaitable, overload

from .async_lifecycle import AsyncLifecycle
from .logger import Logger, ERROR
from .._utils.get_relative_module_path import get_relative_module_path

T = TypeVar("T")
P = ParamSpec("P")

class AsyncComponent(AsyncLifecycle):
    def __init__(self, logger: Logger):
        self._mn_logger = logger

    @overload
    async def _mn_safe_run_and_log(
        self,
        method: Callable[P, Awaitable[T]],
        method_args: list | None = ...,
        method_kwargs: dict | None = ...,
        log_kwargs: dict | None = ...
    ) -> T | None:
        ...
    
    @overload
    async def _mn_safe_run_and_log(
        self,
        method: Callable[P, T],
        method_args: list | None = ...,
        method_kwargs: dict | None = ...,
        log_kwargs: dict | None = ...
    ) -> T | None:
        ...

    async def _mn_safe_run_and_log(
        self,
        method: Callable[..., T | Awaitable[T]],
        method_args: list | None = None,
        method_kwargs: dict | None = None,
        log_kwargs: dict | None = None,
    ) -> T | None:
        "only use on class/instance methods (supports sync and async methods)"
        method_args = method_args or []
        method_kwargs = method_kwargs or {}
        log_kwargs = log_kwargs or {}
        try:
            result = method(*method_args, **method_kwargs)
            if inspect.isawaitable(result):
                return await result
            return result
        except Exception as e:
            rel_modpath = get_relative_module_path(type(self))
            method_name = getattr(method, "__name__", None) or type(method).__name__
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__}.{method_name} failed ({rel_modpath})",
                error_type=type(e).__name__,
                error_message=str(e),
                traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
                **log_kwargs
            )
