import inspect
from collections.abc import Mapping, Sequence
from typing import Awaitable, Callable, ParamSpec, TypeVar, overload

from .._utils.get_relative_module_path import get_relative_module_path
from .async_lifecycle import AsyncLifecycle
from .logger import ERROR, Logger

T = TypeVar("T")
P = ParamSpec("P")

class AsyncComponent(AsyncLifecycle):
    def __init__(self, logger: Logger):
        self._mn_logger = logger

    def _mn_require_bound_method(self, method: Callable[..., object]) -> None:
        owner = getattr(method, "__self__", None)
        if owner is self or owner is type(self):
            return
        raise TypeError(
            "run-and-log helpers require a method bound to this component instance "
            "or its class; pass self.method or type(self).method."
        )

    async def _mn_call_bound_method(
        self,
        method: Callable[..., T | Awaitable[T]],
        method_args: Sequence[object] | None = None,
        method_kwargs: Mapping[str, object] | None = None,
    ) -> T:
        args = method_args or ()
        kwargs = method_kwargs or {}
        result = method(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _mn_log_method_failure(
        self,
        method: Callable[..., object],
        error: Exception,
        log_msg: str | None = None,
        log_kwargs: Mapping[str, object] | None = None,
    ) -> None:
        rel_modpath = get_relative_module_path(type(self))
        method_name = getattr(method, "__name__", None) or type(method).__name__
        msg = log_msg or f"{type(self).__name__}.{method_name} failed ({rel_modpath})"
        kwargs = {
            **(log_kwargs or {}),
            "rel_modpath": rel_modpath,
        }
        await self._mn_logger._mn_log_exception(
            ERROR,
            msg,
            error,
            **kwargs,
        )

    @overload
    async def _mn_run_and_log_failure(
        self,
        method: Callable[P, Awaitable[T]],
        method_args: Sequence[object] | None = ...,
        method_kwargs: Mapping[str, object] | None = ...,
        log_msg: str | None = ...,
        log_kwargs: Mapping[str, object] | None = ...
    ) -> T:
        ...

    @overload
    async def _mn_run_and_log_failure(
        self,
        method: Callable[P, T],
        method_args: Sequence[object] | None = ...,
        method_kwargs: Mapping[str, object] | None = ...,
        log_msg: str | None = ...,
        log_kwargs: Mapping[str, object] | None = ...
    ) -> T:
        ...

    async def _mn_run_and_log_failure(
        self,
        method: Callable[..., T | Awaitable[T]],
        method_args: Sequence[object] | None = None,
        method_kwargs: Mapping[str, object] | None = None,
        log_msg: str | None = None,
        log_kwargs: Mapping[str, object] | None = None,
    ) -> T:
        """Run a bound instance/class method, log failures, and re-raise them."""
        self._mn_require_bound_method(method)
        try:
            return await self._mn_call_bound_method(
                method,
                method_args=method_args,
                method_kwargs=method_kwargs,
            )
        except Exception as e:
            await self._mn_log_method_failure(method, e, log_msg, log_kwargs)
            raise

    @overload
    async def _mn_safe_run_and_log_failure(
        self,
        method: Callable[P, Awaitable[T]],
        method_args: Sequence[object] | None = ...,
        method_kwargs: Mapping[str, object] | None = ...,
        log_msg: str | None = ...,
        log_kwargs: Mapping[str, object] | None = ...
    ) -> T | None:
        ...
    
    @overload
    async def _mn_safe_run_and_log_failure(
        self,
        method: Callable[P, T],
        method_args: Sequence[object] | None = ...,
        method_kwargs: Mapping[str, object] | None = ...,
        log_msg: str | None = ...,
        log_kwargs: Mapping[str, object] | None = ...
    ) -> T | None:
        ...

    async def _mn_safe_run_and_log_failure(
        self,
        method: Callable[..., T | Awaitable[T]],
        method_args: Sequence[object] | None = None,
        method_kwargs: Mapping[str, object] | None = None,
        log_msg: str | None = None,
        log_kwargs: Mapping[str, object] | None = None,
    ) -> T | None:
        """Run a bound instance/class method and log any exception it raises.

        Callers may provide `log_msg` and `log_kwargs` to enrich the fallback
        failure log with additional context.
        """
        self._mn_require_bound_method(method)
        try:
            return await self._mn_call_bound_method(
                method,
                method_args=method_args,
                method_kwargs=method_kwargs,
            )
        except Exception as e:
            await self._mn_log_method_failure(method, e, log_msg, log_kwargs)
