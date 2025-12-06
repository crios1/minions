import asyncio
import inspect
import traceback

from collections.abc import Coroutine
from typing import Any, Awaitable, Callable

from .async_component import AsyncComponent
from .logger import Logger, DEBUG, ERROR

from .._utils.safe_create_task import safe_create_task


class AsyncService(AsyncComponent):
    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._mn_started = asyncio.Event()
        self._mn_start_error: BaseException | None = None

    async def _mn_wait_until_started(self):
        await self._mn_started.wait()
        if self._mn_start_error:
            raise self._mn_start_error

    async def _mn_run(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ):
        pre_args = pre_args or []
        async def _pre():
            self._mn_validate_user_code(self.run, type(self).__module__)
            if pre:
                result = pre(*pre_args)
                if inspect.isawaitable(result):
                    await result
        
        await self._mn_run_lifecycle_phase(
            name="run",
            lifecyle_method=self.run,
            log_kwargs=log_kwargs,
            pre=_pre,
            post=post,
            post_args=post_args,
        )

    async def run(self):
        "Long-running loop or wait, override in user facing classes (Minion, Pipeline, Resource)"
        # self._raise_not_implemented("run", type(self))

    async def _mn_start(self):
        "Is launched as an asyncio.Task and cancelled accordingly."
        try:
            await self._mn_startup()
            self._mn_started.set()
            await self._mn_run()
        except BaseException as e:
            self._mn_start_error = e
            self._mn_started.set()
            try:
                await self._mn_shutdown()
            except Exception as shutdown_err:
                await self._mn_logger._log(
                    ERROR,
                    f"{type(self).__name__} shutdown failed during startup error recovery",
                    error_type=type(shutdown_err).__name__,
                    error_message=str(shutdown_err),
                    traceback="".join(traceback.format_exception(
                        type(shutdown_err), shutdown_err, shutdown_err.__traceback__
                    )),
                )
            raise e

    def safe_create_task(self, coro: Coroutine, name: str | None = None) -> asyncio.Task:
        "A safe wrapper around asyncio.create_task that optionally does logging."
        return safe_create_task(coro, self._mn_logger, name)
