import asyncio
import inspect

from collections.abc import Coroutine
from typing import Any, Awaitable, Callable

from .async_component import AsyncComponent
from .logger import Logger, ERROR

from .._utils.format_exception_traceback import format_exception_traceback
from .._utils.safe_cancel_task import safe_cancel_task
from .._utils.safe_create_task import safe_create_task


class AsyncService(AsyncComponent):
    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._mn_started = asyncio.Event()
        self._mn_start_error: BaseException | None = None
        self._mn_service_tasks: set[asyncio.Task] = set()  # canonical task registry for this service; subclasses may keep narrower domain-specific task views when they need isolated lifecycle control
        self._mn_tasks_gate = asyncio.Lock()  # serializes access to domain-level tasks owned by subclasses; serializes reads and shutdown cleanup of service-level tasks while creates and deletes happen sync on-loop
        self._mn_shutdown_grace_seconds = 1.0

    async def _mn_on_service_task_failure(self, exception: BaseException, task_name: str | None, tb: str) -> None:
        await self._mn_logger._log(
            ERROR,
            f"{type(self).__name__} service task failed",
            task_name=task_name,
            error_type=type(exception).__name__,
            error_message=str(exception),
            traceback=tb,
        )

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
    ) -> None:
        pre_args = pre_args or []
        async def _pre():
            self._mn_validate_user_code(self.run, type(self).__module__)
            if pre:
                result = pre(*pre_args)
                if inspect.isawaitable(result):
                    await result
        
        await self._mn_run_lifecycle_phase(
            name="run",
            lifecycle_method=self.run,
            log_kwargs=log_kwargs,
            pre=_pre,
            post=post,
            post_args=post_args,
        )

    async def run(self) -> None:
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
                    traceback=format_exception_traceback(shutdown_err),
                )
            raise e

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ) -> None:
        async def _post():
            if post:
                post_args_list = post_args or []
                result = post(*post_args_list)
                if inspect.isawaitable(result):
                    await result

            # Two bounded passes:
            # 1) cancel tasks currently tracked
            # 2) catch tasks scheduled on the next loop tick during shutdown
            for _ in range(2):
                async with self._mn_tasks_gate:
                    tasks = list(self._mn_service_tasks)
                if not tasks:
                    await asyncio.sleep(0)
                    continue

                await asyncio.gather(
                    *[
                        safe_cancel_task(
                            task=task,
                            label=getattr(task, "get_name", lambda: "task")(),
                            timeout=self._mn_shutdown_grace_seconds,
                            logger=self._mn_logger,
                        )
                        for task in tasks
                    ],
                    return_exceptions=True,
                )

            async with self._mn_tasks_gate:
                self._mn_service_tasks.clear()

        return await super()._mn_shutdown(
            log_kwargs=log_kwargs,
            pre=pre,
            pre_args=pre_args,
            post=_post,
        )

    def safe_create_task(self, coro: Coroutine, name: str | None = None) -> asyncio.Task:
        "A safe wrapper around asyncio.create_task that optionally logs exceptions."
        task = safe_create_task(
            coro,
            self._mn_logger,
            name,
            on_failure=self._mn_on_service_task_failure,
        )
        self._mn_service_tasks.add(task)
        task.add_done_callback(lambda t: self._mn_service_tasks.discard(t))
        return task
