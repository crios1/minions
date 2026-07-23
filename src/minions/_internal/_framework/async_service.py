import asyncio
import inspect
from collections.abc import Coroutine
from enum import Enum, auto
from typing import Any

from .._domain.exceptions import MinionsError
from .._utils.safe_cancel_task import safe_cancel_task
from .._utils.safe_create_task import safe_create_task
from .async_component import AsyncComponent
from .async_lifecycle import LifecycleCallback
from .logger import ERROR, Logger


class _AsyncServiceState(Enum):
    """Valid lifecycle flows:

    - CREATED → STARTING → RUNNING → STOPPING → STOPPED
    - CREATED → STARTING → STOPPING → STOPPED
    """

    CREATED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


class AsyncService(AsyncComponent):
    """Provide a service managed through a retained asyncio task.

    Lifecycle managers use the private protocol:

        service_task = asyncio.create_task(service._mn_serve())
        await service._mn_wait_until_running()

        # Later, when stopping:
        service_task.cancel()
        try:
            await service_task
        except asyncio.CancelledError:
            pass
        await service._mn_ensure_shutdown()

    Awaiting the service task waits for cleanup to finish. The final
    `_mn_ensure_shutdown()` is required to propagate any cleanup failure suppressed
    by `_mn_serve()` to preserve the primary stop reason.
    """

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._mn_state = _AsyncServiceState.CREATED
        self._mn_start_done = asyncio.Event()
        self._mn_stop_reason: BaseException | None = None
        self._mn_shutdown_task: asyncio.Task[None] | None = None
        self._mn_service_tasks: set[asyncio.Task[None]] = (
            set()
        )  # canonical task registry for this service; subclasses may keep
        # narrower domain-specific task views when they need isolated lifecycle
        # control
        self._mn_tasks_gate = (
            asyncio.Lock()
        )  # serializes access to domain-level tasks owned by subclasses
        # serializes reads and shutdown cleanup of service-level tasks while
        # creates and deletes happen sync on-loop
        self._mn_shutdown_grace_seconds = 1.0

    async def _mn_on_service_task_failure(
        self, exception: BaseException, task_name: str | None
    ) -> None:
        await self._mn_logger._mn_log_exception(
            ERROR,
            f"{type(self).__name__} service task failed",
            exception,
            task_name=task_name,
        )

    def _mn_mark_starting(self) -> None:
        self._mn_state = _AsyncServiceState.STARTING

    def _mn_mark_running(self) -> None:
        self._mn_state = _AsyncServiceState.RUNNING
        self._mn_start_done.set()

    def _mn_mark_stopping(self, reason: BaseException) -> None:
        self._mn_state = _AsyncServiceState.STOPPING
        self._mn_stop_reason = reason
        self._mn_start_done.set()

    def _mn_mark_stopped(self) -> None:
        self._mn_state = _AsyncServiceState.STOPPED

    async def _mn_wait_until_running(self) -> None:
        await self._mn_start_done.wait()
        if self._mn_state is _AsyncServiceState.RUNNING:
            return
        if self._mn_stop_reason is None:
            raise RuntimeError(
                f"{type(self).__name__} reached {self._mn_state.name} without a stop reason"
            )
        raise self._mn_stop_reason

    async def _mn_run(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None,
    ) -> None:
        pre_args = pre_args or []

        async def _pre() -> None:
            self._mn_validate_user_code(self.run, type(self).__module__)
            if pre:
                result = pre(*pre_args)
                if inspect.isawaitable(result):
                    await result
            self._mn_mark_running()

        await self._mn_run_lifecycle_phase(
            name="run",
            lifecycle_method=self.run,
            log_kwargs=log_kwargs,
            pre=_pre,
            post=post,
            post_args=post_args,
        )

    async def run(self) -> None:
        """Remain passively active until the service is cancelled."""
        await asyncio.Event().wait()

    async def _mn_serve(self):
        """Run the service lifecycle until termination."""
        self._mn_mark_starting()

        async def _ensure_shutdown_without_masking_stop_reason(
            phase: str,
        ) -> None:
            try:
                await self._mn_ensure_shutdown()
            except Exception as shutdown_error:
                await self._mn_logger._mn_log_exception(
                    ERROR,
                    f"{type(self).__name__} shutdown failed after {phase} termination",
                    shutdown_error,
                )

        try:
            await self._mn_startup()
        except BaseException as startup_error:
            self._mn_mark_stopping(startup_error)
            await _ensure_shutdown_without_masking_stop_reason("startup")
            raise

        try:
            await self._mn_run()
        except BaseException as run_error:
            self._mn_mark_stopping(run_error)
            await _ensure_shutdown_without_masking_stop_reason("run")
            raise

        stop_reason = MinionsError(
            f"{type(self).__module__}.{type(self).__qualname__}.run returned unexpectedly"
        )
        self._mn_mark_stopping(stop_reason)
        await _ensure_shutdown_without_masking_stop_reason("run")
        raise stop_reason

    async def _mn_ensure_shutdown(self) -> None:
        """Start cleanup once, or wait for it; this does not cancel the service task."""
        if self._mn_state is _AsyncServiceState.CREATED:
            return

        if self._mn_shutdown_task is None:
            if self._mn_state is not _AsyncServiceState.STOPPING:
                self._mn_mark_stopping(
                    MinionsError(
                        f"{type(self).__module__}.{type(self).__qualname__} stopped"
                    )
                )

            async def _shutdown_and_mark_stopped() -> None:
                try:
                    await self._mn_shutdown()
                finally:
                    self._mn_mark_stopped()

            self._mn_shutdown_task = asyncio.create_task(
                _shutdown_and_mark_stopped(),
                name=f"{type(self).__name__}:shutdown",
            )
        await asyncio.shield(self._mn_shutdown_task)

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None,
    ) -> None:
        """Implement one shutdown attempt; lifecycle callers must use `_mn_ensure_shutdown()`."""
        async def _post() -> None:
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

    def safe_create_task(
        self,
        coro: Coroutine[Any, Any, object],
        name: str | None = None,
    ) -> asyncio.Task[None]:
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
