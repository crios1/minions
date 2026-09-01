import asyncio
import inspect
from collections.abc import Collection, Coroutine
from enum import Enum, auto
from typing import Any, ClassVar, final

from .._domain.exceptions import MinionsError, TaskCancellationErrors
from .._utils.safe_cancel_task import safe_cancel_task
from .._utils.safe_create_task import safe_create_task
from .async_component import LifecycleCallback
from .logger import ERROR, Logger
from .logger_backed_async_component import LoggerBackedAsyncComponent


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


class AsyncService(LoggerBackedAsyncComponent):
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

    _mn_non_overridable_public_names: ClassVar[frozenset[str]] = frozenset(
        {"safe_create_task"}
    )
    _mn_user_defined_construction_allowed = False

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
        self._mn_component_owned_task_cancellation_timeout_seconds = 5.0

    async def _mn_on_service_task_failure(
        self, exception: BaseException, task_name: str | None
    ) -> None:
        await self._mn_logger._mn_log_exception(
            ERROR,
            f"{type(self).__name__} service task failed",
            exception,
            task_name=task_name,
        )

    async def _mn_wait_until_tasks_idle(
        self,
        timeout: float | None = None,
        *,
        task_subset: Collection[asyncio.Task[None]] | None = None,
        timeout_msg: str | None = None,
    ) -> None:
        """Wait until the service tasks, or a subset of them, are idle."""
        deadline = (
            None
            if timeout is None
            else asyncio.get_running_loop().time() + timeout
        )

        while True:
            async with self._mn_tasks_gate:
                tasks = tuple(
                    self._mn_service_tasks
                    if task_subset is None
                    else task_subset
                )
            if not tasks:
                return

            remaining = (
                None
                if deadline is None
                else deadline - asyncio.get_running_loop().time()
            )
            if remaining is not None and remaining <= 0:
                raise TimeoutError(timeout_msg or "tasks did not become idle before timeout")

            done, pending = await asyncio.wait(tasks, timeout=remaining)
            if pending and not done:
                raise TimeoutError(timeout_msg or "tasks did not become idle before timeout")

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

            attempted: set[asyncio.Task[None]] = set()
            cancellation_errors: list[Exception] = []
            try:
                # Two bounded passes:
                # 1) cancel tasks currently tracked
                # 2) catch tasks scheduled on the next loop tick during shutdown
                for _ in range(2):
                    async with self._mn_tasks_gate:
                        tasks = [
                            task
                            for task in self._mn_service_tasks
                            if task not in attempted
                        ]
                    attempted.update(tasks)
                    if not tasks:
                        await asyncio.sleep(0)
                        continue

                    results = await asyncio.gather(
                        *[
                            safe_cancel_task(
                                task=task,
                                timeout=(
                                    self._mn_component_owned_task_cancellation_timeout_seconds
                                ),
                                logger=self._mn_logger,
                            )
                            for task in tasks
                        ],
                        return_exceptions=True,
                    )
                    cancellation_errors.extend(
                        result
                        for result in results
                        if isinstance(result, Exception)
                    )
            finally:
                async with self._mn_tasks_gate:
                    self._mn_service_tasks.clear()

            if len(cancellation_errors) == 1:
                raise cancellation_errors[0]
            if cancellation_errors:
                raise TaskCancellationErrors(cancellation_errors)

        return await super()._mn_shutdown(
            log_kwargs=log_kwargs,
            pre=pre,
            pre_args=pre_args,
            post=_post,
        )

    @final
    def safe_create_task(
        self,
        coro: Coroutine[Any, Any, object],
        name: str | None = None,
    ) -> asyncio.Task[None]:
        """Create and track a service-owned background task for failure reporting and shutdown.

        Subclasses should use this method instead of `asyncio.create_task(...)` for
        background work owned by the service. Every returned task is tracked in the
        service-wide task registry and cancelled during final shutdown.

        A subclass may also track the returned task in a narrower semantic registry
        when it needs domain-specific waiting, metrics, or shutdown ordering. Create
        the task and add it to the narrower registry without awaiting between those
        operations. If the registry may only be modified while holding an asynchronous
        lock, acquire the lock first, then create and register the task while the lock
        remains held. This prevents cancellation from leaving a task in the
        service-wide registry but not the narrower one. The narrower registry
        supplements rather than replaces the service-wide lifecycle registry.
        """
        task = safe_create_task(
            coro,
            on_failure=self._mn_on_service_task_failure,
            name=name,
        )
        self._mn_service_tasks.add(task)
        task.add_done_callback(lambda t: self._mn_service_tasks.discard(t))
        return task
