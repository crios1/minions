import asyncio
import inspect
from collections.abc import Awaitable, Coroutine
from typing import Any, Protocol

from .._framework.logger import ERROR, Logger


class TaskFailureHandler(Protocol):
    """Handle a swallowed task failure.

    `task_name` is the `safe_create_task(..., name=...)` value, or the
    coroutine name when one can be inferred.
    """

    def __call__(
        self, exception: BaseException, task_name: str | None
    ) -> Awaitable[None] | None: ...


def safe_create_task(
    coro: Coroutine[Any, Any, object],
    logger: Logger | None = None,
    name: str | None = None,
    on_failure: TaskFailureHandler | None = None,
) -> asyncio.Task[None]:
    """
    Create an asyncio task with a strict runtime-safety boundary for user code.

    Runtime invariant:
    user task failures must not terminate the orchestrator process.

    Behavior:
    - Propagates `asyncio.CancelledError` so normal task cancellation semantics remain intact.
    - Swallows all other user task exceptions (`SystemExit` included) after logging.
    - Emits a structured failure signal through `on_failure` so supervisors can react
      (restart/backoff/alerts) without relying only on logs.
      The callback receives `(exception, task_name)`.
    - Never allows logger or failure-hook errors to escape this boundary.
    """
    if name is None and hasattr(coro, "__name__"):
        name = coro.__name__

    async def _safe_log_exception(msg: str, exc: BaseException) -> None:
        if not logger:
            return
        try:
            await logger._mn_log_exception(ERROR, msg, exc)
        except Exception:
            pass

    async def _safe_call_failure_handler(exception: BaseException) -> None:
        if not on_failure:
            return
        try:
            maybe_awaitable = on_failure(exception, name)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception as notify_error:
            fname = f" ({name})" if name else ""
            await _safe_log_exception(
                f"[safe_create_task on_failure failed]{fname}: {notify_error}",
                notify_error,
            )

    async def wrapper() -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except SystemExit as e:
            msg = f"[SystemExit in Task]{f' ({name})' if name else ''}: {e}"
            await _safe_log_exception(msg, e)
            await _safe_call_failure_handler(e)
        except BaseException as e:
            msg = f"[Exception in asyncio.Task]{f' ({name})' if name else ''}: {e}"
            await _safe_log_exception(msg, e)
            await _safe_call_failure_handler(e)

    return asyncio.create_task(wrapper(), name=name)
