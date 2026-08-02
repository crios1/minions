import asyncio
import inspect
import sys
from collections.abc import Coroutine
from typing import Any

from .task_failure_handler import TaskFailureHandler


def safe_create_task(
    coro: Coroutine[Any, Any, object],
    *,
    on_failure: TaskFailureHandler,
    name: str | None = None,
) -> asyncio.Task[None]:
    """
    Create an asyncio task with a strict runtime-safety boundary for user code.

    Runtime invariant:
    user task failures must not terminate the orchestrator process.

    Behavior:
    - Propagates `asyncio.CancelledError` from the task or its failure handler so
      lifecycle cancellation semantics remain intact.
    - Sends every other `BaseException` to the required `on_failure` handler. The
      handler exclusively owns logging, recovery, and other failure effects.
    - After a non-cancellation failure is handled, the returned supervision task
      completes normally.
    - Contains non-cancellation handler failures and reports them to stderr as a
      last resort so a secondary failure cannot replace the original task outcome.
    """
    if name is None and hasattr(coro, "__name__"):
        name = coro.__name__

    def _report_failure_handler_error(
        task_error: BaseException,
        handler_error: BaseException,
    ) -> None:
        task_label = f" ({name})" if name else ""
        try:
            print(
                f"[safe_create_task failure handler failed]{task_label}: "
                f"{type(handler_error).__name__}: {handler_error}; "
                f"original task failure: {type(task_error).__name__}: {task_error}",
                file=sys.stderr,
            )
        except BaseException:
            pass

    async def _call_failure_handler(exception: BaseException) -> None:
        try:
            maybe_awaitable = on_failure(exception, name)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except asyncio.CancelledError:
            raise
        except BaseException as handler_error:
            _report_failure_handler_error(exception, handler_error)

    async def wrapper() -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            await _call_failure_handler(e)

    return asyncio.create_task(wrapper(), name=name)
