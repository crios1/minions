import asyncio
import sys
import traceback
import types
from typing import Any

from .._domain.exceptions import TaskCancellationTimeoutError
from .._framework.logger import ERROR, Logger


def _format_task_stack(task: asyncio.Task[Any]) -> str:
    coro = task.get_coro()
    frame = getattr(coro, "cr_frame", None) if isinstance(coro, types.CoroutineType) else None
    return "".join(traceback.format_stack(frame)) if frame else "<no traceback>"


async def safe_cancel_task(
    task: asyncio.Task[Any],
    label: str | None = None,
    timeout: float = 60.0,
    logger: Logger | None = None,
) -> None:
    if not task:
        return
    task.cancel()
    _done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        error = TaskCancellationTimeoutError(label or task.get_name(), timeout)
        task_stack = _format_task_stack(task)

        if logger:
            await logger._mn_log_exception(
                ERROR,
                str(error),
                error,
                task_stack=task_stack,
            )
        else:
            print(error, file=sys.stderr)
            print(task_stack, file=sys.stderr)
        raise error

    try:
        task.result()
    except asyncio.CancelledError:
        pass
