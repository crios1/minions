import asyncio
import sys
import traceback
import types
from typing import Any

from .._framework.logger import ERROR, Logger


def _format_task_stack(task: asyncio.Task[Any]) -> str:
    coro = task.get_coro()
    frame = getattr(coro, "cr_frame", None) if isinstance(coro, types.CoroutineType) else None
    return "".join(traceback.format_stack(frame)) if frame else "<no traceback>"


async def safe_cancel_task(
    task: asyncio.Task[Any],
    label: str = "task",
    timeout: float = 60.0,
    logger: Logger | None = None,
) -> None:
    if not task:
        return
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except asyncio.CancelledError:
        pass
    except asyncio.TimeoutError as e:
        msg = (
            f"Timeout while cancelling task '{label}'"
            if label != "task"
            else "Timeout while cancelling task"
        )
        task_stack = _format_task_stack(task)

        if logger:
            await logger._mn_log_exception(ERROR, msg, e, task_stack=task_stack)
        else:
            print(msg, file=sys.stderr)
            print(task_stack, file=sys.stderr)
