import asyncio
import sys
import traceback
import types

from .._framework.logger import Logger, ERROR

async def safe_cancel_task(
    task: asyncio.Task,
    label: str = "task",
    timeout: float = 60.0,
    logger: Logger | None = None
):
    if not task:
        return
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except asyncio.CancelledError:
        pass
    except asyncio.TimeoutError:
        msg = (
            f"Timeout while cancelling task '{label}'"
            if label != "task" else
            "Timeout while cancelling task"
        )

        coro = task.get_coro()
        frame = getattr(coro, "cr_frame", None) if isinstance(coro, types.CoroutineType) else None
        tb = "".join(traceback.format_stack(frame)) if frame else "<no traceback>"

        if logger:
            await logger._log(ERROR, msg, traceback=tb)
        else:
            print(msg, file=sys.stderr)
            print(tb, file=sys.stderr)
