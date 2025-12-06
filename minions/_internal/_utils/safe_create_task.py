import asyncio
import traceback
from collections.abc import Coroutine

from .._framework.logger import Logger, ERROR

def safe_create_task(coro: Coroutine, logger: Logger | None = None, name=None) -> asyncio.Task:
    "A safe wrapper around asyncio.create_task that optionally does logging."
    if name is None and hasattr(coro, "__name__"):
        name = coro.__name__

    async def wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except SystemExit as e:
            # Footgun: exit()/sys.exit()
            if logger:
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                try: await logger._log(ERROR, f"[SystemExit in Task]{f' ({name})' if name else ''}: {e}", traceback=tb)
                except Exception: pass
            # Swallow to keep the process alive.
        except Exception as e:
            msg = f"[Exception in asyncio.Task]{f' ({name})' if name else ''}: {e}"
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            if logger:
                await logger._log(ERROR, msg, traceback=tb)

    return asyncio.create_task(wrapper(), name=name)

"""
import asyncio
import traceback
from collections.abc import Coroutine

class UnsupportedUserCode(RuntimeError): ...
ERROR = "error"

def safe_create_task(coro: Coroutine, logger=None, name: str | None = None) -> asyncio.Task:
    task_name = name or getattr(coro, "__name__", None) or getattr(getattr(coro, "cr_code", None), "co_name", None)

    async def wrapper():
        try:
            return await coro
        except asyncio.CancelledError:
            # Let cancels propagate so shutdown/timeouts work.
            raise
        except SystemExit as e:
            # Footgun: exit()/sys.exit()
            if logger:
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                try: await logger._log(ERROR, f"[SystemExit in Task]{f' ({task_name})' if task_name else ''}: {e}", traceback=tb)
                except Exception: pass
            # Swallow to keep the process alive.
        except BaseException as e:
            # Optional: allow Ctrl-C to stop the program
            if isinstance(e, KeyboardInterrupt):
                raise
            if logger:
                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                try: await logger._log(ERROR, f"[Exception in Task]{f' ({task_name})' if task_name else ''}: {e}", traceback=tb)
                except Exception: pass

    return asyncio.create_task(wrapper(), name=task_name)

You can “disable Ctrl-C,” but the better, safer UX is:
First Ctrl-C = graceful shutdown, Second Ctrl-C = hard abort.
Don't swallow it entirely—just wire it to your shutdown path.
"""