import asyncio
import inspect
import traceback
from collections.abc import Awaitable, Callable
from collections.abc import Coroutine

from .._framework.logger import Logger, ERROR

def safe_create_task(
    coro: Coroutine,
    logger: Logger | None = None,
    name: str | None = None,
    on_failure: Callable[[BaseException, str | None, str], Awaitable[None] | None] | None = None,
) -> asyncio.Task:
    """
    Create an asyncio task with a strict runtime-safety boundary for user code.

    Runtime invariant:
    user task failures must not terminate the orchestrator process.

    Behavior:
    - Propagates `asyncio.CancelledError` so normal task cancellation semantics remain intact.
    - Swallows all other user task exceptions (`SystemExit` included) after logging.
    - Emits a structured failure signal through `on_failure` so supervisors can react
      (restart/backoff/alerts) without relying only on logs.
    - Never allows logger or failure-hook errors to escape this boundary.
    """
    if name is None and hasattr(coro, "__name__"):
        name = coro.__name__

    async def _safe_log(msg: str, tb: str) -> None:
        if not logger:
            return
        try:
            await logger._log(ERROR, msg, traceback=tb)
        except Exception:
            # Task safety takes priority; logging failures must never escape.
            pass

    async def _safe_notify(exception: BaseException, tb: str) -> None:
        if not on_failure:
            return
        try:
            maybe_awaitable = on_failure(exception, name, tb)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception as notify_error:
            notify_tb = "".join(
                traceback.format_exception(type(notify_error), notify_error, notify_error.__traceback__)
            )
            await _safe_log(
                f"[safe_create_task on_failure failed]{f' ({name})' if name else ''}: {notify_error}",
                notify_tb,
            )

    async def wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except SystemExit as e:
            # Footgun: exit()/sys.exit()
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            await _safe_log(f"[SystemExit in Task]{f' ({name})' if name else ''}: {e}", tb)
            await _safe_notify(e, tb)
            # Swallow to keep the process alive.
        except BaseException as e:
            msg = f"[Exception in asyncio.Task]{f' ({name})' if name else ''}: {e}"
            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            await _safe_log(msg, tb)
            await _safe_notify(e, tb)

    return asyncio.create_task(wrapper(), name=name)
