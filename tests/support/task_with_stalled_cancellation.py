import asyncio
from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Protocol


class TaskFactory(Protocol):
    def __call__(
        self,
        coro: Coroutine[Any, Any, object],
        name: str | None = None,
    ) -> asyncio.Task[None]: ...


@asynccontextmanager
async def task_with_stalled_cancellation(
    *,
    name: str | None = None,
    task_factory: TaskFactory | None = None,
) -> AsyncGenerator[asyncio.Task[None]]:
    allow_completion = asyncio.Event()

    async def stall_after_cancellation() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await allow_completion.wait()

    coro = stall_after_cancellation()
    task = (
        asyncio.create_task(coro, name=name)
        if task_factory is None
        else task_factory(coro, name=name)
    )
    await asyncio.sleep(0)
    try:
        yield task
    finally:
        allow_completion.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
