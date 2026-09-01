import asyncio


class GatedTask(asyncio.Task[None]):
    """Test task that remains pending until explicitly released.

    Intentionally subclasses asyncio.Task for a small, ergonomic test primitive.
    This bypasses asyncio.create_task() and therefore does not respect custom
    loop task factories or alternate task-creation semantics.
    Instantiating it schedules the task on the current running event loop.
    """

    def __init__(self) -> None:
        self._gate = asyncio.Event()
        super().__init__(self._wait_for_release())

    async def _wait_for_release(self) -> None:
        await self._gate.wait()

    def release(self) -> None:
        """Allow the task to finish."""
        self._gate.set()
