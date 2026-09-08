import asyncio


class WaiterThresholdGate:
    """Gate that opens and remains open once the threshold of active waiters is reached."""

    def __init__(self, *, threshold: int) -> None:
        if threshold < 1:
            raise ValueError("threshold must be >= 1")
        self._threshold = threshold
        self._active_waiters = 0
        self._open = asyncio.Event()

    async def wait_until_open(self) -> None:
        """Wait until the threshold is reached, or until the gate is already open."""
        if self._open.is_set():
            return

        self._active_waiters += 1
        if self._active_waiters >= self._threshold:
            self._open.set()

        try:
            await self._open.wait()
        finally:
            self._active_waiters -= 1
