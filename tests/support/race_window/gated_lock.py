import asyncio
from types import TracebackType


class GatedLock(asyncio.Lock):
    """asyncio.Lock test gate that holds the lock until explicitly released."""

    def __init__(self) -> None:
        super().__init__()
        self._held = asyncio.Event()
        self._allow_progress = asyncio.Event()
        self.enter_count = 0

    async def __aenter__(self) -> None:
        await self.acquire()
        self.enter_count += 1
        self._held.set()
        await self._allow_progress.wait()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    async def wait_until_held(self, timeout: float = 1.0) -> None:
        """Wait until production code holds the lock at the test gate."""
        await asyncio.wait_for(self._held.wait(), timeout=timeout)

    def allow_progress(self) -> None:
        """Release the test gate so the lock holder can continue."""
        self._allow_progress.set()
