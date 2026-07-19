import asyncio
from types import TracebackType
from typing import Any, Generic, TypeVar

T_Result = TypeVar("T_Result")


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


class GatedAsyncCallable(Generic[T_Result]):
    """Async test double for monkeypatching an async callable.

    Typically used with monkeypatch.setattr to replace a real async method,
    record entry, and block until the test explicitly releases it so a race
    window can be held open deterministically.
    """

    def __init__(self, result: T_Result | None = None) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.enter_count = 0
        self._result = result

    async def __call__(self, *args: Any, **kwargs: Any) -> T_Result | None:
        self.enter_count += 1
        self.entered.set()
        await self.release.wait()
        return self._result
