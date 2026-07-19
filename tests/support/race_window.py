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
    record a call, and block until the test explicitly allows it to return so
    a race window can be held open deterministically. All calls share one gate;
    once opened, current and future calls may return.
    """

    def __init__(self, result: T_Result | None = None) -> None:
        self._called = asyncio.Event()
        self._allow_return = asyncio.Event()
        self.call_count = 0
        self._result = result

    async def __call__(self, *args: Any, **kwargs: Any) -> T_Result | None:
        self.call_count += 1
        self._called.set()
        await self._allow_return.wait()
        return self._result

    async def wait_until_called(self, timeout: float = 1.0) -> None:
        """Wait until production code calls the test double."""
        await asyncio.wait_for(self._called.wait(), timeout=timeout)

    def allow_return(self) -> None:
        """Release the test gate so the call can return."""
        self._allow_return.set()
