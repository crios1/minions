import asyncio
from types import TracebackType
from typing import Any, Generic, TypeVar


T_Result = TypeVar("T_Result")


class GatedLock(asyncio.Lock):
    """asyncio.Lock test gate that holds the lock until explicitly released."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.enter_count = 0

    async def __aenter__(self) -> None:
        await self.acquire()
        self.enter_count += 1
        self.entered.set()
        await self.release_gate.wait()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


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
