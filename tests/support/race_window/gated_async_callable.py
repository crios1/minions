import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Generic, TypeVar

T_Result = TypeVar("T_Result")


class GatedAsyncCallable(Generic[T_Result]):
    """Async test double for monkeypatching an async callable.

    It can either return a fixed result or gate a delegate. In both modes,
    calls are recorded and held until the test explicitly allows them to
    complete so a race window can be held open deterministically. All calls
    share one gate; once opened, current and future calls may complete.
    """

    def __init__(
        self,
        result: T_Result | None = None,
        *,
        delegate: Callable[..., Awaitable[T_Result]] | None = None,
    ) -> None:
        if result is not None and delegate is not None:
            raise ValueError("Specify either result or delegate, not both.")
        self._called = asyncio.Event()
        self._allow_return = asyncio.Event()
        self.call_count = 0
        self._result = result
        self._delegate = delegate

    async def __call__(self, *args: Any, **kwargs: Any) -> T_Result | None:
        self.call_count += 1
        self._called.set()
        await self._allow_return.wait()
        if self._delegate is not None:
            return await self._delegate(*args, **kwargs)
        return self._result

    async def wait_until_called(self, timeout: float = 10.0) -> None:
        """Wait until production code calls the test double."""
        await asyncio.wait_for(self._called.wait(), timeout=timeout)

    def allow_return(self) -> None:
        """Release the test gate so the call can return."""
        self._allow_return.set()
