import asyncio


class GatedLock:
    """Lock-like async test gate built on an internal asyncio.Lock."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.enter_count = 0

    async def __aenter__(self):
        await self._lock.acquire()
        self.enter_count += 1
        self.entered.set()
        await self.release.wait()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()
        return False


class GatedAsyncCallable:
    """Async test double for monkeypatching an async callable.

    Typically used with monkeypatch.setattr to replace a real async method,
    record entry, and block until the test explicitly releases it so a race
    window can be held open deterministically.
    """

    def __init__(self, result=None):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.enter_count = 0
        self._result = result

    async def __call__(self, *args, **kwargs):
        self.enter_count += 1
        self.entered.set()
        await self.release.wait()
        return self._result
