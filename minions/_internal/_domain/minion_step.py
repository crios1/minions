import asyncio
from typing import Callable, ParamSpec, Awaitable, overload

P = ParamSpec("P")

@overload
def minion_step(fn: Callable[P, Awaitable[None]]) -> Callable[P, Awaitable[None]]: ...

@overload
def minion_step(*, name: str | None = None) -> Callable[[Callable[P, Awaitable[None]]], Callable[P, Awaitable[None]]]: ...

def minion_step(
    fn: Callable[P, Awaitable[None]] | None = None,
    *,
    name: str | None = None,
) -> Callable[[Callable[P, Awaitable[None]]], Callable[P, Awaitable[None]]] | Callable[P, Awaitable[None]]:
    def decorator(f: Callable[P, Awaitable[None]]) -> Callable[P, Awaitable[None]]:
        if not asyncio.iscoroutinefunction(f):
            raise TypeError(f"minion_step must decorate async functions, got: {f.__name__}")
        setattr(f, "__minion_step__", {"name": name or f.__name__})
        return f

    if fn is not None:
        return decorator(fn)

    return decorator
