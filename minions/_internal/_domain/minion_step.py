import contextvars
import inspect
from functools import wraps
from typing import Callable, ParamSpec, Awaitable, overload

P = ParamSpec("P")
_mn_step_active_var: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "minion_workflow_step_active",
    default=False,
)

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
        if not inspect.iscoroutinefunction(f):
            raise TypeError(f"minion_step must decorate async functions, got: {f.__name__}")
        step_name = name or f.__name__

        @wraps(f)
        async def _guarded(*args: P.args, **kwargs: P.kwargs) -> None:
            owner = args[0] if args else None
            if _mn_step_active_var.get():
                raise RuntimeError(
                    f"{type(owner).__name__}.{step_name} cannot be called from within another @minion_step; "
                    "workflow step sequencing is owned by the runtime workflow engine."
                )
            token = _mn_step_active_var.set(True)
            try:
                await f(*args, **kwargs)
            finally:
                _mn_step_active_var.reset(token)

        setattr(f, "__minion_step__", {"name": step_name})
        setattr(_guarded, "__minion_step__", {"name": step_name})
        return _guarded

    if fn is not None:
        return decorator(fn)

    return decorator
