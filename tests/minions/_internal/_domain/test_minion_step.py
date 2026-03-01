import inspect
import pytest
from typing import Callable

from minions._internal._domain.minion_step import minion_step

def is_minion_step(fn: Callable) -> bool:
    attr = getattr(fn, "__minion_step__", None)
    return isinstance(attr, dict)

@pytest.mark.asyncio
async def test_minion_step_no_parens():
    @minion_step
    async def step1():
        ...

    assert inspect.iscoroutinefunction(step1)
    assert is_minion_step(step1)
    assert getattr(step1, "__minion_step__")["name"] == "step1"

@pytest.mark.asyncio
async def test_minion_step_with_parens():
    @minion_step()
    async def step2():
        ...

    assert inspect.iscoroutinefunction(step2)
    assert is_minion_step(step2)
    assert getattr(step2, "__minion_step__")["name"] == "step2"

def test_minion_step_sync_function_raises():
    with pytest.raises(TypeError):
        @minion_step # type: ignore[reportArgumentType]
        def not_async():
            ...
