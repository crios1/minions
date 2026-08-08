import inspect

import pytest

from minions._internal._domain.minion_step import minion_step


@pytest.mark.asyncio
async def test_can_be_used_as_direct_decorator():
    @minion_step
    async def step1():
        ...

    assert inspect.iscoroutinefunction(step1)
    assert getattr(step1, "__minion_step__", None) == {"name": "step1"}


@pytest.mark.asyncio
async def test_can_be_used_as_decorator_factory():
    @minion_step()
    async def step2():
        ...

    assert inspect.iscoroutinefunction(step2)
    assert getattr(step2, "__minion_step__", None) == {"name": "step2"}


def test_rejects_sync_function():
    with pytest.raises(TypeError):
        @minion_step  # pyright: ignore[reportArgumentType]
        def not_async() -> None:  # pyright: ignore[reportUnusedFunction]
            ...
