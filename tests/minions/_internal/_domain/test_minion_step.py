import inspect

import pytest

from minions._internal._domain.minion_step import minion_step


@pytest.mark.asyncio
async def test_can_be_used_as_direct_decorator():
    @minion_step
    async def step1():
        ...

    assert inspect.iscoroutinefunction(step1)
    assert getattr(step1, "__minion_step__", None) is True


@pytest.mark.asyncio
async def test_can_be_used_as_decorator_factory():
    @minion_step()
    async def step2():
        ...

    assert inspect.iscoroutinefunction(step2)
    assert getattr(step2, "__minion_step__", None) is True


def test_rejects_sync_function():
    with pytest.raises(TypeError):
        @minion_step  # pyright: ignore[reportArgumentType]
        def not_async() -> None:  # pyright: ignore[reportUnusedFunction]
            ...


def test_rejects_decorated_function_passed_by_keyword() -> None:
    async def step() -> None:
        pass

    with pytest.raises(TypeError, match="positional-only"):
        minion_step(fn=step)  # pyright: ignore[reportCallIssue]


def test_rejects_decorator_options() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'name'"):
        minion_step(name="custom")  # pyright: ignore[reportCallIssue]
