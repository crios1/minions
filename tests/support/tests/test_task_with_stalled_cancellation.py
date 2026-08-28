import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest

from tests.support.task_with_stalled_cancellation import (
    task_with_stalled_cancellation,
)


@pytest.mark.asyncio
async def test_cancellation_stays_pending_until_context_exits():
    async with task_with_stalled_cancellation() as task:
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

    assert task.done()


@pytest.mark.asyncio
async def test_context_exit_cleans_up_task_that_was_not_cancelled():
    async with task_with_stalled_cancellation() as task:
        assert not task.done()

    assert task.done()


@pytest.mark.asyncio
async def test_uses_task_factory_and_name_when_provided():
    created_tasks: list[asyncio.Task[None]] = []

    def task_factory(
        coro: Coroutine[Any, Any, object],
        name: str | None = None,
    ) -> asyncio.Task[None]:
        async def run():
            await coro

        task = asyncio.create_task(run(), name=name)
        created_tasks.append(task)
        return task

    async with task_with_stalled_cancellation(
        name="worker",
        task_factory=task_factory,
    ) as task:
        assert created_tasks == [task]
        assert task.get_name() == "worker"
