import asyncio

import pytest

from minions._internal._framework.state_store import StoredWorkflowContext
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.state_store_failable import FailableStateStore


@pytest.mark.asyncio
async def test_save_failures_can_be_enabled_and_disabled() -> None:
    store = FailableStateStore(InMemoryLogger())

    await store.save_context("workflow", "orchestration", b"first")
    store.save_failures.enable()

    with pytest.raises(RuntimeError, match="controlled save failure"):
        await store.save_context("workflow", "orchestration", b"second")

    assert await store.get_all_contexts() == [
        StoredWorkflowContext("workflow", "orchestration", b"first")
    ]

    store.save_failures.disable()
    await store.save_context("workflow", "orchestration", b"second")

    assert store.save_failures.count == 1
    assert await store.get_all_contexts() == [
        StoredWorkflowContext("workflow", "orchestration", b"second")
    ]


@pytest.mark.asyncio
async def test_delete_failures_can_be_enabled_and_disabled() -> None:
    store = FailableStateStore(InMemoryLogger())
    await store.save_context("workflow", "orchestration", b"context")
    store.delete_failures.enable()

    with pytest.raises(RuntimeError, match="controlled delete failure"):
        await store.delete_context("workflow")

    assert await store.get_all_contexts() != []

    store.delete_failures.disable()
    await store.delete_context("workflow")

    assert store.delete_failures.count == 1
    assert await store.get_all_contexts() == []


@pytest.mark.asyncio
async def test_save_and_delete_failures_are_controlled_independently() -> None:
    store = FailableStateStore(InMemoryLogger())
    await store.save_context("workflow", "orchestration", b"first")

    store.save_failures.enable()
    await store.delete_context("workflow")
    store.save_failures.disable()

    store.delete_failures.enable()
    await store.save_context("workflow", "orchestration", b"second")

    assert await store.get_all_contexts() == [
        StoredWorkflowContext("workflow", "orchestration", b"second")
    ]


@pytest.mark.asyncio
async def test_save_failures_wait_for_returns_after_requested_count() -> None:
    store = FailableStateStore(InMemoryLogger())
    store.save_failures.enable()

    failures = [
        asyncio.create_task(store.save_context(str(i), "orchestration", b"context"))
        for i in range(2)
    ]

    await store.save_failures.wait_for(2)
    results = await asyncio.gather(*failures, return_exceptions=True)

    assert all(isinstance(result, RuntimeError) for result in results)
    assert store.save_failures.count == 2


@pytest.mark.asyncio
async def test_save_failures_wait_for_times_out_when_disabled() -> None:
    store = FailableStateStore(InMemoryLogger())

    with pytest.raises(TimeoutError):
        await store.save_failures.wait_for(timeout=0.01)


@pytest.mark.asyncio
async def test_save_failures_wait_for_rejects_non_positive_count() -> None:
    store = FailableStateStore(InMemoryLogger())

    with pytest.raises(ValueError, match="failure count must be positive"):
        await store.save_failures.wait_for(0)
