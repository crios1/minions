import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass

import pytest

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import assert_runtime_empty

PIPELINE = "tests.assets.pipelines.triggered.counter.default"
MINION = "tests.assets.minions.failure.stalled_cancellation"


@dataclass(frozen=True)
class _CancellationSignals:
    step_entered: asyncio.Event
    cancellation_stalled: asyncio.Event
    allow_cancellation: asyncio.Event
    step_exited: asyncio.Event


def _get_cancellation_signals(minion: object) -> _CancellationSignals:
    minion_type = type(minion)
    assert isinstance(minion, SpiedMinion)
    assert getattr(minion_type, "__module__", None) == MINION
    assert getattr(minion_type, "__qualname__", None) == "AssetMinion"

    def get_signal(name: str) -> asyncio.Event:
        signal = getattr(minion_type, name, None)
        assert isinstance(signal, asyncio.Event)
        return signal

    return _CancellationSignals(
        step_entered=get_signal("step_entered"),
        cancellation_stalled=get_signal("cancellation_stalled"),
        allow_cancellation=get_signal("allow_cancellation"),
        step_exited=get_signal("step_exited"),
    )


@contextlib.asynccontextmanager
async def _running_stalled_workflow(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> AsyncGenerator[tuple[Gru, str, _CancellationSignals]]:
    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        started = await gru.start_orchestration(PIPELINE, MINION)
        assert started.success
        assert started.orchestration_id is not None

        minion = gru._orchestrations[started.orchestration_id].minion
        signals = _get_cancellation_signals(minion)
        pipeline = gru._pipelines[PIPELINE]
        assert isinstance(pipeline, TriggeredPipeline)
        assert type(pipeline).__module__ == PIPELINE
        assert type(pipeline).__qualname__ == "AssetPipeline"

        await pipeline.trigger_event()
        await asyncio.wait_for(
            signals.step_entered.wait(),
            timeout=1.0,
        )
        try:
            yield gru, started.orchestration_id, signals
        finally:
            signals.allow_cancellation.set()
            await asyncio.wait_for(
                signals.step_exited.wait(),
                timeout=1.0,
            )


@pytest.mark.asyncio
async def test_stop_reports_stalled_workflow_cancellation_timeout_and_clears_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with _running_stalled_workflow(
        managed_gru_context,
        logger,
        metrics,
        state_store,
    ) as (gru, orchestration_id, signals):
        stopped = await gru.stop_orchestration(orchestration_id)

        assert not stopped.success
        assert stopped.reason is not None
        assert "Timeout while cancelling task" in stopped.reason
        assert stopped.suggestion == (
            "Consider restarting the process to establish a hard cleanup boundary; "
            "user code may still be running."
        )
        assert signals.cancellation_stalled.is_set()
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_shutdown_reports_stalled_workflow_cancellation_timeout_and_clears_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with _running_stalled_workflow(
        managed_gru_context,
        logger,
        metrics,
        state_store,
    ) as (gru, _orchestration_id, signals):
        shutdown = await gru.shutdown()

        assert not shutdown.success
        cancellation_errors = [
            error
            for error in shutdown.errors
            if error.error_type == "TaskCancellationTimeoutError"
        ]
        assert cancellation_errors
        assert all(
            error.phase == "shutdown_component"
            for error in cancellation_errors
        )
        assert any(
            error.component.startswith("minion:")
            for error in cancellation_errors
        )
        assert signals.cancellation_stalled.is_set()
        await assert_runtime_empty(gru)
