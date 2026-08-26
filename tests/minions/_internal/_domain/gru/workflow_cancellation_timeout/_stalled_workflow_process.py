import asyncio
import json

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.assets.support.state_store_inmemory import InMemoryStateStore

_PIPELINE = "tests.assets.pipelines.triggered.counter.default"
_MINION = "tests.assets.minions.failure.stalled_cancellation"


def _signal(minion_type: type[object], name: str) -> asyncio.Event:
    signal = getattr(minion_type, name, None)
    assert isinstance(signal, asyncio.Event), (
        f"{minion_type.__name__}.{name} is not an asyncio.Event"
    )
    return signal


async def _run_stalled_workflow_process() -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics(logger=logger)
    state_store = InMemoryStateStore(logger=logger)
    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
        component_owned_task_cancellation_timeout_seconds=0.01,
    )
    started = await gru.start_orchestration(_PIPELINE, _MINION)
    assert started.success, started
    assert started.orchestration_id is not None

    minion = gru._orchestrations[started.orchestration_id].minion
    minion_type = type(minion)
    step_entered = _signal(minion_type, "step_entered")
    cancellation_stalled = _signal(minion_type, "cancellation_stalled")
    step_exited = _signal(minion_type, "step_exited")

    pipeline = gru._pipelines[_PIPELINE]
    assert isinstance(pipeline, TriggeredPipeline), type(pipeline)
    await pipeline.trigger_event()
    await asyncio.wait_for(step_entered.wait(), timeout=1.0)

    stopped = await gru.stop_orchestration(started.orchestration_id)
    snapshot = await gru.runtime_state_snapshot()
    await asyncio.wait_for(cancellation_stalled.wait(), timeout=1.0)
    assert not step_exited.is_set(), (
        "workflow exited before process boundary observation"
    )

    print(
        json.dumps(
            {
                "state": "cancellation_stalled",
                "step_exited": step_exited.is_set(),
                "stop_success": stopped.success,
                "reason": stopped.reason,
                "suggestion": stopped.suggestion,
                "runtime_empty": snapshot.is_empty,
            }
        ),
        flush=True,
    )

    # The parent process establishes the hard cleanup boundary. If the user
    # task unexpectedly completes first, exiting here makes the parent fail.
    await step_exited.wait()


if __name__ == "__main__":
    asyncio.run(_run_stalled_workflow_process())
