import asyncio
import importlib
from collections import Counter, defaultdict

import pytest

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


class GateableLock:
    def __init__(self, lock: asyncio.Lock):
        self._lock = lock
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


@pytest.mark.asyncio
async def test_gru_does_not_replay_same_workflow_id_during_startup(gru_factory):
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)
    duplicate_workflow_replay = importlib.import_module(
        "tests.assets.minions.race_cases.duplicate_workflow_replay"
    )
    duplicate_workflow_replay_minion = duplicate_workflow_replay.DuplicateWorkflowReplayMinion
    duplicate_workflow_replay_minion.reset_gates()

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_minion(
            minion="tests.assets.minions.race_cases.duplicate_workflow_replay",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        )
        assert result.success
        assert duplicate_workflow_replay_minion._gate("_startup_entered").is_set()

        def _get_step_1_workflow_ids() -> list[str]:
            return [
                log.kwargs["workflow_id"]
                for log in logger.logs
                if log.msg == "Workflow Step started"
                and log.kwargs.get("minion_name") == "startup-replay-race-minion"
                and log.kwargs.get("step_name") == "step_1"
            ]

        try:
            await asyncio.wait_for(
                duplicate_workflow_replay_minion._gate("_step_1_started").wait(),
                timeout=1.0,
            )
            await asyncio.sleep(0)

            step_1_workflow_ids = _get_step_1_workflow_ids()
            assert step_1_workflow_ids, (
                "Expected startup-replay-race-minion to start step_1."
            )

            counts = Counter(step_1_workflow_ids)
            duplicate_step_1_workflow_ids = sorted(
                workflow_id
                for workflow_id, count in counts.items()
                if count > 1
            )

            assert not duplicate_step_1_workflow_ids, (
                "A single workflow_id started step_1 more than once during startup: "
                f"{duplicate_step_1_workflow_ids}"
            )
        finally:
            duplicate_workflow_replay_minion._gate("_allow_step_1_finish").set()


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_start_requests_for_same_minion(gru_factory):
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._minion_locks = defaultdict(lambda: GateableLock(asyncio.Lock()))
        minion_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        gate = gru._minion_locks[minion_key]

        task1 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )
        task2 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.wait_for(gate.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert gate.enter_count == 1
        assert not task2.done()

        gate.release.set()
        result1, result2 = await asyncio.gather(task1, task2)

        successes = [result for result in (result1, result2) if result.success]
        failures = [result for result in (result1, result2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Minion already running — start request was rejected."
        assert len(gru._minions_by_composite_key) == 1


@pytest.mark.asyncio
async def test_gru_allows_concurrent_start_requests_for_different_minions(gru_factory):
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._minion_locks = defaultdict(lambda: GateableLock(asyncio.Lock()))
        minion_key_1 = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            "tests/assets/config/minions/a.toml",
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        minion_key_2 = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            "tests/assets/config/minions/b.toml",
            "tests.assets.pipelines.simple.simple_event.single_event_2",
        )
        gate1 = gru._minion_locks[minion_key_1]
        gate2 = gru._minion_locks[minion_key_2]

        task1 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path="tests/assets/config/minions/a.toml",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )
        task2 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path="tests/assets/config/minions/b.toml",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_2",
            )
        )

        await asyncio.wait_for(asyncio.gather(gate1.entered.wait(), gate2.entered.wait()), timeout=1.0)
        await asyncio.sleep(0)
        assert gate1.enter_count == 1
        assert gate2.enter_count == 1

        gate1.release.set()
        gate2.release.set()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        assert len(gru._minions_by_composite_key) == 2


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_start_and_stop_for_same_minion(gru_factory):
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Start once normally so the race only covers the stop/start window.
        start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert start_result.success
        gru._minion_locks = defaultdict(lambda: GateableLock(asyncio.Lock()))
        minion_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        gate = gru._minion_locks[minion_key]

        stop_task = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))
        await asyncio.wait_for(gate.entered.wait(), timeout=1.0)

        start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.sleep(0)
        assert gate.enter_count == 1
        assert not start_task.done()

        gate.release.set()
        stop_result, restart_result = await asyncio.gather(stop_task, start_task)

        assert stop_result.success
        assert restart_result.success
        assert len(gru._minions_by_composite_key) == 1
