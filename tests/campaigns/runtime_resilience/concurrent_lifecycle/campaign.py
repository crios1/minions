import asyncio
import random
from dataclasses import dataclass

import pytest

from minions import Gru
from minions._internal._domain.gru_result_types import (
    ShutdownResult,
    StartResult,
    StopResult,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_runtime_component_maps_consistent,
    assert_runtime_resource_maps_consistent,
)

_SEEDS = tuple(range(32))
_BATCHES_PER_SEED = 24


@dataclass(frozen=True, slots=True)
class Composition:
    pipeline: str
    minion: str


@dataclass(slots=True)
class CompositionState:
    orchestration_id: str
    active: bool = False


@dataclass(frozen=True, slots=True)
class LinearizedOutcome:
    successful_starts: int
    successful_stops: int
    active: bool


_COMPOSITIONS = (
    Composition(
        pipeline="tests.assets.pipelines.emit_one.counter.default",
        minion="tests.assets.minions.one_step.counter.default",
    ),
    Composition(
        pipeline="tests.assets.pipelines.emit_one.counter.default",
        minion="tests.assets.minions.two_steps.counter.default",
    ),
    Composition(
        pipeline="tests.assets.pipelines.emit_one.counter.default_b",
        minion="tests.assets.minions.one_step.counter.default",
    ),
)


def _linearized_outcomes(
    *,
    initially_active: bool,
    starts: int,
    stops: int,
) -> set[LinearizedOutcome]:
    pending = [(initially_active, starts, stops, 0, 0)]
    seen: set[tuple[bool, int, int, int, int]] = set()
    outcomes: set[LinearizedOutcome] = set()
    while pending:
        active, starts_left, stops_left, start_successes, stop_successes = pending.pop()
        state = (
            active,
            starts_left,
            stops_left,
            start_successes,
            stop_successes,
        )
        if state in seen:
            continue
        seen.add(state)
        if starts_left == 0 and stops_left == 0:
            outcomes.add(
                LinearizedOutcome(
                    successful_starts=start_successes,
                    successful_stops=stop_successes,
                    active=active,
                )
            )
            continue
        if starts_left:
            pending.append(
                (
                    True,
                    starts_left - 1,
                    stops_left,
                    start_successes + int(not active),
                    stop_successes,
                )
            )
        if stops_left:
            pending.append(
                (
                    False,
                    starts_left,
                    stops_left - 1,
                    start_successes,
                    stop_successes + int(active),
                )
            )
    return outcomes


async def _discover_id(gru: Gru, composition: Composition) -> str:
    started = await gru.start_orchestration(
        pipeline=composition.pipeline,
        minion=composition.minion,
    )
    assert started.success
    assert started.orchestration_id is not None
    stopped = await gru.stop_orchestration(started.orchestration_id)
    assert stopped.success
    return started.orchestration_id


async def _run_gated(
    ready: asyncio.Event,
    operation: str,
    gru: Gru,
    composition: Composition,
    orchestration_id: str,
) -> StartResult | StopResult:
    await ready.wait()
    if operation == "start":
        return await gru.start_orchestration(
            pipeline=composition.pipeline,
            minion=composition.minion,
        )
    return await gru.stop_orchestration(orchestration_id)


async def _assert_runtime_matches(
    gru: Gru,
    states: dict[Composition, CompositionState],
    *,
    context: str,
) -> None:
    snapshot = await gru.runtime_state_snapshot()
    expected_active_ids = frozenset(
        state.orchestration_id for state in states.values() if state.active
    )
    assert snapshot.orchestrations == expected_active_ids, context
    assert set(snapshot.minion_instance_by_orchestration) == expected_active_ids, context
    assert set(snapshot.pipeline_by_orchestration) == expected_active_ids, context
    assert len(snapshot.minion_instances) == len(expected_active_ids), context
    assert len(snapshot.minion_tasks) == len(expected_active_ids), context
    await assert_runtime_component_maps_consistent(gru)
    await assert_runtime_resource_maps_consistent(gru)


async def _run_shutdown_race_call(
    ready: asyncio.Event,
    operation: str,
    gru: Gru,
    composition: Composition | None,
    states: dict[Composition, CompositionState],
) -> tuple[str, StartResult | StopResult | ShutdownResult]:
    await ready.wait()
    if operation == "shutdown":
        return operation, await gru.shutdown()
    assert composition is not None
    if operation == "start":
        return operation, await gru.start_orchestration(
            pipeline=composition.pipeline,
            minion=composition.minion,
        )
    return operation, await gru.stop_orchestration(states[composition].orchestration_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", _SEEDS)
async def test_concurrent_lifecycle_batches_are_linearizable(seed: int) -> None:
    rng = random.Random(seed)
    logger = InMemoryLogger()
    metrics = InMemoryMetrics(logger=logger)
    state_store = InMemoryStateStore(logger=logger)
    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    try:
        states = {
            composition: CompositionState(orchestration_id=await _discover_id(gru, composition))
            for composition in _COMPOSITIONS
        }
        await _assert_runtime_matches(gru, states, context=f"seed={seed}, discovery")

        for batch_index in range(_BATCHES_PER_SEED):
            composition = rng.choice(_COMPOSITIONS)
            state = states[composition]
            operations = [rng.choice(("start", "stop")) for _ in range(rng.randint(2, 6))]
            if len(set(operations)) == 1:
                operations[rng.randrange(len(operations))] = (
                    "stop" if operations[0] == "start" else "start"
                )
            rng.shuffle(operations)

            ready = asyncio.Event()
            tasks = [
                asyncio.create_task(
                    _run_gated(
                        ready,
                        operation,
                        gru,
                        composition,
                        state.orchestration_id,
                    )
                )
                for operation in operations
            ]
            await asyncio.sleep(0)
            ready.set()
            results = await asyncio.gather(*tasks)

            successful_starts = sum(
                int(operation == "start" and result.success)
                for operation, result in zip(operations, results, strict=True)
            )
            successful_stops = sum(
                int(operation == "stop" and result.success)
                for operation, result in zip(operations, results, strict=True)
            )
            snapshot = await gru.runtime_state_snapshot()
            observed_active = state.orchestration_id in snapshot.orchestrations
            observed = LinearizedOutcome(
                successful_starts=successful_starts,
                successful_stops=successful_stops,
                active=observed_active,
            )
            allowed = _linearized_outcomes(
                initially_active=state.active,
                starts=operations.count("start"),
                stops=operations.count("stop"),
            )
            context = (
                f"seed={seed}, batch={batch_index}, operations={operations}, "
                f"initially_active={state.active}, observed={observed}, "
                f"allowed={allowed}"
            )
            assert observed in allowed, context

            for operation, result in zip(operations, results, strict=True):
                if operation == "start":
                    assert isinstance(result, StartResult), context
                    assert result.orchestration_id == state.orchestration_id, context
                if not result.success:
                    assert result.reason in {
                        "Orchestration already running - start request was rejected.",
                        "Orchestration is no longer running.",
                        "No orchestration found with the given ID.",
                    }, context

            state.active = observed_active
            await _assert_runtime_matches(gru, states, context=context)

        shutdown = await gru.shutdown()
        assert shutdown.success
        for state in states.values():
            state.active = False
        await _assert_runtime_matches(gru, states, context=f"seed={seed}, shutdown")
    finally:
        await gru.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", _SEEDS)
async def test_shutdown_race_drains_reserved_work_and_rejects_late_work(
    seed: int,
) -> None:
    rng = random.Random(seed)
    logger = InMemoryLogger()
    gru = await Gru.create(
        logger=logger,
        metrics=InMemoryMetrics(logger=logger),
        state_store=InMemoryStateStore(logger=logger),
    )
    states = {
        composition: CompositionState(orchestration_id=await _discover_id(gru, composition))
        for composition in _COMPOSITIONS
    }
    initially_active = rng.sample(list(_COMPOSITIONS), k=2)
    for composition in initially_active:
        started = await gru.start_orchestration(
            pipeline=composition.pipeline,
            minion=composition.minion,
        )
        assert started.success
        states[composition].active = True

    calls: list[tuple[str, Composition | None]] = [
        ("shutdown", None),
        *[(rng.choice(("start", "stop")), rng.choice(_COMPOSITIONS)) for _ in range(12)],
    ]
    rng.shuffle(calls)
    ready = asyncio.Event()
    tasks = [
        asyncio.create_task(
            _run_shutdown_race_call(
                ready,
                operation,
                gru,
                composition,
                states,
            )
        )
        for operation, composition in calls
    ]
    await asyncio.sleep(0)
    ready.set()
    results = await asyncio.gather(*tasks)

    shutdown_results = [result for operation, result in results if operation == "shutdown"]
    assert len(shutdown_results) == 1
    assert isinstance(shutdown_results[0], ShutdownResult)
    assert shutdown_results[0].success

    allowed_failure_reasons = {
        "Gru is shutting down.",
        "Orchestration already running - start request was rejected.",
        "Orchestration is no longer running.",
        "No orchestration found with the given ID.",
    }
    for operation, result in results:
        if operation == "shutdown":
            continue
        assert isinstance(result, (StartResult, StopResult))
        if not result.success:
            assert result.reason in allowed_failure_reasons

    snapshot = await gru.runtime_state_snapshot()
    assert snapshot.is_empty
    await assert_runtime_component_maps_consistent(gru)
    await assert_runtime_resource_maps_consistent(gru)
    repeated_shutdown = await gru.shutdown()
    assert repeated_shutdown.success
