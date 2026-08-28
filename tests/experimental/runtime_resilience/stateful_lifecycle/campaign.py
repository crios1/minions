import contextlib
import random
from collections.abc import Callable
from dataclasses import dataclass

import pytest

from minions._internal._domain.gru import Gru, GruRuntimeStateSnapshot
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_runtime_component_maps_consistent,
    assert_runtime_resource_maps_consistent,
)

_SEEDS = tuple(range(128))
_COMMANDS_PER_SEED = 64
_MISSING_ORCHESTRATION_ID = "stateful-campaign-missing-orchestration"


@dataclass(frozen=True, slots=True)
class Composition:
    pipeline: str
    minion: str


@dataclass(slots=True)
class LifecycleModel:
    active_composition_by_orchestration: dict[str, Composition]
    orchestration_id_by_composition: dict[Composition, str]
    known_orchestration_ids: set[str]


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


def _context(seed: int, command_index: int, operation: str) -> str:
    return f"seed={seed}, command={command_index}, operation={operation}"


def _assert_pipeline_equivalence(
    snapshot: GruRuntimeStateSnapshot,
    model: LifecycleModel,
    *,
    context: str,
) -> None:
    active_items = tuple(model.active_composition_by_orchestration.items())
    assert len(snapshot.pipelines) == len(
        {composition.pipeline for _, composition in active_items}
    ), context

    for left_index, (left_id, left_composition) in enumerate(active_items):
        left_pipeline_id = snapshot.pipeline_for_orchestration(left_id)
        assert left_pipeline_id is not None, context
        for right_id, right_composition in active_items[left_index + 1 :]:
            right_pipeline_id = snapshot.pipeline_for_orchestration(right_id)
            assert right_pipeline_id is not None, context
            assert (left_pipeline_id == right_pipeline_id) == (
                left_composition.pipeline == right_composition.pipeline
            ), context


async def _assert_model_matches_runtime(
    gru: Gru,
    model: LifecycleModel,
    *,
    context: str,
) -> None:
    snapshot = await gru.runtime_state_snapshot()
    active_ids = set(model.active_composition_by_orchestration)

    assert snapshot.orchestrations == active_ids, context
    assert set(snapshot.minion_instance_by_orchestration) == active_ids, context
    assert set(snapshot.pipeline_by_orchestration) == active_ids, context
    assert len(snapshot.minion_instances) == len(active_ids), context
    assert len(set(snapshot.minion_instance_by_orchestration.values())) == len(
        active_ids
    ), context
    assert not snapshot.resources, context

    await assert_runtime_component_maps_consistent(gru)
    await assert_runtime_resource_maps_consistent(gru)
    _assert_pipeline_equivalence(snapshot, model, context=context)


async def _start_random_composition(
    gru: Gru,
    model: LifecycleModel,
    rng: random.Random,
    *,
    context: str,
) -> None:
    composition = rng.choice(_COMPOSITIONS)
    active_ids_before = set(model.active_composition_by_orchestration)
    active_compositions = set(model.active_composition_by_orchestration.values())
    expected_success = composition not in active_compositions

    result = await gru.start_orchestration(
        pipeline=composition.pipeline,
        minion=composition.minion,
    )

    assert result.success is expected_success, context
    if expected_success:
        assert result.orchestration_id is not None, context
        known_orchestration_id = model.orchestration_id_by_composition.get(composition)
        if known_orchestration_id is None:
            model.orchestration_id_by_composition[composition] = result.orchestration_id
        else:
            assert result.orchestration_id == known_orchestration_id, context
        model.active_composition_by_orchestration[result.orchestration_id] = composition
        model.known_orchestration_ids.add(result.orchestration_id)
    else:
        active_orchestration_id = next(
            orchestration_id
            for orchestration_id, active_composition in (
                model.active_composition_by_orchestration.items()
            )
            if active_composition == composition
        )
        assert result.orchestration_id == active_orchestration_id, context
    assert active_ids_before <= model.known_orchestration_ids, context


async def _stop_random_orchestration(
    gru: Gru,
    model: LifecycleModel,
    rng: random.Random,
    *,
    context: str,
) -> None:
    candidate_ids = tuple(sorted(model.known_orchestration_ids)) + (
        _MISSING_ORCHESTRATION_ID,
    )
    orchestration_id = rng.choice(candidate_ids)
    expected_success = orchestration_id in model.active_composition_by_orchestration

    result = await gru.stop_orchestration(orchestration_id)

    assert result.success is expected_success, context
    if expected_success:
        model.active_composition_by_orchestration.pop(orchestration_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", _SEEDS)
async def test_seeded_gru_lifecycle_matches_reference_model(
    seed: int,
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    rng = random.Random(seed)
    model = LifecycleModel(
        active_composition_by_orchestration={},
        orchestration_id_by_composition={},
        known_orchestration_ids=set(),
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        for command_index in range(_COMMANDS_PER_SEED):
            if rng.random() < 0.52:
                operation = "start"
                context = _context(seed, command_index, operation)
                await _start_random_composition(
                    gru,
                    model,
                    rng,
                    context=context,
                )
            else:
                operation = "stop"
                context = _context(seed, command_index, operation)
                await _stop_random_orchestration(
                    gru,
                    model,
                    rng,
                    context=context,
                )

            await _assert_model_matches_runtime(
                gru,
                model,
                context=context,
            )

        shutdown = await gru.shutdown()
        assert shutdown.success, _context(seed, _COMMANDS_PER_SEED, "shutdown")
        model.active_composition_by_orchestration.clear()
        await _assert_model_matches_runtime(
            gru,
            model,
            context=_context(seed, _COMMANDS_PER_SEED, "shutdown"),
        )

        repeated_shutdown = await gru.shutdown()
        assert repeated_shutdown.success
