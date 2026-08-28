import asyncio
import gc
import tracemalloc
import weakref
from dataclasses import dataclass
from typing import cast

import psutil
import pytest

from minions import Gru
from minions.implementations import NoOpLogger, NoOpMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

_WARMUP_CYCLES = 16
_MEASURED_CYCLES = 128
_MAX_FILE_DESCRIPTOR_GROWTH = 2
_MAX_TRACED_ALLOCATION_GROWTH_BYTES = 4 * 1024 * 1024
_MAX_RSS_GROWTH_BYTES = 32 * 1024 * 1024

_COMPOSITIONS = (
    (
        "tests.assets.pipelines.emit_one.counter.default",
        "tests.assets.minions.one_step.counter.default",
    ),
    (
        "tests.assets.pipelines.emit_one.counter.default",
        "tests.assets.minions.two_steps.counter.default",
    ),
    (
        "tests.assets.pipelines.emit_one.counter.default_b",
        "tests.assets.minions.one_step.counter.default",
    ),
)


@dataclass(frozen=True, slots=True)
class ProcessSample:
    file_descriptors: int
    rss_bytes: int
    traced_bytes: int


def _sample_process(process: psutil.Process) -> ProcessSample:
    traced_bytes = tracemalloc.get_traced_memory()[0] if tracemalloc.is_tracing() else 0
    return ProcessSample(
        file_descriptors=process.num_fds(),
        rss_bytes=process.memory_info().rss,
        traced_bytes=traced_bytes,
    )


async def _run_lifecycle_cycle() -> tuple[weakref.ReferenceType[object], ...]:
    logger = NoOpLogger()
    metrics = NoOpMetrics()
    state_store = InMemoryStateStore(logger=logger)
    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )

    starts = [
        await gru.start_orchestration(pipeline=pipeline, minion=minion)
        for pipeline, minion in _COMPOSITIONS
    ]
    assert all(start.success for start in starts)
    assert all(start.orchestration_id is not None for start in starts)

    for start in reversed(starts):
        assert start.orchestration_id is not None
        stopped = await gru.stop_orchestration(start.orchestration_id)
        assert stopped.success

    assert (await gru.runtime_state_snapshot()).is_empty
    shutdown = await gru.shutdown()
    assert shutdown.success
    assert (await gru.runtime_state_snapshot()).is_empty

    references = (
        cast(weakref.ReferenceType[object], weakref.ref(gru)),
        cast(weakref.ReferenceType[object], weakref.ref(state_store)),
        cast(weakref.ReferenceType[object], weakref.ref(metrics)),
        cast(weakref.ReferenceType[object], weakref.ref(logger)),
    )
    del starts
    del gru
    del state_store
    del metrics
    del logger
    return references


async def _settle_and_assert_released(
    references: tuple[weakref.ReferenceType[object], ...],
) -> None:
    await asyncio.sleep(0)
    gc.collect()
    assert all(reference() is None for reference in references)


@pytest.mark.asyncio
async def test_repeated_gru_lifecycle_resources_remain_bounded():
    current_task = asyncio.current_task()
    baseline_tasks = {
        task
        for task in asyncio.all_tasks()
        if task is not current_task and not task.done()
    }

    for _ in range(_WARMUP_CYCLES):
        await _settle_and_assert_released(await _run_lifecycle_cycle())

    process = psutil.Process()
    gc.collect()
    tracemalloc.start()
    baseline = _sample_process(process)

    peak_file_descriptors = baseline.file_descriptors
    peak_rss_bytes = baseline.rss_bytes
    try:
        for cycle_index in range(_MEASURED_CYCLES):
            await _settle_and_assert_released(await _run_lifecycle_cycle())
            if (cycle_index + 1) % 8 == 0:
                sample = _sample_process(process)
                peak_file_descriptors = max(
                    peak_file_descriptors,
                    sample.file_descriptors,
                )
                peak_rss_bytes = max(peak_rss_bytes, sample.rss_bytes)

        gc.collect()
        final = _sample_process(process)
    finally:
        tracemalloc.stop()

    remaining_tasks = {
        task
        for task in asyncio.all_tasks()
        if task is not current_task and not task.done()
    }
    print(
        "lifecycle_leak_sample "
        f"cycles={_MEASURED_CYCLES} "
        f"task_delta={len(remaining_tasks) - len(baseline_tasks)} "
        f"fd_peak_delta={peak_file_descriptors - baseline.file_descriptors} "
        f"fd_final_delta={final.file_descriptors - baseline.file_descriptors} "
        f"traced_final_delta={final.traced_bytes - baseline.traced_bytes} "
        f"rss_peak_delta={peak_rss_bytes - baseline.rss_bytes}"
    )
    assert remaining_tasks <= baseline_tasks
    assert (
        peak_file_descriptors - baseline.file_descriptors
        <= _MAX_FILE_DESCRIPTOR_GROWTH
    )
    assert (
        final.file_descriptors - baseline.file_descriptors
        <= _MAX_FILE_DESCRIPTOR_GROWTH
    )
    assert (
        final.traced_bytes - baseline.traced_bytes
        <= _MAX_TRACED_ALLOCATION_GROWTH_BYTES
    )
    assert peak_rss_bytes - baseline.rss_bytes <= _MAX_RSS_GROWTH_BYTES
