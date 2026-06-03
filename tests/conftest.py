import contextlib
import importlib
import sys
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


@pytest.fixture
def tests_dir() -> Path:
    return Path(__file__).resolve().parent


@pytest.fixture
def reload_wait_for_subs_pipeline() -> Callable[..., None]:
    def _reload(*, expected_subs: int) -> None:
        from tests.assets.pipelines.simple.simple_event import (
            subscriber_ready_fixed_events,
        )
        importlib.reload(subscriber_ready_fixed_events)
        
        from tests.assets.pipelines.simple.simple_event.subscriber_ready_fixed_events import (
            SimpleSubscriberReadyFixedEventsPipeline,
        )
        SimpleSubscriberReadyFixedEventsPipeline.reset_gate(expected_subs=expected_subs)

    return _reload


@pytest.fixture
def reload_pipeline_module() -> Callable[[str], None]:
    def _reload(module_name: str) -> None:
        mod = importlib.import_module(module_name)
        pipeline_cls = getattr(mod, "pipeline", None)
        if pipeline_cls is None:
            return
        if hasattr(pipeline_cls, "reset_gate"):
            pipeline_cls.reset_gate()
            return
        if hasattr(pipeline_cls, "_emitted"):
            pipeline_cls._emitted = False

    return _reload


@pytest.fixture(autouse=True)
def scrub_asset_modules() -> Generator[None, None, None]:
    "enables non-leaky reuse of assets across tests"
    importlib.invalidate_caches()

    def is_asset_module(name: str) -> bool:
        return name.startswith("tests.assets.") and not name.startswith("tests.assets.support")

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)

    yield

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)


@pytest.fixture
def logger() -> InMemoryLogger:
    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    return InMemoryLogger()

@pytest.fixture
def metrics() -> Generator[InMemoryMetrics, None, None]:
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    metrics = InMemoryMetrics()
    yield metrics
    metrics.assert_recorded_labels_match_contract()

@pytest.fixture
def state_store(logger: InMemoryLogger) -> InMemoryStateStore:
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()
    return InMemoryStateStore(logger=logger)

@pytest.fixture
def gru_factory() -> Callable[..., contextlib.AbstractAsyncContextManager[Gru]]:
    active_context = False

    @contextlib.asynccontextmanager
    async def _factory(**kwargs: Any) -> AsyncGenerator[Gru, None]:
        nonlocal active_context
        if active_context:
            raise RuntimeError(
                "gru_factory does not allow concurrent GRU contexts in a single test."
            )
        active_context = True
        try:
            gru = await Gru.create(**kwargs)
            try:
                yield gru
            finally:
                await gru.shutdown()
        finally:
            active_context = False

    return _factory

@pytest_asyncio.fixture
async def gru(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> AsyncGenerator[Gru, None]:
    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as g:
        yield g
