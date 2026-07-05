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
def logger() -> Generator[InMemoryLogger, None, None]:
    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    logger = InMemoryLogger()
    yield logger
    logger.assert_recorded_logs_match_contracts()


@pytest.fixture
def metrics(logger: InMemoryLogger) -> Generator[InMemoryMetrics, None, None]:
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    metrics = InMemoryMetrics(logger=logger)
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
