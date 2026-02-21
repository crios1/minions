import contextlib
import importlib
import sys

import pytest
import pytest_asyncio

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


@pytest.fixture(autouse=True)
def scrub_asset_modules():
    "enables non-leaky reuse of assets across tests"
    importlib.invalidate_caches()

    def is_asset_module(name: str) -> bool:
        if name.startswith("tests.assets_revamp."):
            return True
        return name.startswith("tests.assets.") and not name.startswith("tests.assets.support")

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)

    yield

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)


@pytest.fixture
def logger():
    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    return InMemoryLogger()

@pytest.fixture
def metrics():
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    return InMemoryMetrics()

@pytest.fixture
def state_store(logger):
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()
    return InMemoryStateStore(logger=logger)

@pytest.fixture
def gru_factory():
    active_context = False

    @contextlib.asynccontextmanager
    async def _factory(**kwargs):
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
async def gru(logger, metrics, state_store, gru_factory):
    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as g:
        yield g
