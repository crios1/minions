import asyncio
import contextlib
import importlib
import sys
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any, TypeVar

import pytest
import pytest_asyncio

import minions._internal._domain.component_identity as component_identity
from minions import Minion, Resource
from minions._internal._domain.gru import Gru
from minions._internal._framework.async_service import AsyncService
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

T_AsyncService = TypeVar("T_AsyncService", bound=AsyncService)


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


@pytest.fixture(autouse=True)
def _isolate_component_id_registry() -> Generator[None, None, None]:
    registry_snapshot = dict(component_identity._COMPONENT_ID_REGISTRY)
    try:
        yield
    finally:
        component_identity._COMPONENT_ID_REGISTRY.clear()
        component_identity._COMPONENT_ID_REGISTRY.update(registry_snapshot)


# Prefer these fixtures over direct InMemory* construction so spy reset,
# contract assertions, and shared logger wiring stay consistent. See
# tests/README.md for the exception policy.
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
    metrics.assert_metric_label_observations_match_contract()


@pytest.fixture
def state_store(logger: InMemoryLogger) -> InMemoryStateStore:
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()
    return InMemoryStateStore(logger=logger)


@pytest.fixture
def managed_gru_context() -> Callable[..., contextlib.AbstractAsyncContextManager[Gru]]:
    active_context = False

    @contextlib.asynccontextmanager
    async def _factory(**kwargs: Any) -> AsyncGenerator[Gru, None]:
        nonlocal active_context
        if active_context:
            raise RuntimeError(
                "managed_gru_context does not allow concurrent GRU contexts in a single test."
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


@contextlib.asynccontextmanager
async def _running_async_service_context(
    service: T_AsyncService,
) -> AsyncGenerator[T_AsyncService, None]:
    service_task = asyncio.create_task(service._mn_serve())
    try:
        await service._mn_wait_until_running()
        yield service
    finally:
        service_task.cancel()
        try:
            await service_task
        except asyncio.CancelledError:
            pass
        await service._mn_ensure_shutdown()


@pytest.fixture
def running_minion_context() -> Callable[
    [Minion[Any, Any]], contextlib.AbstractAsyncContextManager[Minion[Any, Any]]
]:
    @contextlib.asynccontextmanager
    async def _factory(
        minion: Minion[Any, Any],
    ) -> AsyncGenerator[Minion[Any, Any], None]:
        async with _running_async_service_context(minion) as running_minion:
            yield running_minion

    return _factory


@pytest.fixture
def running_resource_context() -> Callable[
    [Resource], contextlib.AbstractAsyncContextManager[Resource]
]:
    @contextlib.asynccontextmanager
    async def _factory(resource: Resource) -> AsyncGenerator[Resource, None]:
        async with _running_async_service_context(resource) as running_resource:
            yield running_resource

    return _factory


@pytest_asyncio.fixture
async def gru(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> AsyncGenerator[Gru, None]:
    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as g:
        yield g
