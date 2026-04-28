import pytest

from minions._internal._framework.logger import WARNING
from minions._internal._framework.state_store_sqlite import (
    BATCH_MAX_FLUSH_DELAY_MS_HDD_LIKE,
    BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE,
    BATCH_MAX_FLUSH_DELAY_MS_SSD_LIKE,
    BATCH_MAX_QUEUED_WRITES_HDD_LIKE,
    BATCH_MAX_QUEUED_WRITES_NVME_LIKE,
    BATCH_MAX_QUEUED_WRITES_SSD_LIKE,
    COMMIT_P50_MS_MAX_NVME_LIKE,
    COMMIT_P50_MS_MAX_SSD_LIKE,
    DEFAULT_BATCH_MAX_FLUSH_DELAY_MS,
    DEFAULT_BATCH_MAX_QUEUED_WRITES,
    SQLiteStateStore,
    StartupMeasurements,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger


pytestmark = pytest.mark.asyncio


async def test_init_resolves_default_batch_config():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
    )

    assert s._batch_tuning == "manual"
    assert s._batch_max_queued_writes == DEFAULT_BATCH_MAX_QUEUED_WRITES
    assert s._batch_max_flush_delay_ms == DEFAULT_BATCH_MAX_FLUSH_DELAY_MS


async def test_init_preserves_configured_batch_max_queued_writes():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
        batch_max_queued_writes=200,
    )

    assert s._batch_max_queued_writes == 200
    assert s._batch_max_flush_delay_ms == DEFAULT_BATCH_MAX_FLUSH_DELAY_MS


async def test_init_accepts_immediate_batch_max_queued_writes():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
        batch_max_queued_writes=1,
    )

    assert s._batch_max_queued_writes == 1
    assert s._batch_max_flush_delay_ms == DEFAULT_BATCH_MAX_FLUSH_DELAY_MS


async def test_init_preserves_configured_batch_max_flush_delay_ms():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
        batch_max_flush_delay_ms=30,
    )

    assert s._batch_max_flush_delay_ms == 30
    assert s._batch_max_queued_writes == DEFAULT_BATCH_MAX_QUEUED_WRITES


async def test_init_with_calibrated_batch_tuning_defers_batch_resolution_until_startup():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
        batch_tuning="calibrated",
    )

    assert s._batch_tuning == "calibrated"
    assert s._batch_max_queued_writes is None
    assert s._batch_max_flush_delay_ms is None


@pytest.mark.parametrize(
    "explicit_batch_config",
    [
        {"batch_max_queued_writes": 128},
        {"batch_max_flush_delay_ms": 16},
    ],
)
async def test_init_rejects_explicit_batch_config_in_calibrated_mode(explicit_batch_config):
    with pytest.raises(
        ValueError,
        match="batch_max_queued_writes and batch_max_flush_delay_ms cannot be set when batch_tuning='calibrated'",
    ):
        SQLiteStateStore(
            db_path=":memory:",
            logger=InMemoryLogger(),
            batch_tuning="calibrated",
            **explicit_batch_config,
        )


@pytest.mark.parametrize("batch_max_queued_writes", [0, 300])
async def test_init_rejects_out_of_range_batch_max_queued_writes(batch_max_queued_writes):
    with pytest.raises(ValueError, match="batch_max_queued_writes must be between 1 and 256"):
        SQLiteStateStore(
            db_path=":memory:",
            logger=InMemoryLogger(),
            batch_max_queued_writes=batch_max_queued_writes,
        )


async def test_init_rejects_invalid_batch_tuning_mode():
    with pytest.raises(ValueError, match="batch_tuning must be 'manual' or 'calibrated'"):
        SQLiteStateStore(
            db_path=":memory:",
            logger=InMemoryLogger(),
            batch_tuning="boom",  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("batch_max_flush_delay_ms", [4, 50])
async def test_init_rejects_out_of_range_batch_max_flush_delay_ms(batch_max_flush_delay_ms):
    with pytest.raises(ValueError, match="batch_max_flush_delay_ms must be between 5 and 40"):
        SQLiteStateStore(
            db_path=":memory:",
            logger=InMemoryLogger(),
            batch_max_flush_delay_ms=batch_max_flush_delay_ms,
        )


async def test_startup_resolves_calibrated_batch_config(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    async def _fixed_measurements(self):
        return StartupMeasurements(
            commit_p50_ms=1.5,
            commit_p95_ms=3.0,
            page_size=4096,
            result="measured",
        )

    # Patch startup measurement collection so calibrated tuning chooses the NVMe bucket.
    monkeypatch.setattr(SQLiteStateStore, "_collect_startup_measurements", _fixed_measurements)

    s, logger = await make_state_store_and_logger(batch_tuning="calibrated")
    assert s._batch_max_queued_writes == BATCH_MAX_QUEUED_WRITES_NVME_LIKE
    assert s._batch_max_flush_delay_ms == BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE
    assert any(
        log.kwargs.get("batch_tuning") == "calibrated"
        and log.kwargs.get("batch_max_queued_writes") == BATCH_MAX_QUEUED_WRITES_NVME_LIKE
        and log.kwargs.get("batch_max_flush_delay_ms") == BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE
        for log in logger.logs
    )


async def test_derive_calibrated_batch_config_maps_latency_to_profile():
    s = SQLiteStateStore(
        db_path=":memory:",
        logger=InMemoryLogger(),
        batch_tuning="calibrated",
    )

    assert s._derive_batch_config_from_measurements(COMMIT_P50_MS_MAX_NVME_LIKE) == (
        BATCH_MAX_QUEUED_WRITES_NVME_LIKE,
        BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE,
    )
    assert s._derive_batch_config_from_measurements(COMMIT_P50_MS_MAX_SSD_LIKE) == (
        BATCH_MAX_QUEUED_WRITES_SSD_LIKE,
        BATCH_MAX_FLUSH_DELAY_MS_SSD_LIKE,
    )
    assert s._derive_batch_config_from_measurements(COMMIT_P50_MS_MAX_SSD_LIKE + 0.1) == (
        BATCH_MAX_QUEUED_WRITES_HDD_LIKE,
        BATCH_MAX_FLUSH_DELAY_MS_HDD_LIKE,
    )


async def test_startup_warns_for_below_recommended_batch_max_queued_writes(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    async def _fixed_measurements(self):
        return StartupMeasurements(
            commit_p50_ms=1.5,
            commit_p95_ms=3.0,
            page_size=4096,
            result="measured",
        )

    # Patch startup measurement collection so the warning path is deterministic.
    monkeypatch.setattr(SQLiteStateStore, "_collect_startup_measurements", _fixed_measurements)

    _s, logger = await make_state_store_and_logger(batch_max_queued_writes=8)
    assert any(
        "below the recommended batching range" in log.msg
        and log.level == WARNING
        and log.kwargs.get("batch_max_queued_writes") == 8
        and log.kwargs.get("immediate_writes_value") == 1
        and log.kwargs.get("recommended_min_batch_max_queued_writes") == 16
        for log in logger.logs
    )


async def test_startup_does_not_warn_for_immediate_batch_max_queued_writes(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    async def _fixed_measurements(self):
        return StartupMeasurements(
            commit_p50_ms=1.5,
            commit_p95_ms=3.0,
            page_size=4096,
            result="measured",
        )

    # Patch startup measurement collection so lack of warning is deterministic.
    monkeypatch.setattr(SQLiteStateStore, "_collect_startup_measurements", _fixed_measurements)

    _s, logger = await make_state_store_and_logger(batch_max_queued_writes=1)
    assert not any("below the recommended batching range" in log.msg for log in logger.logs)
