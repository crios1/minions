import asyncio

import pytest

from minions._internal._framework.logger import CRITICAL, DEBUG, ERROR, INFO, WARNING
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_logs_and_queries():
    logger = InMemoryLogger(level=DEBUG)

    await logger.log(DEBUG, "debug msg", foo=1)
    await logger.log(INFO, "info msg", bar=2)
    await logger.log(WARNING, "warning msg")
    await logger.log(ERROR, "error msg")
    await logger.log(CRITICAL, "critical msg")

    assert len(logger.logs) == 5

    # level filtering check without get_logs
    info_logs = [log for log in logger.logs if log.level == INFO]
    assert len(info_logs) == 1

    # content checks
    assert logger.logs[0].kwargs["foo"] == 1
    assert logger.logs[1].kwargs["bar"] == 2

    # substring + ordering helpers
    assert logger.has_log("warning")
    assert logger.has_log("error")
    assert logger.has_log("debug", log_kwargs={"foo": 1})
    debug_log = logger.find_first_log("debug", log_kwargs={"foo": 1})
    assert debug_log is not None
    assert debug_log.kwargs["foo"] == 1
    assert logger.find_first_log("debug", log_kwargs={"foo": 2}) is None
    assert not logger.has_log("debug", log_kwargs={"foo": 2})
    assert not logger.has_log("debug", log_kwargs={"missing": "value"})
    assert not logger.has_log("debug", log_kwargs={"missing": None})
    assert logger.logged_before("info", "critical")
    assert not logger.logged_before("critical", "debug")


@pytest.mark.asyncio
async def test_wait_for_log_matches_kwargs():
    logger = InMemoryLogger(level=DEBUG)

    async def log_later() -> None:
        await logger.log(INFO, "first event", status="wrong")
        await logger.log(INFO, "first event", status="right")

    await log_later()

    assert await logger.wait_for_log("first", log_kwargs={"status": "right"})
    assert not await logger.wait_for_log("first", log_kwargs={"status": "missing"}, timeout=0.001)


@pytest.mark.asyncio
async def test_wait_for_log_is_notified_by_matching_log():
    logger = InMemoryLogger(level=DEBUG)
    waiter = asyncio.create_task(
        logger.wait_for_log("target", min_level=WARNING, log_kwargs={"status": "ready"})
    )
    await asyncio.sleep(0)

    await logger.log(INFO, "target", status="ready")
    await logger.log(WARNING, "target", status="waiting")
    assert not waiter.done()

    await logger.log(WARNING, "target", status="ready")

    assert await waiter


@pytest.mark.asyncio
async def test_wait_for_log_does_not_miss_log_recorded_before_waiting():
    logger = InMemoryLogger(level=DEBUG)
    waiter = asyncio.create_task(logger.wait_for_log("target", timeout=0.1))

    await logger.log(INFO, "target")

    assert await waiter


@pytest.mark.asyncio
async def test_wait_for_log_notifies_concurrent_waiters_for_their_own_matches():
    logger = InMemoryLogger(level=DEBUG)
    first_waiter = asyncio.create_task(logger.wait_for_log("first"))
    second_waiter = asyncio.create_task(logger.wait_for_log("second"))
    await asyncio.sleep(0)

    await logger.log(INFO, "first")
    assert await first_waiter
    assert not second_waiter.done()

    await logger.log(INFO, "second")
    assert await second_waiter
