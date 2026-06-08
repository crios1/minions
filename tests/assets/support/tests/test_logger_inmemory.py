import pytest

from minions._internal._framework.logger import CRITICAL, DEBUG, ERROR, INFO, WARNING
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_inmemory_logger_logs_and_queries():
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
    assert not logger.has_log("debug", log_kwargs={"foo": 2})
    assert not logger.has_log("debug", log_kwargs={"missing": "value"})
    assert not logger.has_log("debug", log_kwargs={"missing": None})
    assert logger.logged_before("info", "critical")
    assert not logger.logged_before("critical", "debug")


@pytest.mark.asyncio
async def test_inmemory_logger_wait_for_log_matches_kwargs():
    logger = InMemoryLogger(level=DEBUG)

    async def log_later() -> None:
        await logger.log(INFO, "first event", status="wrong")
        await logger.log(INFO, "first event", status="right")

    await log_later()

    assert await logger.wait_for_log("first", log_kwargs={"status": "right"})
    assert not await logger.wait_for_log("first", log_kwargs={"status": "missing"}, timeout=0.001)
