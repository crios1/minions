import pytest

from tests.assets.support.logger_inmemory import InMemoryLogger
from minions._internal._framework.logger import DEBUG, INFO, WARNING, ERROR, CRITICAL

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
    info_logs = [l for l in logger.logs if l.level == INFO]
    assert len(info_logs) == 1

    # content checks
    assert logger.logs[0].kwargs["foo"] == 1
    assert logger.logs[1].kwargs["bar"] == 2

    # substring + ordering helpers
    assert logger.has_log("warning")
    assert logger.has_log("error")
    assert logger.logged_before("info", "critical")
    assert not logger.logged_before("critical", "debug")
