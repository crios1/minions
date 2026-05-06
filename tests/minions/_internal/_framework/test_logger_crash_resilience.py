import pytest

from minions._internal._framework.logger import INFO
from tests.assets.crash.support.logger_boom_log import BoomLogger


@pytest.mark.asyncio
async def test_logger_log_failure_is_contained_by_log_wrapper(capsys):
    logger = BoomLogger()

    await logger._log(INFO, "hello", key="value")

    captured = capsys.readouterr()
    assert "[Logger Error] BoomError: intentional boom" in captured.err
    assert "[Logger Fallback] hello | {'key': 'value'}" in captured.err
