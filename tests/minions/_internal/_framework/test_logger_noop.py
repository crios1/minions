import logging

import pytest

from minions._internal._framework.logger_noop import NoOpLogger


@pytest.mark.asyncio
async def test_is_no_op(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
):
    caplog.set_level(logging.DEBUG)
    logger = NoOpLogger()

    assert await logger.log(logging.INFO, "discard this", field="value") is None
    assert caplog.records == []
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
