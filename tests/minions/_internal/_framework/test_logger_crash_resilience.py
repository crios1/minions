import asyncio
from unittest.mock import AsyncMock

import pytest

from minions._internal._framework.logger import ERROR, INFO
from tests.assets.crash.support.logger.boom_log import AssetLogger as BoomLogLogger
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_log_wrapper_falls_back_to_stderr_when_log_raises_exception(
    capsys: pytest.CaptureFixture[str],
):
    logger = BoomLogLogger()

    await logger._mn_log(INFO, "hello", key="value")

    captured = capsys.readouterr()
    assert "[Logger Error] BoomError: intentional boom" in captured.err
    assert "[Logger Fallback] hello | {'key': 'value'}" in captured.err


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "logger_error",
    [SystemExit("logger requested exit"), KeyboardInterrupt("logger interrupted")],
)
async def test_log_wrapper_falls_back_to_stderr_when_log_raises_system_exit_or_keyboard_interrupt(
    logger_error: BaseException,
    capsys: pytest.CaptureFixture[str],
):
    logger = InMemoryLogger()
    logger.log = AsyncMock(side_effect=logger_error)  # type: ignore[method-assign]

    await logger._mn_log(INFO, "hello", key="value")

    captured = capsys.readouterr()
    assert f"[Logger Error] {type(logger_error).__name__}: {logger_error}" in captured.err
    assert "[Logger Fallback] hello | {'key': 'value'}" in captured.err


@pytest.mark.asyncio
async def test_log_wrapper_can_be_cancelled_while_log_is_blocked():
    logger = InMemoryLogger()
    logging_started = asyncio.Event()

    async def blocking_log(level: int, msg: str, **kwargs: object) -> None:
        logging_started.set()
        await asyncio.Event().wait()

    logger.log = blocking_log
    logging = asyncio.create_task(logger._mn_log(INFO, "hello"))
    await asyncio.wait_for(logging_started.wait(), timeout=1)
    logging.cancel()

    with pytest.raises(asyncio.CancelledError):
        await logging

    assert logging.cancelled()


@pytest.mark.asyncio
async def test_log_exception_records_standard_exception_fields():
    logger = InMemoryLogger()
    exc = RuntimeError("outer")
    setattr(exc, "context", {"workflow_id": "wf-1"})

    await logger._mn_log_exception(ERROR, "operation failed", exc)

    [log] = logger.logs
    assert log.level == ERROR
    assert log.msg == "operation failed"
    assert log.kwargs["error_type"] == "RuntimeError"
    assert log.kwargs["error_message"] == "outer"
    assert "RuntimeError: outer" in log.kwargs["traceback"]
    assert log.kwargs["workflow_id"] == "wf-1"


@pytest.mark.asyncio
async def test_log_exception_records_direct_cause_and_call_site_kwargs_override_context():
    logger = InMemoryLogger()
    try:
        try:
            raise ValueError("inner")
        except ValueError as inner:
            raise RuntimeError("outer") from inner
    except RuntimeError as exc:
        setattr(
            exc,
            "context",
            {
                "component": "context-component",
                "workflow_id": "wf-1",
                "error_type": "MockError",
            },
        )
        await logger._mn_log_exception(
            ERROR,
            "operation failed",
            exc,
            component="call-site-component",
        )

    [log] = logger.logs
    assert log.kwargs["error_type"] == "RuntimeError"
    assert log.kwargs["error_message"] == "outer"
    assert log.kwargs["cause_error_type"] == "ValueError"
    assert log.kwargs["cause_error_message"] == "inner"
    assert "ValueError: inner" in log.kwargs["traceback"]
    assert "RuntimeError: outer" in log.kwargs["traceback"]
    assert log.kwargs["component"] == "call-site-component"
    assert log.kwargs["workflow_id"] == "wf-1"
