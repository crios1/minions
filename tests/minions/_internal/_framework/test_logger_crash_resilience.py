import pytest

from minions._internal._framework.logger import ERROR, INFO
from tests.assets.crash.support.logger_boom_log import BoomLogger
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_logger_log_failure_is_contained_by_log_wrapper(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logger = BoomLogger()

    await logger._mn_log(INFO, "hello", key="value")

    captured = capsys.readouterr()
    assert "[Logger Error] BoomError: intentional boom" in captured.err
    assert "[Logger Fallback] hello | {'key': 'value'}" in captured.err


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
async def test_log_exception_uses_cause_and_call_site_kwargs_override_context():
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
    assert log.kwargs["error_type"] == "ValueError"
    assert log.kwargs["error_message"] == "inner"
    assert "ValueError: inner" in log.kwargs["traceback"]
    assert log.kwargs["component"] == "call-site-component"
    assert log.kwargs["workflow_id"] == "wf-1"
