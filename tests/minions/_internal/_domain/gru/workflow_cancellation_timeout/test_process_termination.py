import json
import os
import select
import signal
import subprocess
import sys
import time

import pytest

_STALLED_WORKFLOW_PROCESS_MODULE = (
    "tests.minions._internal._domain.gru.workflow_cancellation_timeout."
    "_stalled_workflow_process"
)
_READY_TIMEOUT_SECONDS = 5.0
_EXIT_TIMEOUT_SECONDS = 3.0

pytestmark = pytest.mark.skipif(
    os.name != "posix",
    reason="requires POSIX process groups and pipe selection",
)


def _terminate_process_group(process: subprocess.Popen[str]) -> tuple[str, bool]:
    used_sigkill = False
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        output, _ = process.communicate(timeout=_EXIT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        used_sigkill = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate(timeout=_EXIT_TIMEOUT_SECONDS)
    return output, used_sigkill


def _read_observation_line(process: subprocess.Popen[str]) -> str:
    assert process.stdout is not None
    readable, _, _ = select.select(
        [process.stdout],
        [],
        [],
        _READY_TIMEOUT_SECONDS,
    )
    if readable:
        return process.stdout.readline()

    output, used_sigkill = _terminate_process_group(process)
    termination = "SIGKILL was required" if used_sigkill else "SIGTERM succeeded"
    raise AssertionError(
        "subprocess did not report its boundary state "
        f"({termination}):\n{output}"
    )


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    return True


def test_stops_user_code_after_cancellation_timeout():
    process = subprocess.Popen(
        [sys.executable, "-m", _STALLED_WORKFLOW_PROCESS_MODULE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        observation_line = _read_observation_line(process)
        assert observation_line, "subprocess exited without reporting its boundary state"
        observation = json.loads(observation_line)

        assert observation["state"] == "cancellation_stalled"
        assert observation["step_exited"] is False
        assert observation["stop_success"] is False
        reason = observation["reason"]
        assert isinstance(reason, str)
        assert reason.startswith("Timeout while cancelling task")
        assert observation["suggestion"] == (
            "Consider restarting the process to establish a hard cleanup boundary; "
            "user code may still be running."
        )
        assert observation["runtime_empty"] is True
        assert process.poll() is None

        started_termination_at = time.monotonic()
        remaining_output, used_sigkill = _terminate_process_group(process)
        assert not used_sigkill, remaining_output
        assert time.monotonic() - started_termination_at < _EXIT_TIMEOUT_SECONDS
        assert process.returncode == -signal.SIGTERM
        assert not _process_group_exists(process.pid)
    finally:
        if process.poll() is None:
            _terminate_process_group(process)
