import json
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Literal, cast

import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    serialize_persisted_workflow_context,
)
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent

Scenario = Literal[
    "before_checkpoint",
    "queued_checkpoint",
    "active_transaction",
    "after_checkpoint",
    "during_step",
    "between_steps",
    "before_delete_commit",
    "after_delete_commit",
    "during_orchestration_stop",
    "during_gru_shutdown",
    "graceful_sigterm",
    "truncated_payload",
    "incompatible_payload",
]

_RUNNER_MODULE = "tests.campaigns.runtime_resilience.subprocess_recovery.runner"
_PROCESS_TIMEOUT_SECONDS = 5.0
_CRASH_READY_TIMEOUT_SECONDS = 3.0
_MAX_SCENARIO_ARTIFACT_BYTES = 7 * 1024 * 1024


def _artifact_bytes(artifact_dir: Path) -> int:
    return sum(path.stat().st_size for path in artifact_dir.rglob("*") if path.is_file())


def _wait_for_path(path: Path, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + _CRASH_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if path.exists():
            return
        return_code = process.poll()
        if return_code is not None:
            stdout, _ = process.communicate()
            raise AssertionError(
                f"initial subprocess exited before crash boundary ({return_code}):\n{stdout}"
            )
        time.sleep(0.01)
    process.kill()
    stdout, _ = process.communicate()
    raise AssertionError(f"timed out waiting for crash boundary:\n{stdout}")


def _command(
    python: Path,
    *,
    scenario: Scenario,
    role: Literal["initial", "recovery"],
    db_path: Path,
    artifact_dir: Path,
) -> list[str]:
    return [
        str(python),
        "-m",
        _RUNNER_MODULE,
        "--scenario",
        scenario,
        "--role",
        role,
        "--db-path",
        str(db_path),
        "--artifact-dir",
        str(artifact_dir),
    ]


def _assert_sqlite_crash_boundary(db_path: Path, scenario: Scenario) -> None:
    expected_workflow_count = {
        "queued_checkpoint": 0,
        "active_transaction": 0,
        "before_delete_commit": 1,
        "after_delete_commit": 0,
        "during_orchestration_stop": 1,
        "during_gru_shutdown": 1,
        "between_steps": 1,
        "graceful_sigterm": 1,
        "truncated_payload": 1,
        "incompatible_payload": 1,
    }.get(scenario)
    if expected_workflow_count is None:
        return

    stored_context: tuple[object, ...] | None = None
    with sqlite3.connect(db_path, timeout=0.05) as db:
        workflow_count = db.execute("SELECT COUNT(*) FROM workflows").fetchone()
        if scenario == "between_steps":
            stored_context = cast(
                tuple[object, ...] | None,
                db.execute("SELECT context FROM workflows").fetchone(),
            )
    assert workflow_count == (expected_workflow_count,)
    if scenario == "between_steps":
        assert stored_context is not None
        payload = stored_context[0]
        assert isinstance(payload, bytes)
        assert deserialize_workflow_context_blob(payload).next_step_index == 1

    if scenario in {
        "queued_checkpoint",
        "after_delete_commit",
        "during_orchestration_stop",
        "during_gru_shutdown",
        "between_steps",
        "graceful_sigterm",
        "truncated_payload",
        "incompatible_payload",
    }:
        with sqlite3.connect(db_path, timeout=0.05) as db:
            db.execute("BEGIN IMMEDIATE")
            db.rollback()
        return

    with (
        sqlite3.connect(db_path, timeout=0.05) as db,
        pytest.raises(sqlite3.OperationalError, match="database is locked"),
    ):
        db.execute("BEGIN IMMEDIATE")


@pytest.mark.parametrize(
    ("scenario", "expected_step_1_count", "expected_step_2_count"),
    [
        pytest.param("before_checkpoint", 0, 0, id="before-first-checkpoint"),
        pytest.param("queued_checkpoint", 0, 0, id="queued-checkpoint"),
        pytest.param("active_transaction", 0, 0, id="active-sqlite-transaction"),
        pytest.param("after_checkpoint", 1, 1, id="after-committed-checkpoint"),
        pytest.param("during_step", 2, 1, id="during-step"),
        pytest.param("between_steps", 1, 1, id="between-steps"),
        pytest.param("before_delete_commit", 1, 2, id="before-delete-commit"),
        pytest.param("after_delete_commit", 1, 1, id="after-delete-commit"),
        pytest.param("during_orchestration_stop", 2, 1, id="during-orchestration-stop"),
        pytest.param("during_gru_shutdown", 2, 1, id="during-gru-shutdown"),
        pytest.param("graceful_sigterm", 2, 1, id="graceful-sigterm"),
        pytest.param("truncated_payload", 1, 0, id="truncated-payload"),
        pytest.param("incompatible_payload", 1, 0, id="incompatible-payload"),
    ],
)
def test_process_termination_recovers_from_durable_checkpoint(
    tmp_path: Path,
    scenario: Scenario,
    expected_step_1_count: int,
    expected_step_2_count: int,
):
    artifact_dir = tmp_path / scenario
    artifact_dir.mkdir()
    db_path = artifact_dir / "state.db"
    python = Path(__file__).parents[4] / ".venv" / "bin" / "python"
    assert python.is_file()

    initial = subprocess.Popen(
        _command(
            python,
            scenario=scenario,
            role="initial",
            db_path=db_path,
            artifact_dir=artifact_dir,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_path(artifact_dir / "crash_ready", initial)
        assert _artifact_bytes(artifact_dir) < _MAX_SCENARIO_ARTIFACT_BYTES
        _assert_sqlite_crash_boundary(db_path, scenario)
        if scenario == "graceful_sigterm":
            initial.terminate()
        else:
            initial.kill()
        initial_stdout, _ = initial.communicate(timeout=_PROCESS_TIMEOUT_SECONDS)
        assert initial.returncode is not None
        if scenario == "graceful_sigterm":
            assert initial.returncode == 0, initial_stdout
            _assert_sqlite_crash_boundary(db_path, scenario)
            graceful_result = json.loads(
                (artifact_dir / "graceful_result.json").read_text(encoding="utf-8")
            )
            assert graceful_result == {"shutdown_success": True}
        else:
            assert initial.returncode != 0, initial_stdout
    finally:
        if initial.poll() is None:
            initial.kill()
            initial.communicate()

    if scenario in {"truncated_payload", "incompatible_payload"}:
        with sqlite3.connect(db_path) as db:
            stored_identity = cast(
                tuple[object, ...] | None,
                db.execute(
                    "SELECT workflow_id, orchestration_id FROM workflows"
                ).fetchone(),
            )
            assert stored_identity is not None
            workflow_id, orchestration_id = stored_identity
            assert isinstance(workflow_id, str)
            assert isinstance(orchestration_id, str)
            if scenario == "truncated_payload":
                replacement_payload = b"\x81"
            else:
                replacement_payload = serialize_persisted_workflow_context(
                    MinionWorkflowContext(
                        orchestration_id=orchestration_id,
                        workflow_id=workflow_id,
                        event=SimpleEvent(timestamp=1.0),
                        context=SimpleContext(value=1),
                        next_step_index=0,
                    )
                )
            db.execute(
                "UPDATE workflows SET context = ? WHERE workflow_id = ?",
                (replacement_payload, workflow_id),
            )
            db.commit()

    recovery = subprocess.run(
        _command(
            python,
            scenario=scenario,
            role="recovery",
            db_path=db_path,
            artifact_dir=artifact_dir,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=_PROCESS_TIMEOUT_SECONDS,
        check=False,
    )
    assert recovery.returncode == 0, recovery.stdout
    assert _artifact_bytes(artifact_dir) < _MAX_SCENARIO_ARTIFACT_BYTES

    result = json.loads((artifact_dir / "recovery_result.json").read_text(encoding="utf-8"))
    expected_result: dict[str, object] = {
        "integrity": "ok",
        "persisted_workflows": (
            1
            if scenario in {"truncated_payload", "incompatible_payload"}
            else 0
        ),
        "step_1_count": expected_step_1_count,
        "step_2_count": expected_step_2_count,
    }
    if scenario == "truncated_payload":
        expected_result.update(
            start_success=True,
            decode_error_type="WorkflowContextSchemaError",
        )
    elif scenario == "incompatible_payload":
        expected_result.update(
            start_success=False,
            decode_error_type="WorkflowContextTypeMismatchError",
        )
    assert result == expected_result
