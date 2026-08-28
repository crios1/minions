import argparse
import asyncio
import json
import os
import signal
import sqlite3
import time
from pathlib import Path
from typing import Literal, Protocol, cast

from minions import Gru
from minions._internal._framework.logger import Logger
from minions.implementations import NoOpMetrics, SQLiteStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.experimental.runtime_resilience.subprocess_recovery.components import (
    CrashCheckpointMinion,
    CrashCheckpointPipeline,
)

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
Role = Literal["initial", "recovery"]


class _SQLiteGateConnection(Protocol):
    async def set_trace_callback(
        self,
        handler: object,
    ) -> None: ...

    async def set_progress_handler(
        self,
        handler: object,
        n: int,
    ) -> None: ...


class GatedSQLiteStateStore(SQLiteStateStore):
    def __init__(
        self,
        db_path: str,
        logger: Logger,
        *,
        artifact_dir: Path,
        scenario: Scenario,
        role: Role,
    ) -> None:
        super().__init__(
            db_path=db_path,
            logger=logger,
            batch_max_queued_writes=(
                16 if scenario == "queued_checkpoint" else 1
            ),
            batch_max_flush_delay_ms=40,
        )
        self._artifact_dir = artifact_dir
        self._scenario = scenario
        self._role = role
        self._workflow_save_count = 0
        self._workflow_delete_count = 0
        self._active_transaction_statement_seen = False
        self._active_transaction_gate_entered = False

    async def startup(self) -> None:
        await super().startup()
        if self._role != "initial" or self._scenario not in {
            "active_transaction",
            "before_delete_commit",
        }:
            return

        db = cast(_SQLiteGateConnection, self._require_db())

        def trace_statement(statement: str) -> None:
            statement_kind_matches = (
                self._scenario == "active_transaction"
                and "INSERT INTO workflows" in statement
            ) or (
                self._scenario == "before_delete_commit"
                and "DELETE FROM workflows" in statement
            )
            if statement_kind_matches:
                self._active_transaction_statement_seen = True

        def gate_active_transaction() -> int:
            if (
                not self._active_transaction_statement_seen
                or self._active_transaction_gate_entered
            ):
                return 0
            self._active_transaction_gate_entered = True
            (self._artifact_dir / "crash_ready").touch()
            while True:
                time.sleep(1.0)

        await db.set_trace_callback(trace_statement)
        await db.set_progress_handler(gate_active_transaction, 1)

    async def _flush_soon(self) -> None:
        if self._role == "initial" and self._scenario == "queued_checkpoint":
            (self._artifact_dir / "crash_ready").touch()
            await asyncio.Event().wait()
        await super()._flush_soon()

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        self._workflow_save_count += 1
        if (
            self._role == "initial"
            and self._scenario == "before_checkpoint"
            and self._workflow_save_count == 1
        ):
            (self._artifact_dir / "crash_ready").touch()
            await asyncio.Event().wait()

        await super().save_context(workflow_id, orchestration_id, context)

        gated_save_count = {
            "after_checkpoint": 1,
            "between_steps": 3,
        }.get(self._scenario)
        if (
            self._role == "initial"
            and gated_save_count is not None
            and self._workflow_save_count == gated_save_count
        ):
            (self._artifact_dir / "crash_ready").touch()
            await asyncio.Event().wait()

    async def delete_context(self, workflow_id: str) -> None:
        self._workflow_delete_count += 1
        await super().delete_context(workflow_id)
        if (
            self._role == "initial"
            and self._scenario == "after_delete_commit"
            and self._workflow_delete_count == 1
        ):
            (self._artifact_dir / "crash_ready").touch()
            await asyncio.Event().wait()


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


async def _wait_for_recovery_result(
    scenario: Scenario,
    store: SQLiteStateStore,
    artifact_dir: Path,
) -> dict[str, object]:
    if scenario in {
        "before_checkpoint",
        "queued_checkpoint",
            "active_transaction",
        }:
        await asyncio.sleep(0.2)
    else:
        async with asyncio.timeout(2.0):
            while True:
                step_1_count = _line_count(artifact_dir / "step_1.log")
                step_2_count = _line_count(artifact_dir / "step_2.log")
                contexts = await store.get_all_contexts()
                expected_step_1_count = (
                    2
                    if scenario
                    in {
                        "during_step",
                        "during_orchestration_stop",
                        "during_gru_shutdown",
                        "graceful_sigterm",
                        "truncated_payload",
                        "incompatible_payload",
                    }
                    else 1
                )
                if (
                    step_1_count >= expected_step_1_count
                    and step_2_count >= 1
                    and not contexts
                ):
                    break
                await asyncio.sleep(0.01)

    contexts = await store.get_all_contexts()
    with sqlite3.connect(store.db_path) as db:
        integrity = db.execute("PRAGMA integrity_check").fetchone()

    return {
        "integrity": None if integrity is None else integrity[0],
        "persisted_workflows": len(contexts),
        "step_1_count": _line_count(artifact_dir / "step_1.log"),
        "step_2_count": _line_count(artifact_dir / "step_2.log"),
    }


async def _wait_for_bad_payload_result(
    *,
    store: SQLiteStateStore,
    artifact_dir: Path,
    start_success: bool,
    decode_error_type: object,
) -> dict[str, object]:
    if not isinstance(decode_error_type, str):
        raise RuntimeError("decode failure log has no string error_type")
    contexts = await store.get_all_contexts()
    with sqlite3.connect(store.db_path) as db:
        integrity = db.execute("PRAGMA integrity_check").fetchone()
    return {
        "integrity": None if integrity is None else integrity[0],
        "persisted_workflows": len(contexts),
        "step_1_count": _line_count(artifact_dir / "step_1.log"),
        "step_2_count": _line_count(artifact_dir / "step_2.log"),
        "start_success": start_success,
        "decode_error_type": decode_error_type,
    }


async def _run(
    *,
    scenario: Scenario,
    role: Role,
    db_path: Path,
    artifact_dir: Path,
) -> None:
    os.environ["MINIONS_CRASH_SCENARIO"] = scenario
    os.environ["MINIONS_CRASH_ROLE"] = role
    os.environ["MINIONS_CRASH_ARTIFACT_DIR"] = str(artifact_dir)

    shutdown_requested = asyncio.Event()
    if role == "initial" and scenario == "graceful_sigterm":
        asyncio.get_running_loop().add_signal_handler(
            signal.SIGTERM,
            shutdown_requested.set,
        )

    logger = InMemoryLogger()
    store = GatedSQLiteStateStore(
        str(db_path),
        logger,
        artifact_dir=artifact_dir,
        scenario=scenario,
        role=role,
    )
    gru = await Gru.create(
        state_store=store,
        logger=logger,
        metrics=NoOpMetrics(),
    )
    try:
        started = await gru.start_orchestration(
            CrashCheckpointPipeline,
            CrashCheckpointMinion,
        )
        if role == "recovery" and scenario in {
            "truncated_payload",
            "incompatible_payload",
        }:
            if scenario == "truncated_payload" and not started.success:
                raise RuntimeError("truncated payload should be skipped without rejecting startup")
            if scenario == "incompatible_payload":
                if started.success:
                    raise RuntimeError("incompatible payload should reject orchestration startup")
                if (
                    started.reason is None
                    or "could not be decoded with the current Minion event "
                    "and workflow context types" not in started.reason
                ):
                    raise RuntimeError("incompatible payload has no type-mismatch reason")
                if (
                    started.suggestion is None
                    or "drain the orchestration" not in started.suggestion
                ):
                    raise RuntimeError("incompatible payload has no recovery suggestion")
            await asyncio.sleep(0.2)
            decode_log = logger.find_first_log(
                "StateStore failed to decode stored workflow context"
            )
            if decode_log is None:
                raise RuntimeError("recovery did not log persisted payload decode failure")
            result = await _wait_for_bad_payload_result(
                store=store,
                artifact_dir=artifact_dir,
                start_success=started.success,
                decode_error_type=decode_log.kwargs.get("error_type"),
            )
            (artifact_dir / "recovery_result.json").write_text(
                json.dumps(result, sort_keys=True),
                encoding="utf-8",
            )
            return
        if not started.success:
            raise RuntimeError(started.reason)

        if role == "initial":
            if scenario == "graceful_sigterm":
                await shutdown_requested.wait()
                shutdown = await gru.shutdown()
                (artifact_dir / "graceful_result.json").write_text(
                    json.dumps({"shutdown_success": shutdown.success}),
                    encoding="utf-8",
                )
                if not shutdown.success:
                    raise RuntimeError(shutdown.reason)
                return
            if scenario in {"during_orchestration_stop", "during_gru_shutdown"}:
                async with asyncio.timeout(2.0):
                    while _line_count(artifact_dir / "step_1.log") < 1:
                        await asyncio.sleep(0.01)
                if scenario == "during_orchestration_stop":
                    if started.orchestration_id is None:
                        raise RuntimeError("started orchestration has no ID")
                    await gru.stop_orchestration(started.orchestration_id)
                else:
                    await gru.shutdown()
                raise RuntimeError("shutdown operation returned before crash boundary")
            await asyncio.Event().wait()

        result = await _wait_for_recovery_result(scenario, store, artifact_dir)
        (artifact_dir / "recovery_result.json").write_text(
            json.dumps(result, sort_keys=True),
            encoding="utf-8",
        )
    finally:
        await gru.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=(
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
        ),
        required=True,
    )
    parser.add_argument("--role", choices=("initial", "recovery"), required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(
        _run(
            scenario=args.scenario,
            role=args.role,
            db_path=args.db_path,
            artifact_dir=args.artifact_dir,
        )
    )


if __name__ == "__main__":
    main()
