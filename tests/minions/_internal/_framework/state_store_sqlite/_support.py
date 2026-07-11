import asyncio
import time
from collections.abc import Generator
from types import TracebackType
from typing import Any

import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import persist_workflow_context
from minions._internal._framework.state_store_sqlite import (
    SQL_WORKFLOW_DELETE,
    SQL_WORKFLOW_SELECT_BY_ID,
    SQL_WORKFLOW_UPSERT,
    WORKFLOW_ID_STARTUP_PROBE,
    PendingWrite,
    SQLiteStateStore,
)
from minions._internal._utils.serialization import serialize


def mk_ctx(
    i: int = 0,
    size: int = 32,
    module_path: str = "app.minion",
) -> MinionWorkflowContext[dict[str, int], dict[str, str]]:
    return MinionWorkflowContext(
        orchestration_id=f"dummy-orchestration-id-{i}",
        workflow_id=f"wf-{i}",
        event={"i": i},
        context={"p": "x" * size},
        next_step_index=i,
        started_at=time.time(),
        error_msg=None,
    )


def blob_for(ctx: MinionWorkflowContext[Any, Any]) -> bytes:
    return serialize(persist_workflow_context(ctx))


async def cancel_and_drain_tasks(*tasks: asyncio.Task[Any] | None) -> None:
    active_tasks = [task for task in tasks if task is not None]
    if not active_tasks:
        return

    _done, pending = await asyncio.wait(active_tasks, timeout=2.0)
    for task in pending:
        task.cancel()
    await asyncio.gather(*active_tasks, return_exceptions=True)


class _StaticRowCursor:
    def __init__(self, row: tuple[str, bytes] | None):
        self._row = row

    async def fetchone(self) -> tuple[str, bytes] | None:
        return self._row


class _StaticCursorContext:
    def __init__(self, row: tuple[str, bytes] | None):
        self._row = row

    async def __aenter__(self) -> _StaticRowCursor:
        return _StaticRowCursor(self._row)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class _AwaitableResult:
    def __await__(self) -> Generator[Any, None, None]:
        if False:
            yield
        return None


class BlockedCommitBatchNowGate:
    def __init__(self, s: SQLiteStateStore, monkeypatch: pytest.MonkeyPatch):
        self.entered = asyncio.Event()
        self.commit_count = 0
        self.max_active_commit_count = 0
        self.operation_order: list[tuple[str, str]] = []
        self._active_commit_count = 0
        self._release = asyncio.Event()
        # Hold the private commit worker open at a deterministic point for race tests.
        original_commit_batch_now = s._commit_batch_now

        async def wrapped_commit_batch_now(items: list[PendingWrite]) -> float:
            self.commit_count += 1
            self.operation_order.extend((item.op, item.workflow_id) for item in items)
            self._active_commit_count += 1
            self.max_active_commit_count = max(
                self.max_active_commit_count,
                self._active_commit_count,
            )
            try:
                if not self.entered.is_set():
                    self.entered.set()
                    await self._release.wait()
                return await original_commit_batch_now(items)
            finally:
                self._active_commit_count -= 1

        monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)

    async def wait_until_entered(self) -> None:
        await asyncio.wait_for(self.entered.wait(), timeout=1.0)

    def release(self) -> None:
        self._release.set()

    async def release_and_cancel_and_drain_tasks(self, *tasks: asyncio.Task[Any] | None) -> None:
        self.release()
        await cancel_and_drain_tasks(*tasks)


class StartupProbeDb:
    def __init__(
        self, *, select_row: tuple[str, bytes] | None, delete_error: Exception | None = None
    ):
        self._select_row = select_row
        self._delete_error = delete_error

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> object:
        if sql == SQL_WORKFLOW_UPSERT:
            assert params[0] == WORKFLOW_ID_STARTUP_PROBE
            return _AwaitableResult()
        if sql == SQL_WORKFLOW_SELECT_BY_ID:
            assert params == (WORKFLOW_ID_STARTUP_PROBE,)
            return _StaticCursorContext(self._select_row)
        if sql == SQL_WORKFLOW_DELETE:
            assert params == (WORKFLOW_ID_STARTUP_PROBE,)
            if self._delete_error is not None:
                raise self._delete_error
            return _AwaitableResult()
        raise AssertionError(f"unexpected SQL: {sql!r}")

    async def commit(self) -> None:
        return None
