import asyncio
import time
from typing import Any

import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import persist_workflow_context
from minions._internal._framework.state_store_sqlite import (
    PendingWrite,
    SQLiteStateStore,
)
from minions._internal._utils.serialization import serialize


def mk_ctx(
    i: int = 0,
    size: int = 32,
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


async def cancel_and_suppress_task_exceptions(*tasks: asyncio.Task[Any] | None) -> None:
    """Allow tasks to finish, cancel pending tasks, and suppress their exceptions."""
    active_tasks = [task for task in tasks if task is not None]
    if not active_tasks:
        return

    _done, pending = await asyncio.wait(active_tasks, timeout=2.0)
    for task in pending:
        task.cancel()
    await asyncio.gather(*active_tasks, return_exceptions=True)


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
