import time
from typing import Any

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import persist_workflow_context
from minions._internal._framework.state_store_sqlite import (
    SQL_WORKFLOW_DELETE,
    SQL_WORKFLOW_SELECT_BY_ID,
    SQL_WORKFLOW_UPSERT,
    WORKFLOW_ID_STARTUP_PROBE,
)
from minions._internal._utils.serialization import serialize


def mk_ctx(i=0, size=32, modpath="app.minion"):
    return MinionWorkflowContext(
        minion_composite_key=f"{modpath}|cfg-{i}|app.pipeline",
        minion_modpath=modpath,
        workflow_id=f"wf-{i}",
        event={"i": i},
        context={"p": "x" * size},
        context_cls=dict,
        next_step_index=i,
        started_at=time.time(),
        error_msg=None,
    )


def blob_for(ctx: MinionWorkflowContext) -> bytes:
    return serialize(persist_workflow_context(ctx))


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

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _AwaitableResult:
    def __await__(self):
        async def _done():
            return None

        return _done().__await__()


class StartupProbeDb:
    def __init__(self, *, select_row: tuple[str, bytes] | None, delete_error: Exception | None = None):
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
