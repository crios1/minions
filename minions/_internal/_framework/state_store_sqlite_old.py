import aiosqlite
import importlib
import traceback

from typing import List

from .logger import Logger, ERROR
from .state_store import StateStore
from .._domain.minion_workflow_context import MinionWorkflowContext
from .._utils.serialization import serialize, deserialize

def serialize_type(t: type) -> str:
    return f"{t.__module__}.{t.__qualname__}"

def deserialize_type(s: str) -> type:
    module_name, _, class_name = s.rpartition(".")
    module = importlib.import_module(module_name)
    return getattr(module, class_name)

def serialize_context(ctx: MinionWorkflowContext) -> str:
    d = ctx.as_dict()
    d["context_cls"] = serialize_type(ctx.context_cls)
    return serialize(d).decode("utf-8")

def deserialize_context(ctx: bytes) -> MinionWorkflowContext:
    d = deserialize(ctx)
    d["context_cls"] = deserialize_type(d["context_cls"])
    return MinionWorkflowContext(**d)

class SQLiteStateStore(StateStore):
    def __init__(self, db_path: str, logger: Logger):
        "db_path=':memory:' can be used to make an ephemeral in-memory DB, could be useful for testing"
        super().__init__(logger)
        self.db_path = db_path
        self._ensured_table = False

    async def _ensure_table(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS workflows (
                    workflow_id TEXT PRIMARY KEY,
                    context_json BLOB NOT NULL
                );
            """)
            await db.commit()
        self._ensured_table = True

    async def save_context(self, ctx: MinionWorkflowContext):
        if not self._ensured_table:
            await self._ensure_table()
        async with aiosqlite.connect(self.db_path) as db:
            try:
                s_ctx = serialize_context(ctx)
            except Exception as e:
                await self._logger._log(
                    ERROR,
                    f"{type(self).__name__}.save_context failed to serialize context",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
                    context=ctx
                )
            else:
                await db.execute(
                    """
                    INSERT INTO workflows (workflow_id, context_json)
                    VALUES (?, ?)
                    ON CONFLICT(workflow_id) DO UPDATE SET
                        context_json = excluded.context_json
                    """,
                    (
                        ctx.workflow_id,
                        s_ctx,
                    )
                )
                await db.commit()

    async def delete_context(self, workflow_id: str):
        if not self._ensured_table:
            await self._ensure_table()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM workflows WHERE workflow_id = ?", (workflow_id,))
            await db.commit()

    async def load_all_contexts(self) -> List[MinionWorkflowContext]:
        if not self._ensured_table:
            await self._ensure_table()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT workflow_id, context_json FROM workflows") as cursor:
                rows = await cursor.fetchall()
        if not rows:
            return []
        contexts = []
        for row in rows:
            try:
                context = deserialize_context(row[1])
            except Exception as e:
                await self._logger._log(
                    ERROR,
                    f"{type(self).__name__}.load_all_contexts failed to deserialize context",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
                    context=row[1]
                )
            else:
                contexts.append(context)
        return contexts
