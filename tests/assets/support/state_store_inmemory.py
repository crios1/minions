import asyncio

from minions._internal._framework.logger import Logger
from minions._internal._framework.state_store import StateStore
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext

from tests.assets.support.mixin_spy import SpyMixin

class InMemoryStateStore(StateStore, SpyMixin):
    """In-memory implementation of StateStore for testing."""

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._contexts: dict[str, MinionWorkflowContext] = {}
        self._lock = asyncio.Lock()

    async def save_context(self, ctx: MinionWorkflowContext) -> None:
        async with self._lock:
            self._contexts[ctx.workflow_id] = ctx

    async def delete_context(self, workflow_id: str) -> None:
        async with self._lock:
            self._contexts.pop(workflow_id, None)

    async def load_all_contexts(self) -> list[MinionWorkflowContext]:
        async with self._lock:
            return list(self._contexts.values())