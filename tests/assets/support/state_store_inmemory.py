import asyncio
import copy

from minions._internal._framework.logger import Logger
from minions._internal._framework.state_store import StoredWorkflowContext

from tests.assets.support.state_store_spied import SpiedStateStore


class InMemoryStateStore(SpiedStateStore):
    """In-memory implementation of StateStore for testing."""

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._contexts: dict[str, StoredWorkflowContext] = {}
        self._lock = asyncio.Lock()

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        async with self._lock:
            self._contexts[workflow_id] = StoredWorkflowContext(
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
                context=bytes(context),
            )

    async def delete_context(self, workflow_id: str) -> None:
        async with self._lock:
            self._contexts.pop(workflow_id, None)

    async def get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        async with self._lock:
            return [
                copy.deepcopy(ctx)
                for ctx in self._contexts.values()
                if ctx.orchestration_id == orchestration_id
            ]

    async def get_all_contexts(self) -> list[StoredWorkflowContext]:
        async with self._lock:
            return [copy.deepcopy(ctx) for ctx in self._contexts.values()]
