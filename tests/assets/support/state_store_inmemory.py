import asyncio
import copy

from minions._internal._framework.logger import Logger
from minions._internal._framework.state_store_payload_types import StateStorePayload

from tests.assets.support.state_store_spied import SpiedStateStore


class InMemoryStateStore(SpiedStateStore):
    """In-memory implementation of StateStore for testing."""

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._contexts: dict[str, StateStorePayload] = {}
        self._lock = asyncio.Lock()

    async def save_context(
        self,
        workflow_id: str,
        payload: StateStorePayload,
    ) -> None:
        async with self._lock:
            self._contexts[workflow_id] = copy.deepcopy(payload)

    async def delete_context(self, workflow_id: str) -> None:
        async with self._lock:
            self._contexts.pop(workflow_id, None)

    async def get_all_contexts(self) -> list[StateStorePayload]:
        async with self._lock:
            return [copy.deepcopy(value) for value in self._contexts.values()]
