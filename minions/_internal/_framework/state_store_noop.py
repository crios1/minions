from .state_store import StateStore
from .logger_noop import NoOpLogger
from .logger import Logger
from .state_store_payload_types import StateStorePayload

class NoOpStateStore(StateStore):
    def __init__(self, logger: Logger | None = None):
        super().__init__(logger=logger or NoOpLogger())

    async def save_context(
        self,
        workflow_id: str,
        payload: StateStorePayload,
    ) -> None:
        return None

    async def delete_context(self, workflow_id: str) -> None:
        return None
    
    async def get_all_contexts(self) -> list[StateStorePayload]:
        return []
