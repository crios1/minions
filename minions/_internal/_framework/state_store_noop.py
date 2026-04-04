from .state_store import StateStore
from .state_store import StoredWorkflowContext
from .logger_noop import NoOpLogger
from .logger import Logger

class NoOpStateStore(StateStore):
    def __init__(self, logger: Logger | None = None):
        super().__init__(logger=logger or NoOpLogger())

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        return None

    async def delete_context(self, workflow_id: str) -> None:
        return None

    async def get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        return []

    async def get_all_contexts(self) -> list[StoredWorkflowContext]:
        return []
