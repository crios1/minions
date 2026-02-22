from .state_store import StateStore
from .logger_noop import NoOpLogger
from .logger import Logger
from .._domain.minion_workflow_context import MinionWorkflowContext

class NoOpStateStore(StateStore):
    def __init__(self, logger: Logger | None = None):
        super().__init__(logger=logger or NoOpLogger())

    async def save_context(self, ctx: MinionWorkflowContext) -> None:
        return None

    async def delete_context(self, workflow_id: str) -> None:
        return None
    
    async def get_all_contexts(self) -> list[MinionWorkflowContext]:
        return []
