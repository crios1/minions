from minions.interfaces import StateStore
from minions.types import MinionWorkflowContext

class MyStateStore(StateStore):
    """How to bring-your-own state store."""
    async def save_context(self, ctx: MinionWorkflowContext): ...

    async def delete_context(self, workflow_id: str): ...

    async def load_all_contexts(self) -> list[MinionWorkflowContext]: ...
