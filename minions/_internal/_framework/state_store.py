from abc import abstractmethod

from .async_component import AsyncComponent
from .._domain.minion_workflow_context import MinionWorkflowContext


class StateStore(AsyncComponent):
    @abstractmethod
    async def save_context(self, ctx: MinionWorkflowContext):
        "override to implement your own StateStore, this method must store every field in `ctx`"

    @abstractmethod
    async def delete_context(self, workflow_id: str):
        "override to implement your own StateStore"

    @abstractmethod
    async def load_all_contexts(self) -> list[MinionWorkflowContext]:
        "override to implement your own StateStore"

    async def _save_context(self, ctx: MinionWorkflowContext):
        await self._mn_safe_run_and_log(
            method=self.save_context,
            method_args=[ctx],
            log_kwargs={'workflow_id': ctx.workflow_id}
        )

    async def _delete_context(self, workflow_id: str):
        await self._mn_safe_run_and_log(
            method=self.delete_context,
            method_args=[workflow_id],
            log_kwargs={'workflow_id': workflow_id}
        )

    async def _load_all_contexts(self) -> list[MinionWorkflowContext]:
        result = await self._mn_safe_run_and_log(
            self.load_all_contexts
        )
        return result or []
