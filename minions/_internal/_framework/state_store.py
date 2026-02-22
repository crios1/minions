from abc import abstractmethod

from .async_component import AsyncComponent
from .._domain.minion_workflow_context import MinionWorkflowContext


class StateStore(AsyncComponent):
    _mn_user_facing = True

    @abstractmethod
    async def save_context(self, ctx: MinionWorkflowContext) -> None:
        """Persist one workflow context.

        Implementers must store all fields on `ctx` so replay can reconstruct state.
        """

    @abstractmethod
    async def delete_context(self, workflow_id: str) -> None:
        """Delete one persisted workflow context by workflow id."""

    @abstractmethod
    async def get_all_contexts(self) -> list[MinionWorkflowContext]:
        """Return all persisted contexts.

        This is the only required read method so custom stores have a minimal contract.
        """

    async def get_contexts_for_minion(
        self,
        minion_modpath: str,
    ) -> list[MinionWorkflowContext]:
        """Return contexts for a single minion.

        Default behavior is correctness-first: load all then filter.
        Backends with indexing/query support should override this for performance.
        """
        contexts = await self.get_all_contexts()
        return [
            ctx for ctx in contexts
            if getattr(ctx, "minion_modpath", None) == minion_modpath
        ]

    async def _save_context(self, ctx: MinionWorkflowContext):
        await self._mn_safe_run_and_log(
            method=self.save_context,
            method_args=[ctx],
            log_kwargs={"workflow_id": ctx.workflow_id},
        )

    async def _delete_context(self, workflow_id: str):
        await self._mn_safe_run_and_log(
            method=self.delete_context,
            method_args=[workflow_id],
            log_kwargs={"workflow_id": workflow_id},
        )

    async def _get_all_contexts(self) -> list[MinionWorkflowContext]:
        result = await self._mn_safe_run_and_log(
            self.get_all_contexts
        )
        return result or []

    async def _get_contexts_for_minion(
        self,
        minion_modpath: str,
    ) -> list[MinionWorkflowContext]:
        result = await self._mn_safe_run_and_log(
            method=self.get_contexts_for_minion,
            method_args=[minion_modpath],
            log_kwargs={"minion_modpath": minion_modpath},
        )
        return result or []
