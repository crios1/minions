from abc import abstractmethod

from .async_component import AsyncComponent
from .logger import ERROR
from .minion_workflow_context_codec import (
    deserialize_workflow_context,
    serialize_workflow_context,
)
from .state_store_payload_types import StateStorePayload
from .._domain.minion_workflow_context import MinionWorkflowContext


class StateStore(AsyncComponent):
    _mn_user_facing = True

    @abstractmethod
    async def save_context(
        self,
        workflow_id: str,
        payload: StateStorePayload,
    ) -> None:
        """Persist one storage-safe workflow-context payload by workflow id.

        Current runtime policy is fail-open: implementations should log persistence
        failures and allow workflow execution to continue.
        """

    @abstractmethod
    async def delete_context(self, workflow_id: str) -> None:
        """Delete one persisted workflow context by workflow id."""

    @abstractmethod
    async def get_all_contexts(self) -> list[StateStorePayload]:
        """Return all persisted storage-safe workflow-context payloads."""

    async def get_contexts_for_minion(
        self,
        minion_modpath: str,
    ) -> list[MinionWorkflowContext]:
        """Return contexts for a single minion.

        Default behavior is correctness-first: load all then filter.
        Backends with indexing/query support should override this for performance.
        """
        contexts = await self._get_all_contexts()
        return [
            ctx for ctx in contexts
            if getattr(ctx, "minion_modpath", None) == minion_modpath
        ]

    async def _save_context(self, ctx: MinionWorkflowContext):
        payload = serialize_workflow_context(ctx)
        await self._mn_safe_run_and_log(
            method=self.save_context,
            method_args=[ctx.workflow_id, payload],
            log_kwargs={"workflow_id": ctx.workflow_id},
        )

    async def _delete_context(self, workflow_id: str):
        await self._mn_safe_run_and_log(
            method=self.delete_context,
            method_args=[workflow_id],
            log_kwargs={"workflow_id": workflow_id},
        )

    async def _get_all_contexts(self) -> list[MinionWorkflowContext]:
        payloads = await self._mn_safe_run_and_log(self.get_all_contexts) or []
        contexts: list[MinionWorkflowContext] = []
        for payload in payloads:
            try:
                contexts.append(deserialize_workflow_context(dict(payload)))
            except Exception as e:
                await self._mn_logger._log(
                    ERROR,
                    f"{type(self).__name__}.get_all_contexts load failed",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    payload=payload,
                )
        return contexts

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
