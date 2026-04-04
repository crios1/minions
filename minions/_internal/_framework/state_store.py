from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

from .async_component import AsyncComponent
from .logger import ERROR
from .minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    serialize_persisted_workflow_context,
)
from .._domain.minion_workflow_context import MinionWorkflowContext
from .._utils.format_exception_traceback import format_exception_traceback

# todo: does it make a difference to use a msgspec here? or when is msgspec actually more efficient?
# todo: might rename this class
# gives a bit of a smell cuz "state"
# is embedded in the name
# and i suspect that this class
# may be being used during different
# states of the context
@dataclass(frozen=True)
class StoredWorkflowContext:
    workflow_id: str
    orchestration_id: str
    context: bytes


class StateStore(AsyncComponent):
    _mn_user_facing = True

    # User Code

    @abstractmethod
    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        """Store a serialized workflow context.

        Current runtime policy is fail-open: implementations should log persistence
        failures and allow workflow execution to continue.
        """

    @abstractmethod
    async def delete_context(self, workflow_id: str) -> None:
        """Delete a stored workflow context."""

    @abstractmethod
    async def get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        """Return stored workflow contexts for an orchestration."""

    @abstractmethod
    async def get_all_contexts(self) -> list[StoredWorkflowContext]:
        """Return all stored workflow contexts."""

    # Wrappers

    async def _mn_save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        await self._mn_safe_run_and_log_failure(
            method=self.save_context,
            method_args=[workflow_id, orchestration_id, context],
            log_kwargs={
                "workflow_id": workflow_id,
                "orchestration_id": orchestration_id,
            },
        )

    async def _mn_delete_context(self, workflow_id: str) -> None:
        await self._mn_safe_run_and_log_failure(
            method=self.delete_context,
            method_args=[workflow_id],
            log_kwargs={"workflow_id": workflow_id},
        )

    async def _mn_get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        stored_contexts = await self._mn_safe_run_and_log_failure(
            method=self.get_contexts_for_orchestration,
            method_args=[orchestration_id],
            log_kwargs={"orchestration_id": orchestration_id},
        ) or []
        return stored_contexts

    async def _mn_get_all_contexts(self) -> list[StoredWorkflowContext]:
        stored_contexts = await self._mn_safe_run_and_log_failure(
            method=self.get_all_contexts,
        ) or []
        return stored_contexts

    # Helpers

    async def _mn_serialize_and_save_context(
        self,
        ctx: MinionWorkflowContext,
    ) -> None:
        workflow_id = ctx.workflow_id
        orchestration_id = ctx.minion_composite_key

        try:
            serialized_context = serialize_persisted_workflow_context(ctx)
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                "StateStore failed to serialize workflow context",
                error_type=type(e).__name__,
                error_message=str(e),
                traceback=format_exception_traceback(e),
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
                state_store=type(self).__name__,
            )
            return
        await self._mn_save_context(
            workflow_id,
            orchestration_id,
            serialized_context,
        )

    async def _mn_decode_stored_contexts(
        self,
        stored_contexts: list[StoredWorkflowContext],
        *,
        event_cls: Any | None = None,
        context_cls: type | None = None,
        log_action: str,
        log_kwargs: dict[str, object] | None = None,
    ) -> list[MinionWorkflowContext]:
        contexts: list[MinionWorkflowContext] = []
        for stored_context in stored_contexts:
            try:
                contexts.append(
                    deserialize_workflow_context_blob(
                        stored_context.context,
                        event_cls=event_cls,
                        context_cls=context_cls,
                    )
                )
            except Exception as e:
                await self._mn_logger._log(
                    ERROR,
                    "StateStore failed to decode stored workflow context",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback=format_exception_traceback(e),
                    workflow_id=stored_context.workflow_id,
                    orchestration_id=stored_context.orchestration_id,
                    state_store=type(self).__name__,
                    **(log_kwargs or {}),
                )
        return contexts

    async def _mn_get_decoded_contexts_for_orchestration(
        self,
        orchestration_id: str,
        *,
        event_cls: Any | None = None,
        context_cls: type | None = None,
    ) -> list[MinionWorkflowContext]:
        stored_contexts = await self._mn_get_contexts_for_orchestration(
            orchestration_id,
        )
        decoded_contexts = await self._mn_decode_stored_contexts(
            stored_contexts,
            event_cls=event_cls,
            context_cls=context_cls,
            log_action="get_contexts_for_orchestration",
            log_kwargs={"requested_orchestration_id": orchestration_id},
        )
        return decoded_contexts

    async def _mn_get_all_decoded_contexts(self) -> list[MinionWorkflowContext]:
        stored_contexts = await self._mn_get_all_contexts()
        decoded_contexts = await self._mn_decode_stored_contexts(
            stored_contexts,
            log_action="get_all_contexts",
        )
        return decoded_contexts
