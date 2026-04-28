from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

from .async_component import AsyncComponent
from .logger import ERROR
from .minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    serialize_persisted_workflow_context,
)
from .._domain.minion_workflow_context import MinionWorkflowContext
from .._utils.format_exception_traceback import format_exception_traceback

@dataclass(frozen=True)
class StoredWorkflowContext:
    workflow_id: str
    orchestration_id: str
    context: bytes


@dataclass(frozen=True)
class PersistenceOperationResult:
    persisted: bool
    failure_stage: Literal["serialize", "save", "delete"] | None = None
    error: Exception | None = None
    retryable: bool = False


class StateStore(AsyncComponent):
    """Base class for durable workflow context storage.

    Custom stores must persist serialized context bytes by workflow ID, keep
    the orchestration ID with each saved context, support lookup by
    orchestration, support listing all saved contexts for startup recovery, and
    delete contexts when workflows finish. `save_context` and `delete_context`
    must return only after the requested change is durably reflected by the
    store, not merely after it has been accepted into an in-memory queue.
    Override `startup` and `shutdown` only when the store needs async setup or
    cleanup.
    """

    _mn_user_facing = True

    # User Code

    @abstractmethod
    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        """Save a serialized workflow context and return after it is persisted."""

    @abstractmethod
    async def delete_context(self, workflow_id: str) -> None:
        """Delete a stored workflow context and return after the delete is persisted."""

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
    ) -> PersistenceOperationResult:
        try:
            await self.save_context(workflow_id, orchestration_id, context)
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__}.save_context failed",
                error_type=type(e).__name__,
                error_message=str(e),
                traceback=format_exception_traceback(e),
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
            )
            return PersistenceOperationResult(
                persisted=False,
                failure_stage="save",
                error=e,
                retryable=True,
            )
        return PersistenceOperationResult(persisted=True)

    async def _mn_delete_context(self, workflow_id: str) -> PersistenceOperationResult:
        try:
            await self.delete_context(workflow_id)
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__}.delete_context failed",
                error_type=type(e).__name__,
                error_message=str(e),
                traceback=format_exception_traceback(e),
                workflow_id=workflow_id,
            )
            return PersistenceOperationResult(
                persisted=False,
                failure_stage="delete",
                error=e,
                retryable=True,
            )
        return PersistenceOperationResult(persisted=True)

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
    ) -> PersistenceOperationResult:
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
            return PersistenceOperationResult(
                persisted=False,
                failure_stage="serialize",
                error=e,
                retryable=False,
            )
        return await self._mn_save_context(
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
