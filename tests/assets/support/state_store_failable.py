import asyncio

from minions._internal._framework.logger import Logger
from minions._internal._framework.state_store import StoredWorkflowContext

from .state_store_inmemory import InMemoryStateStore


class FailureControl:
    def __init__(self) -> None:
        self._enabled = False
        self._failure_count = 0
        self._failure_observed = asyncio.Condition()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def count(self) -> int:
        return self._failure_count

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    async def _record_failure(self) -> None:
        async with self._failure_observed:
            self._failure_count += 1
            self._failure_observed.notify_all()

    async def wait_for(self, count: int = 1, *, timeout: float = 1.0) -> None:
        if count < 1:
            raise ValueError("failure count must be positive")

        async def wait() -> None:
            async with self._failure_observed:
                await self._failure_observed.wait_for(lambda: self._failure_count >= count)

        await asyncio.wait_for(wait(), timeout=timeout)


class FailableStateStore(InMemoryStateStore):
    """In-memory StateStore with independently controlled save and delete failures."""

    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        self._save_failure_control = FailureControl()
        self._delete_failure_control = FailureControl()
        self._saved_context_history: list[StoredWorkflowContext] = []

    @property
    def save_failures(self) -> FailureControl:
        return self._save_failure_control

    @property
    def delete_failures(self) -> FailureControl:
        return self._delete_failure_control

    @property
    def saved_context_history(self) -> tuple[StoredWorkflowContext, ...]:
        return tuple(self._saved_context_history)

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        if self._save_failure_control.enabled:
            await self._save_failure_control._record_failure()
            raise RuntimeError("controlled save failure")
        await super().save_context(workflow_id, orchestration_id, context)
        self._saved_context_history.append(
            StoredWorkflowContext(
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
                context=bytes(context),
            )
        )

    async def delete_context(self, workflow_id: str) -> None:
        if self._delete_failure_control.enabled:
            await self._delete_failure_control._record_failure()
            raise RuntimeError("controlled delete failure")
        await super().delete_context(workflow_id)
