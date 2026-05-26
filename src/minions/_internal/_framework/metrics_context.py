import contextvars
from types import TracebackType
from typing import Literal, Self

ResourceMetricCallerKind = Literal["minion", "pipeline", "resource", "unknown"]
ResourceMetricCaller = tuple[ResourceMetricCallerKind, str]


_resource_metric_orchestration_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "resource_metric_orchestration_id",
    default="",
)
_resource_metric_caller: contextvars.ContextVar[ResourceMetricCaller] = contextvars.ContextVar(
    "resource_metric_caller",
    default=("unknown", ""),
)


def current_resource_metric_orchestration_id() -> str:
    return _resource_metric_orchestration_id.get()


def current_resource_metric_caller() -> ResourceMetricCaller:
    return _resource_metric_caller.get()


class resource_metric_context:
    """Context manager that sets ambient resource metric labeling context.

    Implemented as a class so ContextVar tokens are reset only from __exit__;
    generator-based context managers can be finalized from a different asyncio
    context during cancellation.
    """

    def __init__(
        self,
        *,
        orchestration_id: str | None = None,
        caller_kind: ResourceMetricCallerKind | None = None,
        caller: str | None = None,
    ) -> None:
        self._orchestration_id: str | None = orchestration_id
        self._caller_kind: ResourceMetricCallerKind | None = caller_kind
        self._caller: str | None = caller
        self._orchestration_token: contextvars.Token[str] | None = None
        self._caller_token: contextvars.Token[ResourceMetricCaller] | None = None

    def __enter__(self) -> Self:
        if self._orchestration_id is not None:
            self._orchestration_token = _resource_metric_orchestration_id.set(
                self._orchestration_id
            )

        if self._caller_kind is not None or self._caller is not None:
            caller_kind = self._caller_kind or "unknown"
            caller = self._caller if self._caller is not None else ""
            self._caller_token = _resource_metric_caller.set(
                (caller_kind, caller)
            )

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._caller_token is not None:
            _resource_metric_caller.reset(self._caller_token)
            self._caller_token = None
        if self._orchestration_token is not None:
            _resource_metric_orchestration_id.reset(self._orchestration_token)
            self._orchestration_token = None
