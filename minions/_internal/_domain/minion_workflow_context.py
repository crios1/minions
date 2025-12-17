from dataclasses import dataclass, asdict
from typing import Generic

from .types import T_Event, T_Ctx

@dataclass
class MinionWorkflowContext(Generic[T_Event, T_Ctx]):
    "`context` must be a dict-like object or a dataclass so i can serialize it to statestore"
    minion_modpath: str
    workflow_id: str
    event: T_Event
    context: T_Ctx
    context_cls: type
    step_index: int = 0
    # failed: bool = False
    error_msg: str | None = None
    started_at: float | None = None

    def as_dict(self) -> dict:
        return asdict(self) # pragma: no cover

    @classmethod
    def from_dict(cls: type["MinionWorkflowContext[T_Event, T_Ctx]"], d: dict) -> "MinionWorkflowContext[T_Event, T_Ctx]":
        return cls(**d) # pragma: no cover
