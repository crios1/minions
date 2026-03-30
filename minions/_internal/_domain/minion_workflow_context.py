from dataclasses import dataclass, asdict
from typing import Generic

from .types import T_Event, T_Ctx

@dataclass
class MinionWorkflowContext(Generic[T_Event, T_Ctx]):
    """If you change this dataclass, review minion_workflow_context_codec.py and add any needed codec updates or migration."""

    minion_modpath: str
    workflow_id: str
    event: T_Event
    context: T_Ctx
    context_cls: type
    next_step_index: int = 0
    error_msg: str | None = None
    started_at: float | None = None

    def as_dict(self) -> dict:
        return asdict(self) # pragma: no cover
