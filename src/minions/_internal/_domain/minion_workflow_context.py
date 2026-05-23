from dataclasses import dataclass
from typing import Generic

from .types import T_Event, T_Ctx

@dataclass
class MinionWorkflowContext(Generic[T_Event, T_Ctx]):
    """If you change this dataclass, review minion_workflow_context_codec.py and add any needed codec updates or migration."""

    minion_composite_key: str
    minion_modpath: str
    workflow_id: str
    event: T_Event
    context: T_Ctx
    context_cls: type
    next_step_index: int = 0
    error_msg: str | None = None
    started_at: float | None = None

    def as_dict(self) -> dict[str, object]:
        # explicit field assembly is much faster than dataclasses.asdict(...) here
        return {
            "minion_composite_key": self.minion_composite_key,
            "minion_modpath": self.minion_modpath,
            "workflow_id": self.workflow_id,
            "event": self.event,
            "context": self.context,
            "context_cls": self.context_cls,
            "next_step_index": self.next_step_index,
            "error_msg": self.error_msg,
            "started_at": self.started_at,
        }
