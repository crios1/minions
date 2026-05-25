from dataclasses import dataclass


@dataclass(frozen=True)
class MinionWorkflowHandle:
    """Stable workflow identity for optional business log correlation."""

    orchestration_id: str
    workflow_id: str
