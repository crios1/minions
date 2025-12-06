from dataclasses import dataclass, field


# === Supporting Types ===

@dataclass
class ConflictingMinion:
    instance_id: str
    modpath: str
    config_modpath: str
    pipeline_modpath: str


# === Gru Return Types ===

@dataclass
class GruResult:
    success: bool
    reason: str | None = None
    suggestion: str | None = None

    # def __post_init__(self):
    #     if not self.success and not self.reason:
    #         raise ValueError("`reason` must be set if `success` is False")

@dataclass
class StopMinionResult(GruResult):
    conflicts: list[ConflictingMinion] = field(default_factory=list)

@dataclass
class StartMinionResult(GruResult):
    name: str | None = None
    instance_id: str | None = None

@dataclass
class MinionStatusResult(GruResult):
    name: str | None = None
    instance_id: str | None = None
    composite_key: str | None = None

    status: str | None = None  # e.g. "running", "stopped", "cancelled", "error"

    started_at: float | None = None  # UNIX timestamp (or datetime if you prefer)
    uptime_seconds: float | None = None

    workflows_started: int | None = None
    workflows_completed: int | None = None
    workflows_inflight: int | None = None
