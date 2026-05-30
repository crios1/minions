import inspect
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from minions._internal._domain.pipeline import Pipeline

_ORCHESTRATION_START_PARAMS = set(inspect.signature(Gru.start_orchestration).parameters)
_ORCHESTRATION_STOP_PARAMS = set(inspect.signature(Gru.stop_orchestration).parameters)


class Directive:
    pass


@dataclass(frozen=True)
class RuntimeExpectSpec:
    persistence: dict[str, int] | None = None
    resolutions: dict[str, dict[str, int]] | None = None
    workflow_steps: dict[str, dict[str, int]] | None = None
    workflow_steps_mode: str = "at_least"


@dataclass(frozen=True)
class OrchestrationStart(Directive):
    minion: str | type[Minion[Any, Any]]
    pipeline: str | type[Pipeline[Any]]
    minion_config_path: str | None = None
    minion_config: object | None = None
    expect_success: bool = True

    def as_kwargs(self) -> dict[str, object]:
        return {k: v for k, v in self.__dict__.items() if k in _ORCHESTRATION_START_PARAMS}

    @property
    def minion_modpath(self) -> str:
        if isinstance(self.minion, str):
            return self.minion
        return self.minion.__module__

    @property
    def pipeline_modpath(self) -> str:
        if isinstance(self.pipeline, str):
            return self.pipeline
        return self.pipeline.__module__


@dataclass(frozen=True)
class OrchestrationStop(Directive):
    id: str | OrchestrationStart
    expect_success: bool

    def as_kwargs(self) -> dict[str, object]:
        return {k: v for k, v in self.__dict__.items() if k in _ORCHESTRATION_STOP_PARAMS}


@dataclass(frozen=True)
class Concurrent(Directive):
    directives: tuple[Directive, ...]

    def __init__(self, *directives: Directive):
        object.__setattr__(self, "directives", directives)


@dataclass(frozen=True)
class WaitWorkflowCompletions(Directive):
    minion_names: set[str] | None = None
    workflow_steps_mode: str = "at_least"


@dataclass(frozen=True)
class AfterWorkflowStepStarts(Directive):
    expected: dict[str, dict[str, int]]
    directive: Directive


@dataclass(frozen=True)
class ExpectRuntime(Directive):
    at: str | int = "latest"
    expect: RuntimeExpectSpec = field(default_factory=RuntimeExpectSpec)


@dataclass(frozen=True)
class GruShutdown(Directive):
    expect_success: bool = True


def iter_directives_flat(directives: Iterable["Directive"]) -> Iterator["Directive"]:
    for d in directives:
        if isinstance(d, Concurrent):
            yield from iter_directives_flat(d.directives)
            continue
        if isinstance(d, AfterWorkflowStepStarts):
            yield from iter_directives_flat((d.directive,))
            continue
        yield d
