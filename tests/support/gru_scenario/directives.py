import inspect
from dataclasses import dataclass, field
from typing import Iterable, Iterator

from minions._internal._domain.gru import Gru


_MINION_START_PARAMS = set(inspect.signature(Gru.start_minion).parameters)
_MINION_STOP_PARAMS = set(inspect.signature(Gru.stop_minion).parameters)


class Directive:
    pass


@dataclass(frozen=True)
class RuntimeExpectSpec:
    persistence: dict[str, int] | None = None
    resolutions: dict[str, dict[str, int]] | None = None


@dataclass(frozen=True)
class MinionStart(Directive):
    minion: str
    pipeline: str
    minion_config_path: str | None = None
    expect_success: bool = True

    def as_kwargs(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k in _MINION_START_PARAMS}


@dataclass(frozen=True)
class MinionStop(Directive):
    name_or_instance_id: str
    expect_success: bool

    def as_kwargs(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k in _MINION_STOP_PARAMS}


@dataclass(frozen=True)
class Concurrent(Directive):
    directives: tuple[Directive, ...]

    def __init__(self, *directives: Directive):
        object.__setattr__(self, "directives", directives)


@dataclass(frozen=True)
class WaitWorkflowCompletions(Directive):
    minion_names: set[str] | None = None


@dataclass(frozen=True)
class WaitWorkflowStartsThen(Directive):
    expected: dict[str, int]
    directive: Directive


@dataclass(frozen=True)
class WaitWorkflows(WaitWorkflowCompletions):
    """Temporary compatibility alias for WaitWorkflowCompletions."""


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
        if isinstance(d, WaitWorkflowStartsThen):
            yield from iter_directives_flat((d.directive,))
            continue
        yield d
