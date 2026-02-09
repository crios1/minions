import inspect
from dataclasses import dataclass
from typing import Iterable, Iterator

from minions._internal._domain.gru import Gru


_MINION_START_PARAMS = set(inspect.signature(Gru.start_minion).parameters)
_MINION_STOP_PARAMS = set(inspect.signature(Gru.stop_minion).parameters)


class Directive:
    pass


@dataclass(frozen=True)
class MinionRunSpec:
    workflow_resolutions: dict[str, int] | None = None  # {'succeeded': 1, 'failed': 1, 'aborted': 1}
    minion_call_overrides: dict[str, int] | None = None


@dataclass(frozen=True)
class MinionStart(Directive):
    minion: str
    pipeline: str
    minion_config_path: str | None = None
    expect_success: bool = True
    expect: MinionRunSpec | None = None

    def as_kwargs(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k in _MINION_START_PARAMS}


@dataclass(frozen=True)
class MinionStop(Directive):
    expect_success: bool
    name_or_instance_id: str

    def as_kwargs(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if k in _MINION_STOP_PARAMS}


@dataclass(frozen=True)
class Concurrent(Directive):
    directives: tuple[Directive, ...]

    def __init__(self, *directives: Directive):
        object.__setattr__(self, "directives", directives)


@dataclass(frozen=True)
class WaitWorkflows(Directive):
    minion_names: set[str] | None = None


@dataclass(frozen=True)
class GruShutdown(Directive):
    expect_success: bool = True


def iter_directives_flat(directives: Iterable["Directive"]) -> Iterator["Directive"]:
    for d in directives:
        if isinstance(d, Concurrent):
            yield from iter_directives_flat(d.directives)
            continue
        yield d