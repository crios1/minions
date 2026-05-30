import importlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, TypeGuard

from minions._internal._domain.component_identity import get_component_id
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

    @property
    def pipeline_id(self) -> str:
        if isinstance(self.pipeline, str):
            try:
                pipeline_cls = _get_pipeline_class(self.pipeline)
            except Exception:
                return self.pipeline_modpath
        else:
            pipeline_cls = self.pipeline
        return get_component_id(pipeline_cls) or self.pipeline_modpath


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


def _get_pipeline_class(pipeline_modpath: str) -> type[Pipeline[Any]]:
    mod = importlib.import_module(pipeline_modpath)

    def is_pipeline_class(obj: object) -> TypeGuard[type[Pipeline[Any]]]:
        return isinstance(obj, type) and issubclass(obj, Pipeline)

    pipeline_attr = getattr(mod, "pipeline", None)
    if pipeline_attr is None:
        pipeline_classes: list[type[Pipeline[Any]]] = [
            obj for obj in vars(mod).values()
            if is_pipeline_class(obj) and obj is not Pipeline
        ]
        if len(pipeline_classes) == 1:
            return pipeline_classes[0]
        if len(pipeline_classes) == 0:
            raise ImportError(
                f"Module '{pipeline_modpath}' must define a `pipeline` variable or contain at least one subclass of `Pipeline`."
            )
        raise ImportError(
            f"Module '{pipeline_modpath}' contains multiple Pipeline subclasses but no explicit `pipeline` variable to resolve the entrypoint."
        )
    if is_pipeline_class(pipeline_attr):
        return pipeline_attr
    raise TypeError(f"`pipeline` attribute in module '{pipeline_modpath}' is not a subclass of Pipeline")
