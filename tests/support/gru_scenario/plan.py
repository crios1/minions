from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.pipeline import Pipeline

from .directives import (
    AfterWorkflowStepStarts,
    Concurrent,
    Directive,
    ExpectRuntime,
    OrchestrationStart,
    OrchestrationStop,
    WaitWorkflowCompletions,
    iter_directives_flat,
)

PipelineEventCountKey = str | type[Any]


@dataclass
class ScenarioPlan:
    directives: list[Directive]
    flat_directives: list[Directive]
    directive_index_map: dict[int, int]
    pipeline_event_targets: dict[str, int]

    def __init__(
        self,
        directives: Sequence[Directive],
        *,
        pipeline_event_counts: Mapping[Any, object],
    ) -> None:
        self.directives = list(directives)
        self.flat_directives = list(iter_directives_flat(self.directives))
        self._validate_unique_directive_instances()
        self.directive_index_map = {id(d): idx for idx, d in enumerate(self.flat_directives)}
        self.pipeline_event_targets = {}
        for pipeline, count in pipeline_event_counts.items():
            pipeline_id = self._pipeline_event_count_key_to_id(pipeline)
            if not isinstance(count, int):
                raise ValueError(
                    f"pipeline_event_counts[{pipeline!r}] must be an int, got "
                    f"{type(count).__name__}."
                )
            self.pipeline_event_targets[pipeline_id] = count
        self._validate()

    @staticmethod
    def _pipeline_event_count_key_to_id(pipeline: object) -> str:
        if isinstance(pipeline, str):
            return pipeline
        if not isinstance(pipeline, type) or not issubclass(pipeline, Pipeline):
            raise TypeError(
                "pipeline_event_counts keys must be pipeline IDs, module path strings, "
                f"or Pipeline subclasses; got {pipeline!r}."
            )
        pipeline_cls = cast(type[Any], pipeline)
        return get_component_id(pipeline_cls) or (
            f"{pipeline_cls.__module__}.{pipeline_cls.__name__}"
        )

    def directive_index(self, directive: Directive) -> int:
        return self.directive_index_map[id(directive)]

    def _validate(self) -> None:
        for pipeline, count in self.pipeline_event_targets.items():
            if count < 0:
                raise ValueError(f"pipeline_event_counts[{pipeline!r}] must be >= 0, got {count}.")

        self._validate_concurrent_dependency_boundaries()

        for directive in self.flat_directives:
            if not isinstance(directive, WaitWorkflowCompletions):
                continue
            if directive.orchestrations is None:
                continue

            wait_index = self.directive_index(directive)
            seen_start_ids: set[int] = set()
            for start in directive.orchestrations:
                start_id = id(start)
                if start_id in seen_start_ids:
                    raise ValueError(
                        "WaitWorkflowCompletions.orchestrations must not contain "
                        "duplicate directive references."
                    )
                seen_start_ids.add(start_id)
                start_index = self.directive_index_map.get(start_id)
                if start_index is None:
                    raise ValueError(
                        "WaitWorkflowCompletions references an OrchestrationStart "
                        "outside this ScenarioPlan."
                    )
                if start_index >= wait_index:
                    raise ValueError(
                        "WaitWorkflowCompletions may reference only OrchestrationStart "
                        "directives that precede it."
                    )

        self._validate_expectation_references(self.directives)

    def _validate_concurrent_dependency_boundaries(self) -> None:
        starts_by_concurrent_group: dict[int, set[int]] = {}
        dependencies_by_concurrent_group: dict[
            int, list[tuple[str, set[int] | None]]
        ] = {}

        def visit(
            directives: Sequence[Directive],
            concurrent_groups: tuple[int, ...],
        ) -> None:
            for directive in directives:
                if isinstance(directive, Concurrent):
                    group_id = id(directive)
                    starts_by_concurrent_group.setdefault(group_id, set())
                    dependencies_by_concurrent_group.setdefault(group_id, [])
                    visit(directive.directives, (*concurrent_groups, group_id))
                    continue

                if isinstance(directive, OrchestrationStart):
                    for group_id in concurrent_groups:
                        starts_by_concurrent_group[group_id].add(id(directive))
                    continue

                dependency_start_ids: set[int] | None = set()
                if isinstance(directive, OrchestrationStop):
                    if isinstance(directive.id, OrchestrationStart):
                        dependency_start_ids.add(id(directive.id))
                elif isinstance(directive, WaitWorkflowCompletions):
                    if directive.orchestrations is None:
                        dependency_start_ids = None
                    else:
                        dependency_start_ids.update(id(start) for start in directive.orchestrations)
                elif isinstance(directive, AfterWorkflowStepStarts):
                    dependency_start_ids.update(id(start) for start in directive.expected)
                    wrapped = directive.directive
                    if isinstance(wrapped, OrchestrationStop) and isinstance(
                        wrapped.id, OrchestrationStart
                    ):
                        dependency_start_ids.add(id(wrapped.id))
                elif isinstance(directive, ExpectRuntime):
                    for section in (
                        directive.expect.persistence,
                        directive.expect.resolutions,
                        directive.expect.workflow_steps,
                    ):
                        if section is not None:
                            dependency_start_ids.update(id(start) for start in section)

                if dependency_start_ids is None or dependency_start_ids:
                    for group_id in concurrent_groups:
                        dependencies_by_concurrent_group[group_id].append(
                            (type(directive).__name__, dependency_start_ids)
                        )

        visit(self.directives, ())

        for group_id, dependencies in dependencies_by_concurrent_group.items():
            concurrent_start_ids = starts_by_concurrent_group[group_id]
            for directive_name, dependency_start_ids in dependencies:
                conflicting_start_ids = (
                    concurrent_start_ids
                    if dependency_start_ids is None
                    else concurrent_start_ids & dependency_start_ids
                )
                if not conflicting_start_ids:
                    continue
                raise ValueError(
                    f"{directive_name} cannot depend on an OrchestrationStart in the "
                    "same Concurrent group; place the dependent directive after the "
                    "Concurrent group so the dependency is ordered."
                )

    def _validate_expectation_references(
        self,
        directives: Sequence[Directive],
    ) -> None:
        for directive in directives:
            if isinstance(directive, Concurrent):
                self._validate_expectation_references(directive.directives)
                continue

            references: object | None = None
            owner = type(directive).__name__
            boundary_index: int | None = None
            if isinstance(directive, AfterWorkflowStepStarts):
                references = directive.expected
                boundary_index = self.directive_index(directive.directive)
            elif isinstance(directive, ExpectRuntime):
                references = {
                    start
                    for section in (
                        directive.expect.persistence,
                        directive.expect.resolutions,
                        directive.expect.workflow_steps,
                    )
                    if section is not None
                    for start in section
                }
                boundary_index = self.directive_index(directive)

            if references is None:
                continue
            for start in references:
                start_index = self.directive_index_map.get(id(start))
                if start_index is None:
                    raise ValueError(
                        f"{owner} references an OrchestrationStart outside this ScenarioPlan."
                    )
                if boundary_index is not None and start_index >= boundary_index:
                    raise ValueError(
                        f"{owner} may reference only OrchestrationStart directives that precede it."
                    )

    def _validate_unique_directive_instances(self) -> None:
        indices_by_id: dict[int, list[int]] = {}
        for idx, directive in enumerate(self.flat_directives):
            indices_by_id.setdefault(id(directive), []).append(idx)

        collisions = [
            (self.flat_directives[idxs[0]], idxs)
            for idxs in indices_by_id.values()
            if len(idxs) > 1
        ]
        if not collisions:
            return

        parts = [
            f"{type(directive).__name__} at flat indices {idxs}"
            for directive, idxs in collisions
        ]
        raise ValueError(
            "Directives must be unique instances in a ScenarioPlan; "
            "reusing the same directive object is not supported: "
            + "; ".join(parts)
        )
