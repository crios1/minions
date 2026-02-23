from dataclasses import dataclass
from collections.abc import Sequence
from .directives import Directive, MinionStart, iter_directives_flat


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
        pipeline_event_counts: dict[str, int],
    ) -> None:
        self.directives = list(directives)
        self.flat_directives = list(iter_directives_flat(self.directives))
        self._validate_unique_directive_instances()
        self.directive_index_map = {id(d): idx for idx, d in enumerate(self.flat_directives)}
        self.pipeline_event_targets = dict(pipeline_event_counts)
        self._validate()

    def directive_index(self, directive: Directive) -> int:
        return self.directive_index_map[id(directive)]

    def _validate(self) -> None:
        for pipeline, count in self.pipeline_event_targets.items():
            if not isinstance(count, int):
                raise ValueError(
                    f"pipeline_event_counts[{pipeline!r}] must be an int, got {type(count).__name__}."
                )
            if count < 0:
                raise ValueError(f"pipeline_event_counts[{pipeline!r}] must be >= 0, got {count}.")

        started_pipelines = {
            d.pipeline
            for d in self.flat_directives
            if isinstance(d, MinionStart) and d.expect_success
        }

        missing = sorted(started_pipelines - set(self.pipeline_event_targets))
        if missing:
            raise ValueError(
                "Missing pipeline_event_counts entries for started pipelines: "
                + ", ".join(missing)
            )

        unused = sorted(set(self.pipeline_event_targets) - started_pipelines)
        if unused:
            raise ValueError(
                "pipeline_event_counts contains entries for pipelines not started in directives: "
                + ", ".join(unused)
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
