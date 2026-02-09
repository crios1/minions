from dataclasses import dataclass
from .directives import Directive, MinionStart, iter_directives_flat


@dataclass
class ScenarioPlan:
    directives: list[Directive]
    flat_directives: list[Directive]
    directive_index_map: dict[int, int]
    pipeline_event_targets: dict[str, int]

    def __init__(
        self,
        directives: list[Directive],
        *,
        pipeline_event_counts: dict[str, int],
    ) -> None:
        self.directives = directives
        self.flat_directives = list(iter_directives_flat(directives))
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
