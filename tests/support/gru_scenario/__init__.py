from minions._internal._domain.gru import Gru

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from .directives import (
    Concurrent,
    Directive,
    GruShutdown,
    MinionRunSpec,
    MinionStart,
    MinionStop,
    WaitWorkflows,
)
from .plan import ScenarioPlan
from .runner import ScenarioRunner
from .verify import ScenarioVerifier


async def run_gru_scenario(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    directives: list[Directive],
    *,
    pipeline_event_counts: dict[str, int],
    per_verification_timeout: float = 5.0,
) -> None:
    plan = ScenarioPlan(directives, pipeline_event_counts=pipeline_event_counts)

    runner = ScenarioRunner(gru, plan, per_verification_timeout=per_verification_timeout)
    result = await runner.run()

    verifier = ScenarioVerifier(
        plan,
        result,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
        per_verification_timeout=per_verification_timeout,
    )
    await verifier.verify()


__all__ = [
    "Directive",
    "MinionRunSpec",
    "MinionStart",
    "MinionStop",
    "Concurrent",
    "WaitWorkflows",
    "GruShutdown",
    "run_gru_scenario",
]
