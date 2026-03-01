from minions._internal._domain.gru import Gru

from tests.assets.support.logger_spied import SpiedLogger
from tests.assets.support.metrics_spied import SpiedMetrics
from tests.assets.support.state_store_spied import SpiedStateStore

from .directives import Directive
from .plan import ScenarioPlan
from .runner import ScenarioRunner
from .verify import ScenarioVerifier


async def run_gru_scenario(
    gru: Gru,
    logger: SpiedLogger,
    metrics: SpiedMetrics,
    state_store: SpiedStateStore,
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
