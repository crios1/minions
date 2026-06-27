from minions._internal._domain.gru import Gru

from .directives import Directive
from .plan import PipelineEventCountKey, ScenarioPlan
from .runner import ScenarioRunner
from .verify import ScenarioVerifier


async def run_gru_scenario(
    gru: Gru,
    directives: list[Directive],
    *,
    pipeline_event_counts: dict[PipelineEventCountKey, int],
    per_verification_timeout: float = 5.0,
) -> None:
    """Run and verify a scenario against a pre-wired Gru.

    The supplied Gru must use InMemoryLogger, InMemoryMetrics, and
    InMemoryStateStore; ScenarioRunner validates that contract before execution.
    """
    plan = ScenarioPlan(directives, pipeline_event_counts=pipeline_event_counts)

    runner = ScenarioRunner(gru, plan, per_verification_timeout=per_verification_timeout)
    result = await runner.run()

    verifier = ScenarioVerifier(
        plan,
        result,
        logger=runner.logger,
        metrics=runner.metrics,
        state_store=runner.state_store,
        per_verification_timeout=per_verification_timeout,
    )
    await verifier.verify()
