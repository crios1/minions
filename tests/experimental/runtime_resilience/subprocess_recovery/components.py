"""Trusted components used by the subprocess recovery verification campaign."""

import asyncio
import os
from pathlib import Path

from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


def _artifact_path(name: str) -> Path:
    artifact_dir = Path(os.environ["MINIONS_CRASH_ARTIFACT_DIR"])
    return artifact_dir / name


def _record(name: str, value: str) -> None:
    path = _artifact_path(name)
    with path.open("a", encoding="utf-8") as artifact:
        artifact.write(f"{value}\n")
        artifact.flush()
        os.fsync(artifact.fileno())


class CrashCheckpointPipeline(
    SubscriberReadyFixedEventsPipeline[CounterEvent],
):
    async def produce_event(self) -> CounterEvent:
        if os.environ["MINIONS_CRASH_ROLE"] == "recovery":
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        return CounterEvent(seq=7)


class CrashCheckpointMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        _record("step_1.log", os.environ["MINIONS_CRASH_ROLE"])
        if (
            os.environ["MINIONS_CRASH_SCENARIO"]
            in {
                "during_step",
                "during_orchestration_stop",
                "during_gru_shutdown",
                "graceful_sigterm",
                "truncated_payload",
                "incompatible_payload",
            }
            and os.environ["MINIONS_CRASH_ROLE"] == "initial"
        ):
            if os.environ["MINIONS_CRASH_SCENARIO"] in {
                "during_step",
                "graceful_sigterm",
                "truncated_payload",
                "incompatible_payload",
            }:
                _artifact_path("crash_ready").touch()
            await asyncio.Event().wait()
        self.context.seq = self.event.seq

    @minion_step
    async def step_2(self) -> None:
        _record("step_2.log", os.environ["MINIONS_CRASH_ROLE"])
        self.context.handled = True

    async def shutdown(self) -> None:
        if (
            os.environ["MINIONS_CRASH_SCENARIO"]
            in {"during_orchestration_stop", "during_gru_shutdown"}
            and os.environ["MINIONS_CRASH_ROLE"] == "initial"
        ):
            _artifact_path("crash_ready").touch()
            await asyncio.Event().wait()


pipeline = CrashCheckpointPipeline
minion = CrashCheckpointMinion
