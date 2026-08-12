import asyncio
import tomllib
from dataclasses import dataclass
from pathlib import Path

from minions import Minion, Resource, minion_id, minion_step, pipeline_id, resource_id
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.pipeline_triggered import TriggeredPipeline

from . import MINION_COMPONENT_ID, PIPELINE_COMPONENT_ID, RESOURCE_COMPONENT_ID


@dataclass
class MarkerConfig:
    marker: str


@dataclass
class MarkerContext:
    config_marker: str | None = None


@resource_id(RESOURCE_COMPONENT_ID)
class MarkerRecordingResource(Resource):
    async def startup(self) -> None:
        self.recording_started = asyncio.Event()
        self.allow_recording = asyncio.Event()
        self.recorded_markers: list[str | None] = []

    async def record_marker(self, marker: str | None) -> None:
        self.recording_started.set()
        await self.allow_recording.wait()
        self.recorded_markers.append(marker)


@pipeline_id(PIPELINE_COMPONENT_ID)
class ControlledPipeline(TriggeredPipeline[SimpleEvent]):
    async def produce_event(self) -> SimpleEvent:
        return SimpleEvent(timestamp=0)


@minion_id(MINION_COMPONENT_ID)
class MarkerTransferMinion(Minion[SimpleEvent, MarkerContext]):
    config: MarkerConfig
    resource: MarkerRecordingResource

    async def load_config(self, config_path: str) -> MarkerConfig:
        contents = await asyncio.to_thread(Path(config_path).read_text)
        parsed = tomllib.loads(contents)
        return MarkerConfig(marker=parsed["config"]["marker"])

    @minion_step
    async def capture_config_marker(self) -> None:
        self.context.config_marker = self.config.marker

    @minion_step
    async def record_context_marker(self) -> None:
        await self.resource.record_marker(self.context.config_marker)
