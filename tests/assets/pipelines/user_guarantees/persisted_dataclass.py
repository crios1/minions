from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)
from tests.assets.user_guarantees.persisted_shapes import DataclassEvent


class DataclassPersistenceGuaranteePipeline(
    SubscriberReadyFixedEventsPipeline[DataclassEvent],
):
    async def produce_event(self) -> DataclassEvent:
        return DataclassEvent(kind="dataclass-event", payload_value=10)


pipeline = DataclassPersistenceGuaranteePipeline
