from minions import Pipeline

from tests.assets.events.record import RecordEvent


class Pipeline1(Pipeline[RecordEvent]):
    ...


class Pipeline2(Pipeline[RecordEvent]):
    ...
