from tests.assets.support.minion_spied import SpiedMinion
from .event_counter import CounterEvent


class BadContextMinion(SpiedMinion[CounterEvent, set[int]]):
    name = "bad-context-minion"


minion = BadContextMinion
