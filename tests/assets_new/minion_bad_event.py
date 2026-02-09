from tests.assets.support.minion_spied import SpiedMinion


class UnserializableEvent:
    pass


class BadEventMinion(SpiedMinion[UnserializableEvent, dict]):
    name = "bad-event-minion"


minion = BadEventMinion
