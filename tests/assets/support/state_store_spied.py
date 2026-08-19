from minions._internal._framework.state_store import StateStore

from .component_spy_meta import ComponentSpyMeta


class SpiedStateStore(StateStore, metaclass=ComponentSpyMeta):
    pass
