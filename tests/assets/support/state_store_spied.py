from minions._internal._framework.state_store import StateStore

from .mixin_spy import SpyMixin


class SpiedStateStore(SpyMixin, StateStore):
    pass
