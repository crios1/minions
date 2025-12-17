from minions._internal._domain.minion import Minion
from .mixin_spy import SpyMixin

from minions._internal._domain.types import T_Event, T_Ctx

class SpiedMinion(SpyMixin, Minion[T_Event, T_Ctx], defer_minion_setup=True):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
