from minions._internal._domain.minion import Minion
from minions._internal._domain.types import T_Ctx, T_Event

from .mixin_spy import SpyMixin


class SpiedMinion(SpyMixin, Minion[T_Event, T_Ctx], defer_minion_setup=True):
    pass
