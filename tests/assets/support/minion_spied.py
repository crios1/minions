from minions._internal._domain.minion import Minion
from minions._internal._domain.types import T_Ctx, T_Event

from .component_spy_meta import ComponentSpyMeta


class SpiedMinion(
    Minion[T_Event, T_Ctx],
    defer_minion_setup=True,
    metaclass=ComponentSpyMeta,
):
    pass
