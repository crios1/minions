from typing import Any

from minions._internal._framework.state_store import StateStore

from .mixin_spy import SpyMixin


class SpiedStateStore(SpyMixin, StateStore):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
