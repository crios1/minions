from typing import Any

from minions._internal._framework.logger import Logger

from .mixin_spy import SpyMixin


class SpiedLogger(SpyMixin, Logger):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
