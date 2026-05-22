from minions._internal._framework.logger import Logger

from .mixin_spy import SpyMixin


class SpiedLogger(SpyMixin, Logger):
    pass
