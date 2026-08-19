from minions._internal._framework.logger import Logger

from .component_spy_meta import ComponentSpyMeta


class SpiedLogger(Logger, metaclass=ComponentSpyMeta):
    pass
