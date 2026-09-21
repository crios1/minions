from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.state_store import StateStore

from .component_spy_meta import ComponentSpyMeta


class SpiedStateStore(StateStore, metaclass=ComponentSpyMeta):
    def __init__(self, logger: Logger, metrics: Metrics):
        super().__init__(logger, metrics)
