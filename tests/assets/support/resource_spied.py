from minions._internal._domain.resource import Resource

from .component_spy_meta import ComponentSpyMeta


class SpiedResource(Resource, metaclass=ComponentSpyMeta):
    pass
