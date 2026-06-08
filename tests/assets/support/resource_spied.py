from minions._internal._domain.resource import Resource

from .mixin_spy import SpyMixin


class SpiedResource(SpyMixin, Resource):
    pass
