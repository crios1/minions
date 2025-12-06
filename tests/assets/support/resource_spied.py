from minions._internal._domain.resource import Resource
from .mixin_spy import SpyMixin

class SpiedResource(Resource, SpyMixin):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)