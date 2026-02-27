import pytest

from minions._internal._domain.exceptions import UnsupportedUserCode
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource


def test_spied_minion_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[dict, dict]):
            _mn_bad_cls_atrr = 1


def test_spied_minion_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[dict, dict]):
            async def _mn_bad_method(self):
                ...


def test_spied_pipeline_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[dict]):
            _mn_bad_cls_atrr = 1


def test_spied_pipeline_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[dict]):
            async def _mn_bad_method(self):
                ...


def test_spied_resource_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):
            _mn_bad_cls_atrr = 1


def test_spied_resource_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):
            async def _mn_bad_method(self):
                ...
