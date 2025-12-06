import pytest

from minions import Resource

from minions._internal._domain.exceptions import UnsupportedUserCode

class TestMinionSubclassingInvalid:

    def test_invalid_class_var_name(self):
        with pytest.raises(UnsupportedUserCode):
            class MyResource(Resource):
                _mn_cls_var = 0

    def test_invalid_method_name(self):
        with pytest.raises(UnsupportedUserCode):
            class MyResource(Resource):
                def _mn_method(self):
                    ...