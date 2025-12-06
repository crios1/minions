from typing import Optional, Union, Any

from minions._internal._utils.get_class import get_class

class Dummy:
    pass

def test_direct_class():
    assert get_class(Dummy) is Dummy

def test_optional_class():
    assert get_class(Optional[Dummy]) is Dummy

def test_union_class_first():
    assert get_class(Union[Dummy, None]) is Dummy

def test_union_class_second():
    assert get_class(Union[None, Dummy]) is Dummy

def test_pep604_union():
    assert get_class(Dummy | None) is Dummy
    assert get_class(None | Dummy) is Dummy

def test_union_of_nones():
    assert get_class(Union[None, type(None)]) is None
    assert get_class(Union[type(None), type(None)]) is None

def test_any():
    assert get_class(Any) is Any
