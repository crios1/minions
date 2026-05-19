from typing import Annotated, Any, Literal, Optional, Union

from minions._internal._utils.get_type_from_hint import get_type_from_hint


class Dummy:
    pass


def test_direct_class():
    assert get_type_from_hint(Dummy) is Dummy


def test_optional_class():
    assert get_type_from_hint(Optional[Dummy]) is Dummy


def test_union_class_first():
    assert get_type_from_hint(Union[Dummy, None]) is Dummy


def test_union_class_second():
    assert get_type_from_hint(Union[None, Dummy]) is Dummy


def test_pep604_union():
    assert get_type_from_hint(Dummy | None) is Dummy
    assert get_type_from_hint(None | Dummy) is Dummy


def test_union_of_nones():
    assert get_type_from_hint(Union[None, type(None)]) is None
    assert get_type_from_hint(Union[type(None), type(None)]) is None


def test_typing_constructs_return_none():
    assert get_type_from_hint(Any) is None
    assert get_type_from_hint(list[int]) is None
    assert get_type_from_hint(Literal["x"]) is None
    assert get_type_from_hint(Annotated[Dummy, "metadata"]) is None
