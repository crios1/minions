from __future__ import annotations

import collections.abc
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple, TypedDict, Union

import msgspec
import pytest

from minions._internal._utils.serialization import (
    SERIALIZABLE_PRIMITIVE_TYPES,
    deserialize,
    is_type_serializable,
    require_user_declared_type,
    serialize,
    type_checks,
)


@dataclass
class MyDC:
    x: int
    y: str = "ok"


class MyTD(TypedDict):
    a: int
    b: str


class MyStruct(msgspec.Struct):
    x: int
    y: str = "ok"


@dataclass
class _BadDC:
    lock: threading.Lock


@dataclass
class _OKDC:
    a: int
    b: list[str]
    c: dict[str, float]


class _BadTD(TypedDict):
    x: set[int]


class _OKTD(TypedDict):
    x: int
    y: list[str]


@dataclass
class _DCWithOptional:
    x: Optional[int]


@dataclass
class _DCWithPEP604Optional:
    x: int | None


class _TDWithOptional(TypedDict, total=False):
    x: Optional[int]


@dataclass
class _DCWithTuple:
    t: Tuple[int, str]


def test_is_type_serializable_accepts_basic_supported_shapes():
    assert is_type_serializable(dict)
    assert is_type_serializable(MyTD)
    assert is_type_serializable(MyDC)
    assert is_type_serializable(MyStruct)
    assert is_type_serializable(list)
    assert is_type_serializable(int)


@pytest.mark.parametrize("primitive_type", SERIALIZABLE_PRIMITIVE_TYPES)
def test_serializable_primitive_types_are_serializable(primitive_type: object) -> None:
    assert is_type_serializable(primitive_type)


def test_private_require_type_serializable_raises_supported_types_message():
    with pytest.raises(
        TypeError,
        match=(
            r"MyOwner: event type is not serializable\. "
            r"Supported types:"
        ),
    ):
        type_checks._require_type_serializable(set[int], owner="MyOwner", type_label="event type")


def test_private_require_type_serializable_accepts_serializable_type():
    type_checks._require_type_serializable(MyDC, owner="MyOwner", type_label="event type")


@pytest.mark.parametrize("primitive_type", SERIALIZABLE_PRIMITIVE_TYPES)
def test_require_user_declared_type_rejects_primitive_contracts(
    primitive_type: object,
) -> None:
    with pytest.raises(
        TypeError,
        match=(
            r"MyOwner: event type is not supported\. "
            r"Supported user-declared types: "
            r"\(dataclass, msgspec\.Struct\)\."
        )
    ):
        require_user_declared_type(primitive_type, owner="MyOwner", type_label="event")


def test_require_user_declared_type_rejects_mapping_contracts():
    with pytest.raises(
        TypeError,
        match=r"MyOwner: event type is not supported\. Supported user-declared types:"
    ):
        require_user_declared_type(dict[str, int], owner="MyOwner", type_label="event")


def test_require_user_declared_type_rejects_typed_dict_contracts():
    with pytest.raises(TypeError):
        require_user_declared_type(MyTD, owner="MyOwner", type_label="event")


def test_require_user_declared_type_accepts_dataclass_and_msgspec_struct():
    require_user_declared_type(MyDC, owner="MyOwner", type_label="event")
    require_user_declared_type(MyStruct, owner="MyOwner", type_label="event")


def test_require_user_declared_type_rejects_any_with_contract_message():
    with pytest.raises(
        TypeError,
        match=r"MyOwner: event type is not supported\. Supported user-declared types:",
    ):
        require_user_declared_type(Any, owner="MyOwner", type_label="event")


def test_require_user_declared_type_rejects_plain_class_with_contract_message():
    class PlainEvent:
        pass

    with pytest.raises(
        TypeError,
        match=r"MyOwner: event type is not supported\. Supported user-declared types:",
    ):
        require_user_declared_type(
            PlainEvent,
            owner="MyOwner",
            type_label="event",
        )


def test_require_user_declared_type_checks_declared_type_fields_are_serializable():
    with pytest.raises(
        TypeError,
        match=r"MyOwner: event type is not serializable\. Supported types:",
    ):
        require_user_declared_type(_BadDC, owner="MyOwner", type_label="event")


def test_require_user_declared_type_labels_workflow_context_errors():
    with pytest.raises(
        TypeError,
        match=(
            r"MyOwner: workflow context type is not supported\. "
            r"Supported user-declared types:"
        ),
    ):
        require_user_declared_type(
            Any,
            owner="MyOwner",
            type_label="workflow context",
        )


def test_require_user_declared_type_labels_config_errors():
    with pytest.raises(
        TypeError,
        match=r"MyOwner: config type is not supported\. Supported user-declared types:",
    ):
        require_user_declared_type(Any, owner="MyOwner", type_label="config")


def test_require_user_declared_type_labels_inline_minion_config_errors():
    with pytest.raises(
        TypeError,
        match=(
            r"MyOwner: minion_config type is not supported\. "
            r"Supported user-declared types:"
        ),
    ):
        require_user_declared_type(Any, owner="MyOwner", type_label="minion_config")


def test_is_type_serializable_enforces_dict_key_and_value_recursion():
    assert is_type_serializable(Dict[str, int])
    assert not is_type_serializable(Dict[int, str])
    assert is_type_serializable(Dict[str, Dict[str, int]])
    assert not is_type_serializable(Dict[str, Dict[int, int]])


def test_is_type_serializable_recurses_dataclass_and_typed_dict_fields():
    assert not is_type_serializable(_BadDC)
    assert is_type_serializable(_OKDC)
    assert not is_type_serializable(_BadTD)
    assert is_type_serializable(_OKTD)


def test_is_type_serializable_handles_optional_and_union_types():
    assert is_type_serializable(_DCWithOptional) is True
    assert is_type_serializable(_DCWithPEP604Optional) is True
    assert is_type_serializable(int | str) is True
    assert is_type_serializable(int | set[int]) is False
    assert is_type_serializable(_TDWithOptional) is True


def test_is_type_serializable_handles_bytes_and_unions():
    assert is_type_serializable(bytes)
    assert is_type_serializable(Union[int, str])
    assert is_type_serializable(Union[int, None])
    assert not is_type_serializable(Union[int, set[int]])
    assert is_type_serializable(Union[Dict[str, int], list[int]])


def test_fixed_length_tuple_as_field_supported_by_checker():
    assert is_type_serializable(_DCWithTuple) is True


def test_dataclass_default_factory_mismatch_and_serialization():
    @dataclass
    class DC1:
        x: list[str] = field(default_factory=lambda: {1, 2, 3})  # type: ignore[assignment]

    assert is_type_serializable(DC1) is True

    try:
        blob = serialize(DC1())
    except TypeError:
        pass
    else:
        try:
            out = deserialize(blob, type_=DC1)
        except ValueError:
            pass
        else:
            assert isinstance(out.x, list)
            assert isinstance(DC1().x, set)


def test_dataclass_default_factory_any_annotation_behaviour():
    @dataclass
    class DC2:
        x: Any = field(default_factory=lambda: object())

    assert is_type_serializable(DC2) is False
    with pytest.raises(TypeError):
        serialize(DC2())


def test_dataclass_default_factory_any_annotation_but_serializable_instance():
    @dataclass
    class DC3:
        x: Any = field(default_factory=lambda: [1, 2, 3])

    assert is_type_serializable(DC3) is False
    serialize(DC3())


@pytest.mark.parametrize(
    "tp",
    [
        bytes,
        list,
        tuple,
        dict,
        list[int],
        Tuple[int, ...],
        Tuple[int, str],
        Dict[str, int],
        dict[str, int],
        Dict[str, Dict[str, int]],
        Mapping[str, int],
        Mapping[str, list[int]],
        collections.abc.Mapping[str, int],
        MyDC,
        MyTD,
        MyStruct,
    ],
)
def test_serializable_type_annotations_are_accepted(tp: object) -> None:
    assert type_checks.is_type_serializable(tp) is True


@pytest.mark.parametrize(
    "tp",
    [
        Any,
        set[int],
        Dict[int, int],
        dict[int, int],
        Dict[str, Dict[int, int]],
        Mapping,
        Mapping[int, int],
        Mapping[str, set[int]],
        collections.abc.Mapping,
        collections.abc.Mapping[int, int],
        _BadDC,
        _BadTD,
    ],
)
def test_unserializable_type_annotations_are_rejected(tp: object) -> None:
    assert type_checks.is_type_serializable(tp) is False


def test_mapping_value_type_helper_handles_key_policy_edge_cases():
    assert type_checks._mapping_value_type_if_str_key(dict, ()) == (True, None)
    assert type_checks._mapping_value_type_if_str_key(Mapping, ()) == (False, None)
    assert type_checks._mapping_value_type_if_str_key(dict, (int,)) == (False, None)
    assert type_checks._mapping_value_type_if_str_key(Mapping, (int,)) == (True, None)
    assert type_checks._mapping_value_type_if_str_key(dict, (int, int)) == (False, None)
    assert type_checks._mapping_value_type_if_str_key(Mapping, (int, int)) == (False, None)

    ok, v = type_checks._mapping_value_type_if_str_key(dict, (str, int))
    assert ok is True and v is int

    ok, v = type_checks._mapping_value_type_if_str_key(Mapping, (str, int))
    assert ok is True and v is int
