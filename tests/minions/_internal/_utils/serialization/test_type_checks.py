from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, TypedDict, Union

import pytest

from minions._internal._utils.serialization import (
    deserialize,
    is_type_serializable,
    serialize,
)
from minions._internal._utils.serialization import type_checks as s


@dataclass
class MyDC:
    x: int
    y: str = "ok"


class MyTD(TypedDict):
    a: int
    b: str


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
class _HasOptional:
    x: Optional[int]


class _TDWithOptional(TypedDict, total=False):
    x: Optional[int]


@dataclass
class _DCWithTuple:
    t: Tuple[int, str]


def test_is_type_serializable():
    assert is_type_serializable(dict)
    assert is_type_serializable(MyTD)
    assert is_type_serializable(MyDC)
    assert is_type_serializable(list)
    assert is_type_serializable(int)


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


def test_optional_and_union_supported_by_type_policy():
    assert is_type_serializable(_HasOptional) is True
    assert is_type_serializable(_TDWithOptional) is True


def test_bytes_and_union_handling():
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


def test_bare_list_and_tuple_allowed():
    assert s._is_serializable_field_type(list) is True
    assert s._is_serializable_field_type(tuple) is True


def test_tuple_ellipsis_and_parametrized_tuple():
    assert s._is_serializable_field_type(Tuple[int, ...]) is True
    assert s._is_serializable_field_type(Tuple[int, str]) is True


def test_dataclass_fields_handled():
    @dataclass
    class DC:
        a: int
        b: str

    assert s._is_serializable_field_type(DC) is True


def test_typed_dict_handled():
    class TD(TypedDict):
        x: int
        y: str

    assert s._is_serializable_field_type(TD) is True


def test_mapping_with_non_str_key_returns_false():
    from typing import Dict as TypingDict

    assert s._is_serializable_field_type(TypingDict[int, int]) is False
    assert s._is_serializable_field_type(TypingDict[str, int]) is True


def test_is_type_serializable_bytes_and_dict_cases():
    assert s.is_type_serializable(bytes) is True
    assert s.is_type_serializable(Dict[str, int]) is True
    assert s.is_type_serializable(Dict[int, int]) is False


def test_tuple_empty_and_param_variants():
    assert s.is_type_serializable(tuple) is True
    assert s.is_type_serializable(Tuple[int, ...]) is True
    assert s.is_type_serializable(Tuple[int, str]) is True


def test_mapping_and_dict_key_checks():
    from typing import Mapping as TypingMapping, Dict as TypingDict

    assert s.is_type_serializable(TypingMapping[str, int]) is True
    assert s.is_type_serializable(TypingMapping[int, int]) is True

    assert s._is_serializable_field_type(dict) is True
    assert s._is_serializable_field_type(TypingDict[str, int]) is True
    assert s._is_serializable_field_type(TypingDict[int, int]) is False


def test_bytes_leaf_and_is_serializable_field_type_dict_branches():
    assert s._is_serializable_field_type(bytes) is True


def test_internal_branches_with_monkeypatch(monkeypatch):
    class Sentinel:
        pass

    monkeypatch.setattr(s, "get_origin", lambda tp: list if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, str) if tp is Sentinel else s.get_args(tp))
    assert s._is_serializable_field_type(Sentinel) is False

    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is Sentinel else s.get_args(tp))
    assert s._is_serializable_field_type(Sentinel) is True

    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is Sentinel else s.get_args(tp))
    assert s.is_type_serializable(Sentinel) is False


def test_mapping_dict_args_len_not_two_and_exception_branch(monkeypatch):
    class Sentinel2:
        pass

    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is Sentinel2 else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int, int) if tp is Sentinel2 else s.get_args(tp))
    assert s._is_serializable_field_type(Sentinel2) is False


def test_is_type_serializable_mapping_len_not_two_returns_true(monkeypatch):
    class Sentinel3:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is Sentinel3 else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is Sentinel3 else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is Sentinel3 else s.get_args(tp))

    assert s.is_type_serializable(Sentinel3) is True


def test_mapping_origin_dict_len2_k_str(monkeypatch):
    class SentinelA:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelA else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelA else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelA else s.get_args(tp))

    assert s._is_serializable_field_type(SentinelA) is True


def test_mapping_origin_dict_len2_k_not_str(monkeypatch):
    class SentinelB:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelB else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelB else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelB else s.get_args(tp))

    assert s._is_serializable_field_type(SentinelB) is False


def test_mapping_exception_passes(monkeypatch):
    class SentinelC:
        pass

    def raise_exc(tp):
        if tp is SentinelC:
            raise RuntimeError("boom")
        return s._is_mapping_type(tp)

    monkeypatch.setattr(s, "_is_mapping_type", raise_exc)
    assert s._is_serializable_field_type(SentinelC) is False


def test_mapping_sequential_calls(monkeypatch):
    class SentinelE:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelE else s._is_mapping_type(tp))

    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelE:
            if state["calls"] == 0:
                state["calls"] += 1
                return None
            return dict
        return s.get_origin(tp)

    def get_args_seq(tp):
        if tp is SentinelE:
            return (int, int)
        return s.get_args(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(s, "get_args", get_args_seq)

    assert s._is_serializable_field_type(SentinelE) is False


def test_is_type_serializable_mapping_sequential_calls(monkeypatch):
    class SentinelF:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelF else s._is_mapping_type(tp))

    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is SentinelF else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelF else s.get_args(tp))
    assert s.is_type_serializable(SentinelF) is True

    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelF else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelF else s.get_args(tp))
    assert s.is_type_serializable(SentinelF) is False


def test_is_type_serializable_mapping_dict_len_not_two_dict_origin(monkeypatch):
    class SentinelG:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelG else s._is_mapping_type(tp))
    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelG:
            state["calls"] += 1
            if state["calls"] < 3:
                return s.Mapping
            return dict
        return s.get_origin(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelG else s.get_args(tp))

    assert s.is_type_serializable(SentinelG) is True


def test_is_type_serializable_mapping_dict_k_str_calls_field_checker(monkeypatch):
    class SentinelH:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelH else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelH else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelH else s.get_args(tp))

    assert s.is_type_serializable(SentinelH) is True


def test_mapping_origin_dict_len2_k_str_appends_and_continues(monkeypatch):
    class SentinelI:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelI else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelI else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelI else s.get_args(tp))

    assert s._is_serializable_field_type(SentinelI) is True


def test_is_type_serializable_bytes_hits_bytes_branch(monkeypatch):
    real_leaf = s._is_serializable_leaf_type

    monkeypatch.setattr(s, "_is_serializable_leaf_type", lambda tp: False if tp is bytes else real_leaf(tp))
    try:
        assert s.is_type_serializable(bytes) is True
    finally:
        monkeypatch.setattr(s, "_is_serializable_leaf_type", real_leaf)


def test_is_type_serializable_mapping_dict_len_not_two_true_and_k_not_str(monkeypatch):
    class SentinelD:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelD else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is SentinelD else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelD else s.get_args(tp))
    assert s.is_type_serializable(SentinelD) is True

    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelD else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelD else s.get_args(tp))
    assert s.is_type_serializable(SentinelD) is False


def test_direct_dict_branches_with_builtin_generics():
    assert s._is_serializable_field_type(dict[str, int]) is True
    assert s._is_serializable_field_type(dict[int, int]) is False

    assert s.is_type_serializable(dict[str, int]) is True
    assert s.is_type_serializable(dict[int, int]) is False


def test_mapping_helper_and_edge_cases_direct():
    assert s._mapping_value_type_if_str_key(dict, ()) == (True, None)
    assert s._mapping_value_type_if_str_key(dict, (int,)) == (True, None)
    assert s._mapping_value_type_if_str_key(dict, (int, int)) == (False, None)

    ok, v = s._mapping_value_type_if_str_key(dict, (str, int))
    assert ok is True and v is int


def test_is_serializable_field_type_mapping_subclass_append(monkeypatch):
    class SentinelZ:
        pass

    monkeypatch.setattr(
        s, "_is_mapping_type", lambda tp: True if tp is SentinelZ else s._is_mapping_type(tp)
    )

    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelZ:
            if state["calls"] == 0:
                state["calls"] += 1
                return None
            return dict
        return s.get_origin(tp)

    def get_args_seq(tp):
        if tp is SentinelZ:
            return (str, int)
        return s.get_args(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(s, "get_args", get_args_seq)

    assert s._is_serializable_field_type(SentinelZ) is True


def test_is_type_serializable_mapping_block_k_not_str_and_k_str(monkeypatch):
    class SentinelY:
        pass

    monkeypatch.setattr(
        s, "_is_mapping_type", lambda tp: True if tp is SentinelY else s._is_mapping_type(tp)
    )

    state = {"calls": 0}

    orig_get_origin = s.get_origin
    orig_get_args = s.get_args

    def get_origin_seq(tp):
        if tp is SentinelY:
            if state["calls"] == 0:
                state["calls"] += 1
                return s.Mapping
            return dict
        return orig_get_origin(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(
        s, "get_args", lambda tp: (int, int) if tp is SentinelY else orig_get_args(tp)
    )
    assert s.is_type_serializable(SentinelY) is False

    state["calls"] = 0
    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(
        s, "get_args", lambda tp: (str, list[int]) if tp is SentinelY else orig_get_args(tp)
    )
    assert s.is_type_serializable(SentinelY) is True


def test_is_type_serializable_dict_origin_direct_k_not_str_and_k_str(monkeypatch):
    class SentinelK:
        pass

    monkeypatch.setattr(
        s, "_is_mapping_type", lambda tp: True if tp is SentinelK else s._is_mapping_type(tp)
    )

    orig_get_origin = s.get_origin
    orig_get_args = s.get_args

    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelK:
            if state["calls"] == 0:
                state["calls"] += 1
                return s.Mapping
            return dict
        return orig_get_origin(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(
        s, "get_args", lambda tp: (int, int) if tp is SentinelK else orig_get_args(tp)
    )
    assert s.is_type_serializable(SentinelK) is False

    state["calls"] = 0
    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(
        s, "get_args", lambda tp: (str, int) if tp is SentinelK else orig_get_args(tp)
    )
    assert s.is_type_serializable(SentinelK) is True
