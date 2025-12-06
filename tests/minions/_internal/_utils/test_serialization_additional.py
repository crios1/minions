from dataclasses import dataclass
from typing import TypedDict, Dict, Tuple

import pytest

from minions._internal._utils import serialization as s


def test_bare_list_and_tuple_allowed():
    assert s._is_json_jsonable_field_type(list) is True
    assert s._is_json_jsonable_field_type(tuple) is True


def test_tuple_ellipsis_and_parametrized_tuple():
    from typing import Tuple as TypingTuple

    assert s._is_json_jsonable_field_type(TypingTuple[int, ...]) is True
    assert s._is_json_jsonable_field_type(TypingTuple[int, str]) is True


def test_dataclass_fields_handled():
    @dataclass
    class DC:
        a: int
        b: str

    # should process dataclass fields
    assert s._is_json_jsonable_field_type(DC) is True


def test_typed_dict_handled():
    class TD(TypedDict):
        x: int
        y: str

    assert s._is_json_jsonable_field_type(TD) is True


def test_mapping_with_non_str_key_returns_false():
    from typing import Dict as TypingDict

    assert s._is_json_jsonable_field_type(TypingDict[int, int]) is False
    # str keys allowed
    assert s._is_json_jsonable_field_type(TypingDict[str, int]) is True


def test_is_type_serializable_bytes_and_dict_cases():
    assert s.is_type_serializable(bytes) is True
    assert s.is_type_serializable(Dict[str, int]) is True
    assert s.is_type_serializable(Dict[int, int]) is False


def test_tuple_empty_and_param_variants():
    from typing import Tuple as TypingTuple

    # bare tuple allowed
    assert s.is_type_serializable(tuple) is True
    # parametrized with ellipsis
    assert s.is_type_serializable(TypingTuple[int, ...]) is True
    # fixed-length tuple
    assert s.is_type_serializable(TypingTuple[int, str]) is True


def test_mapping_and_dict_key_checks():
    from typing import Mapping as TypingMapping, Dict as TypingDict

    # Mapping with str keys should be allowed
    assert s.is_type_serializable(TypingMapping[str, int]) is True
    # Mapping with non-str keys: non-dict mappings are allowed as best-effort
    assert s.is_type_serializable(TypingMapping[int, int]) is True

    # dict[...] branches exercised
    assert s._is_json_jsonable_field_type(dict) is True
    assert s._is_json_jsonable_field_type(TypingDict[str, int]) is True
    assert s._is_json_jsonable_field_type(TypingDict[int, int]) is False


def test_bytes_leaf_and_is_json_jsonable_field_type_dict_branches():
    # bytes leaf in field checker
    assert s._is_json_jsonable_field_type(bytes) is True


def test_internal_branches_with_monkeypatch(monkeypatch):
    # Use a sentinel object to drive get_origin/get_args to force branches
    class Sentinel:
        pass

    # Case 1: list with two args -> should return False at the len(args)!=1 branch
    monkeypatch.setattr(s, "get_origin", lambda tp: list if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, str) if tp is Sentinel else s.get_args(tp))
    assert s._is_json_jsonable_field_type(Sentinel) is False

    # Case 2: dict origin with len(args)!=2 -> should continue/return True in mapping handling
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is Sentinel else s.get_args(tp))
    # mapping branch treats len(args)!=2 as "continue" and ultimately returns True
    assert s._is_json_jsonable_field_type(Sentinel) is True

    # Case 3: mapping origin dict with k not str in is_type_serializable mapping branch
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is Sentinel else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is Sentinel else s.get_args(tp))
    assert s.is_type_serializable(Sentinel) is False


def test_mapping_dict_args_len_not_two_and_exception_branch(monkeypatch):
    # Force a dict-origin path where get_args returns more than 2 items
    class Sentinel2:
        pass

    # len(args) != 2 should return False in _is_json_jsonable_field_type
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is Sentinel2 else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int, int) if tp is Sentinel2 else s.get_args(tp))
    assert s._is_json_jsonable_field_type(Sentinel2) is False


def test_is_type_serializable_mapping_len_not_two_returns_true(monkeypatch):
    # In is_type_serializable mapping branch, len(args) != 2 returns True
    class Sentinel3:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is Sentinel3 else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is Sentinel3 else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is Sentinel3 else s.get_args(tp))

    assert s.is_type_serializable(Sentinel3) is True


def test__is_json_mapping_origin_dict_len2_k_str(monkeypatch):
    class SentinelA:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelA else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelA else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelA else s.get_args(tp))

    assert s._is_json_jsonable_field_type(SentinelA) is True


def test__is_json_mapping_origin_dict_len2_k_not_str(monkeypatch):
    class SentinelB:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelB else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelB else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelB else s.get_args(tp))

    assert s._is_json_jsonable_field_type(SentinelB) is False


def test__is_json_mapping_exception_passes(monkeypatch):
    class SentinelC:
        pass

    def raise_exc(tp):
        if tp is SentinelC:
            raise RuntimeError("boom")
        return s._is_mapping_type(tp)

    monkeypatch.setattr(s, "_is_mapping_type", raise_exc)
    assert s._is_json_jsonable_field_type(SentinelC) is False


def test__is_json_mapping_sequential_calls(monkeypatch):
    """
    Ensure the mapping-subclass branch in _is_json_jsonable_field_type
    is exercised when get_origin returns a non-dict first and dict second.
    This forces the later mapping block to run (two calls to get_origin).
    """
    class SentinelE:
        pass

    # make _is_mapping_type return True for our sentinel so the mapping block runs
    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelE else s._is_mapping_type(tp))

    # track number of calls to get_origin/get_args
    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelE:
            # first call: avoid matching the earlier dict branch
            if state["calls"] == 0:
                state["calls"] += 1
                return None
            # second call: simulate a dict-origin inside the mapping-block
            return dict
        return s.get_origin(tp)

    def get_args_seq(tp):
        if tp is SentinelE:
            # when mapping block runs we want a k,v pair where k is not str
            return (int, int)
        return s.get_args(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(s, "get_args", get_args_seq)

    # k is not str -> should return False
    assert s._is_json_jsonable_field_type(SentinelE) is False


def test_is_type_serializable_mapping_sequential_calls(monkeypatch):
    """
    Force the mapping branch inside is_type_serializable to exercise
    the len(args) != 2 and k-not-str branches using controlled
    get_origin/get_args behavior.
    """
    class SentinelF:
        pass

    # make it look like a mapping type
    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelF else s._is_mapping_type(tp))

    # First scenario: len(args) != 2 -> should return True when treated as Mapping
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is SentinelF else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelF else s.get_args(tp))
    assert s.is_type_serializable(SentinelF) is True

    # Second scenario: len(args) == 2 but k is not str -> should return False
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelF else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelF else s.get_args(tp))
    assert s.is_type_serializable(SentinelF) is False


def test_is_type_serializable_mapping_dict_len_not_two_dict_origin(monkeypatch):
    class SentinelG:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelG else s._is_mapping_type(tp))
    # Make get_origin return Mapping on the first/top-level call so the
    # earlier `if origin is dict or tp is dict` branch is not taken, then
    # return dict when called from inside the mapping-handling block.
    state = {"calls": 0}

    def get_origin_seq(tp):
        if tp is SentinelG:
            # return Mapping for the first two calls so the earlier "dict"
            # branch in is_type_serializable is not taken; return dict on
            # the third call when inside the mapping-handling block.
            state["calls"] += 1
            if state["calls"] < 3:
                return s.Mapping
            return dict
        return s.get_origin(tp)

    monkeypatch.setattr(s, "get_origin", get_origin_seq)
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelG else s.get_args(tp))

    # dict origin with len(args) != 2 inside the mapping handling should return True
    assert s.is_type_serializable(SentinelG) is True


def test_is_type_serializable_mapping_dict_k_str_calls_field_checker(monkeypatch):
    class SentinelH:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelH else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelH else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelH else s.get_args(tp))

    # dict origin with k == str should delegate to _is_json_jsonable_field_type on v
    assert s.is_type_serializable(SentinelH) is True


def test__is_json_mapping_origin_dict_len2_k_str_appends_and_continues(monkeypatch):
    class SentinelI:
        pass

    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelI else s._is_mapping_type(tp))
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelI else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (str, int) if tp is SentinelI else s.get_args(tp))

    # should append v (int) to the stack and ultimately return True
    assert s._is_json_jsonable_field_type(SentinelI) is True


def test_is_type_serializable_bytes_hits_bytes_branch(monkeypatch):
    # Force _is_json_leaf_type to return False so code reaches the `tp is bytes` check
    real_leaf = s._is_json_leaf_type

    monkeypatch.setattr(s, "_is_json_leaf_type", lambda tp: False if tp is bytes else real_leaf(tp))
    try:
        assert s.is_type_serializable(bytes) is True
    finally:
        monkeypatch.setattr(s, "_is_json_leaf_type", real_leaf)


def test_is_type_serializable_mapping_dict_len_not_two_true_and_k_not_str(monkeypatch):
    class SentinelD:
        pass

    # len(args) != 2 -> should return True in the mapping branch
    monkeypatch.setattr(s, "_is_mapping_type", lambda tp: True if tp is SentinelD else s._is_mapping_type(tp))
    # Use Mapping origin so is_type_serializable treats it as "other mapping" and returns True
    monkeypatch.setattr(s, "get_origin", lambda tp: s.Mapping if tp is SentinelD else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int,) if tp is SentinelD else s.get_args(tp))
    assert s.is_type_serializable(SentinelD) is True

    # len(args) == 2 but k not str should return False when origin is dict
    monkeypatch.setattr(s, "get_origin", lambda tp: dict if tp is SentinelD else s.get_origin(tp))
    monkeypatch.setattr(s, "get_args", lambda tp: (int, int) if tp is SentinelD else s.get_args(tp))
    assert s.is_type_serializable(SentinelD) is False


def test_direct_dict_branches_with_builtin_generics():
    # Use PEP 585 generics (dict[...]) to exercise the 'origin is dict' branches
    assert s._is_json_jsonable_field_type(dict[str, int]) is True
    assert s._is_json_jsonable_field_type(dict[int, int]) is False

    assert s.is_type_serializable(dict[str, int]) is True
    assert s.is_type_serializable(dict[int, int]) is False


def test_mapping_helper_and_edge_cases_direct():
    # exercise the helper behavior for dict origin and mapping-like shapes
    # empty args -> treated as acceptable
    class SEmpty:
        pass

    # simulate dict with no args by directly calling with a typing object
    assert s._mapping_value_type_if_str_key(dict, ()) == (True, None)

    # len(args) != 2 for dict -> mapping helper returns (True, None)
    assert s._mapping_value_type_if_str_key(dict, (int,)) == (True, None)

    # non-str key -> should return (False, None)
    assert s._mapping_value_type_if_str_key(dict, (int, int)) == (False, None)

    # str key -> returns (True, <value-type>)
    ok, v = s._mapping_value_type_if_str_key(dict, (str, int))
    assert ok is True and v is int


def test_is_json_jsonable_field_type_mapping_subclass_append(monkeypatch):
    # Use a mapping-subclass scenario where the mapping-block appends the value type
    class SentinelZ:
        pass

    # Make _is_mapping_type True so the mapping-handling block runs
    monkeypatch.setattr(s, '_is_mapping_type', lambda tp: True if tp is SentinelZ else s._is_mapping_type(tp))

    # Sequence get_origin so the first call doesn't return dict (avoids earlier dict branch),
    # and a later call inside the mapping-block returns dict to exercise append/continue.
    state = {'calls': 0}

    def get_origin_seq(tp):
        if tp is SentinelZ:
            if state['calls'] == 0:
                state['calls'] += 1
                return None
            return dict
        return s.get_origin(tp)

    def get_args_seq(tp):
        if tp is SentinelZ:
            # provide a (k, v) pair where k is str so v will be appended
            return (str, int)
        return s.get_args(tp)

    monkeypatch.setattr(s, 'get_origin', get_origin_seq)
    monkeypatch.setattr(s, 'get_args', get_args_seq)

    assert s._is_json_jsonable_field_type(SentinelZ) is True


def test_is_type_serializable_mapping_block_k_not_str_and_k_str(monkeypatch):
    """Exercise the mapping-handling block inside is_type_serializable
    so the k,v assignment and subsequent branches are covered.
    """
    class SentinelY:
        pass

    # Make it look like a mapping type so the mapping block runs
    monkeypatch.setattr(s, '_is_mapping_type', lambda tp: True if tp is SentinelY else s._is_mapping_type(tp))

    # First scenario: inside mapping block origin becomes dict and k is not str
    state = {'calls': 0}

    # capture originals so our sequenced functions can call through
    orig_get_origin = s.get_origin
    orig_get_args = s.get_args

    def get_origin_seq(tp):
        if tp is SentinelY:
            # first/top-level origin: not dict so earlier dict branch isn't taken
            if state['calls'] == 0:
                state['calls'] += 1
                return s.Mapping
            # second call: inside mapping handling, return dict
            return dict
        return orig_get_origin(tp)

    # len==2 and k not str
    monkeypatch.setattr(s, 'get_origin', get_origin_seq)
    monkeypatch.setattr(s, 'get_args', lambda tp: (int, int) if tp is SentinelY else orig_get_args(tp))
    assert s.is_type_serializable(SentinelY) is False

    # Second scenario: k is str and v is a non-leaf type (list[int]) so
    # _is_json_jsonable_field_type(v) must be called; reuse the same sequencing.
    state['calls'] = 0
    monkeypatch.setattr(s, 'get_origin', get_origin_seq)
    monkeypatch.setattr(s, 'get_args', lambda tp: (str, list[int]) if tp is SentinelY else orig_get_args(tp))
    assert s.is_type_serializable(SentinelY) is True


def test_is_type_serializable_dict_origin_direct_k_not_str_and_k_str(monkeypatch):
    """Directly force the `origin is dict` branch in is_type_serializable
    and exercise the k-not-str and k-str sub-branches using captured
    originals to avoid recursion.
    """
    class SentinelK:
        pass

    # Ensure mapping-detection returns True so mapping handling runs
    monkeypatch.setattr(s, '_is_mapping_type', lambda tp: True if tp is SentinelK else s._is_mapping_type(tp))

    # Capture originals
    orig_get_origin = s.get_origin
    orig_get_args = s.get_args

    # Sequence get_origin to return dict when called inside mapping block
    state = {'calls': 0}

    def get_origin_seq(tp):
        if tp is SentinelK:
            # first/top-level call: return Mapping to avoid earlier dict branch
            if state['calls'] == 0:
                state['calls'] += 1
                return s.Mapping
            # subsequent call inside mapping block: return dict
            return dict
        return orig_get_origin(tp)

    # First: k not str -> expect False
    monkeypatch.setattr(s, 'get_origin', get_origin_seq)
    monkeypatch.setattr(s, 'get_args', lambda tp: (int, int) if tp is SentinelK else orig_get_args(tp))
    assert s.is_type_serializable(SentinelK) is False

    # Second: k is str and v is a simple jsonable type -> expect True
    state['calls'] = 0
    monkeypatch.setattr(s, 'get_origin', get_origin_seq)
    monkeypatch.setattr(s, 'get_args', lambda tp: (str, int) if tp is SentinelK else orig_get_args(tp))
    assert s.is_type_serializable(SentinelK) is True
