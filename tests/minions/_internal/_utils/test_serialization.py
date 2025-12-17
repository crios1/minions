from __future__ import annotations

import datetime
import pytest
import msgspec
import uuid

from dataclasses import dataclass, field
from typing import Any

from minions._internal._utils.serialization import (
    serialize,
    deserialize,
    _cached_decoder,
    is_type_serializable
)

@dataclass
class Event:
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    user_id: int | None = None
    active: bool = True
    type: str = "generic.event"
    source: str | None = None
    payload: dict[str, Any] | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

def _make_event() -> Event:
    return Event(
        id="00000000000000000000000000000000",
        ts=datetime.datetime(2023, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone.utc),
        user_id=42,
        active=False,
        type="test.event",
        source="unit-test",
        payload={"k": "v", "n": 1},
        metadata={"env": "test"},
        tags=["std", "unit"]
    )

# NOTE: test_valid_roundtrip() is done for a general Event type, 
# we need to test a valid roundtrip for all of the event types
# we support serialization/deserialization for!
# So basically test_valid_roundtrip as a template and pass in params/fixtures
# so don't have to clone code and just use o1 and o2 for variable names.
# It would be nice if there is a test/assert that gets the types that the framework
# says we support in the error message that happens on violation, 
# and assert that a tests exists and passes for it, if the type is in the error message

def test_valid_roundtrip():
    e1 = _make_event()
    e2 = deserialize(serialize(e1), type_=Event)
    assert e1 == e2

def test_invalid_serialize():
    with pytest.raises(TypeError):
        serialize(print)

def test_invalid_deserialize():
    with pytest.raises(ValueError):
        deserialize(bytes([1,2,3]), Event)
    with pytest.raises(TypeError):
        deserialize('invalid', None) # type: ignore

# --- Decoder cache & fallback -------------------------------------------------

MODULE = "minions._internal._utils.serialization"

def test_decoder_cache_used_for_hashable_types():
    orig = _cached_decoder
    orig.cache_clear()

    evt = _make_event()
    blob = serialize(evt)

    # first decode should populate the cache (one miss, zero hits)
    out1 = deserialize(blob, type_=Event)
    assert out1 == evt
    ci1 = orig.cache_info()
    assert ci1.misses == 1
    assert ci1.hits == 0

    # second decode should reuse cached decoder (one miss, one hit)
    out2 = deserialize(blob, type_=Event)
    assert out2 == evt
    ci2 = orig.cache_info()
    assert ci2.misses == 1
    assert ci2.hits == 1

def test_decoder_fallback_for_unhashable_annotation(monkeypatch):
    # ensure cache doesn't contain a prebuilt decoder
    orig = _cached_decoder
    orig.cache_clear()

    # Force the cached path to fail like an unhashable key would
    def boom(_t):
        raise TypeError("unhashable")

    monkeypatch.setattr(f"{MODULE}._cached_decoder", boom)

    evt = _make_event()
    blob = serialize(evt)
    out = deserialize(blob, type_=Event)
    assert out == evt

def test_decoder_cache_prevents_repeated_decoder_instantiation(monkeypatch):
    orig = _cached_decoder
    orig.cache_clear()
    real_Decoder = msgspec.msgpack.Decoder
    inst = {"n": 0}

    class SpyDecoder:
        def __init__(self, *a, **kw):
            inst["n"] += 1
            self._inner = real_Decoder(*a, **kw)
        def decode(self, *a, **kw):
            return self._inner.decode(*a, **kw)

    # Replace the Decoder class so that constructing a decoder increments our counter
    monkeypatch.setattr("msgspec.msgpack.Decoder", SpyDecoder)

    evt = _make_event()
    blob = serialize(evt)

    # first decode should construct one Decoder and populate the cache
    out1 = deserialize(blob, type_=Event)
    assert out1 == evt
    assert inst["n"] == 1
    ci1 = orig.cache_info()
    assert ci1.misses == 1
    assert ci1.hits == 0

    # second decode should reuse the cached Decoder and not construct another
    out2 = deserialize(blob, type_=Event)
    assert out2 == evt
    assert inst["n"] == 1
    ci2 = orig.cache_info()
    assert ci2.misses == 1
    assert ci2.hits == 1


from typing import Any, Dict, Mapping, TypedDict

@dataclass
class MyDC:
    x: int
    y: str = "ok"

class MyTD(TypedDict):
    a: int
    b: str

class MyMap(Mapping):
    def __init__(self, d): self._d = d
    def __getitem__(self, k): return self._d[k]
    def __iter__(self): return iter(self._d)
    def __len__(self): return len(self._d)

def test_is_type_serializable():
    assert is_type_serializable(dict)
    assert is_type_serializable(MyTD)
    assert is_type_serializable(MyDC)
    assert is_type_serializable(list)
    assert is_type_serializable(int)

import threading
from typing import Dict, TypedDict

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

import uuid
from typing import Dict, Tuple, Optional, TypedDict

# --- Optional/Union: document current behavior (unsupported by checker) ---
@dataclass
class _HasOptional:
    x: Optional[int]

class _TDWithOptional(TypedDict, total=False):
    x: Optional[int]

def test_optional_and_union_currently_not_supported_by_type_checker():
    assert is_type_serializable(_HasOptional) is True
    assert is_type_serializable(_TDWithOptional) is True

def test_bytes_and_union_handling():
    # bytes is considered serializable
    assert is_type_serializable(bytes)

    from typing import Union

    # simple unions where all options are serializable should be accepted
    assert is_type_serializable(Union[int, str])
    assert is_type_serializable(Union[int, None])

    # unions mixing a serializable and a non-serializable option should be rejected
    assert not is_type_serializable(Union[int, set[int]])

    # nested container unions
    assert is_type_serializable(Union[Dict[str, int], list[int]])

# --- dict recursion already tested; add fixed-length tuple type check path ---
@dataclass
class _HasTuple:
    t: Tuple[int, str]

def test_fixed_length_tuple_as_field_supported_by_checker():
    assert is_type_serializable(_HasTuple) is True


def test_dataclass_default_factory_mismatch_and_serialization():
    from dataclasses import dataclass, field

    @dataclass
    class DC1:
        # annotated as list[str] (serializable), but default factory returns a set
        x: list[str] = field(default_factory=lambda: {1, 2, 3}) # type: ignore

    # Type checker sees list[str] so the type-level check is optimistic
    assert is_type_serializable(DC1) is True

    # The actual instance contains a set which may or may not be serializable by msgspec.
    # If serialization fails we consider that acceptable. If it succeeds, the decoded
    # value will likely differ in type (e.g., become a list), demonstrating a mismatch
    # between the annotated type and the runtime default.
    try:
        blob = serialize(DC1())
    except TypeError:
        # runtime rejected the non-matching default — acceptable
        pass
    else:
        try:
            out = deserialize(blob, type_=DC1)
        except ValueError:
            # Validation failed because runtime default elements don't match annotated type
            pass
        else:
            # If decode succeeded, value types will likely differ (list vs set)
            assert isinstance(out.x, list)
            assert isinstance(DC1().x, set)


def test_dataclass_default_factory_any_annotation_behaviour():
    from dataclasses import dataclass, field

    @dataclass
    class DC2:
        # Unknown annotation (Any) -> type checker will be conservative
        x: Any = field(default_factory=lambda: object())

    assert is_type_serializable(DC2) is False
    with pytest.raises(TypeError):
        serialize(DC2())


def test_dataclass_default_factory_any_annotation_but_serializable_instance():
    from dataclasses import dataclass, field

    @dataclass
    class DC3:
        x: Any = field(default_factory=lambda: [1, 2, 3])

    # Type-level check is conservative for Any
    assert is_type_serializable(DC3) is False

    # But the actual instance is serializable; serialize should succeed
    serialize(DC3())
