from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass, field
from typing import Any

import msgspec
import pytest

from minions._internal._utils.serialization.codec import (
    _cached_decoder,
    deserialize,
    serialize,
)

MODULE = "minions._internal._utils.serialization.codec"


@dataclass
class Event:
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    user_id: int | None = None
    active: bool = True
    type: str = "generic.event"
    source: str | None = None
    payload: dict[str, Any] | None = None
    metadata: dict[str, str] = field(default_factory=lambda: dict())
    tags: list[str] = field(default_factory=lambda: list())


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
        tags=["std", "unit"],
    )


def test_invalid_serialize() -> None:
    with pytest.raises(TypeError):
        serialize(print)


def test_invalid_deserialize() -> None:
    with pytest.raises(ValueError):
        deserialize(bytes([1, 2, 3]), Event)
    with pytest.raises(TypeError):
        deserialize("invalid", None)  # type: ignore[arg-type]


def test_decoder_cache_used_for_hashable_types() -> None:
    orig = _cached_decoder
    orig.cache_clear()

    evt = _make_event()
    blob = serialize(evt)

    out1 = deserialize(blob, type_=Event)
    assert out1 == evt
    ci1 = orig.cache_info()
    assert ci1.misses == 1
    assert ci1.hits == 0

    out2 = deserialize(blob, type_=Event)
    assert out2 == evt
    ci2 = orig.cache_info()
    assert ci2.misses == 1
    assert ci2.hits == 1


def test_decoder_fallback_for_unhashable_annotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orig = _cached_decoder
    orig.cache_clear()

    def boom(_t: object) -> msgspec.msgpack.Decoder[object]:
        raise TypeError("unhashable")

    monkeypatch.setattr(f"{MODULE}._cached_decoder", boom)

    evt = _make_event()
    blob = serialize(evt)
    out = deserialize(blob, type_=Event)
    assert out == evt


def test_decoder_cache_prevents_repeated_decoder_instantiation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orig = _cached_decoder
    orig.cache_clear()
    Decoder = msgspec.msgpack.Decoder
    inst = {"n": 0}

    class SpyDecoder:
        def __init__(self, *a: object, **kw: object) -> None:
            inst["n"] += 1
            self._inner = Decoder(*a, **kw)

        def decode(self, buf: bytes) -> object:
            return self._inner.decode(buf)

    monkeypatch.setattr("msgspec.msgpack.Decoder", SpyDecoder)

    evt = _make_event()
    blob = serialize(evt)

    out1 = deserialize(blob, type_=Event)
    assert out1 == evt
    assert inst["n"] == 1
    ci1 = orig.cache_info()
    assert ci1.misses == 1
    assert ci1.hits == 0

    out2 = deserialize(blob, type_=Event)
    assert out2 == evt
    assert inst["n"] == 1
    ci2 = orig.cache_info()
    assert ci2.misses == 1
    assert ci2.hits == 1
