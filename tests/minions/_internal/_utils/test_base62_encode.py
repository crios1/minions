from minions._internal._utils.base62_encode import base62_encode


def test_base62_encode_zero_bytes_as_zero() -> None:
    assert base62_encode(b"\x00") == "0"
    assert base62_encode(b"\x00\x00") == "0"


def test_base62_encode_uses_expected_alphabet_boundaries() -> None:
    assert base62_encode(bytes([9])) == "9"
    assert base62_encode(bytes([10])) == "A"
    assert base62_encode(bytes([35])) == "Z"
    assert base62_encode(bytes([36])) == "a"
    assert base62_encode(bytes([61])) == "z"
    assert base62_encode(bytes([62])) == "10"


def test_base62_encode_ignores_leading_zero_bytes() -> None:
    assert base62_encode(b"\x00\x3e") == "10"


def test_base62_encode_is_deterministic() -> None:
    value = b"minions"
    assert base62_encode(value) == base62_encode(value)