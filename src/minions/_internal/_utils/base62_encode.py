BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def base62_encode(data: bytes) -> str:
    value = int.from_bytes(data, "big")

    if value == 0:
        return BASE62_ALPHABET[0]

    chars: list[str] = []

    while value:
        value, remainder = divmod(value, 62)
        chars.append(BASE62_ALPHABET[remainder])

    return "".join(reversed(chars))
