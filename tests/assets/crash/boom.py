from typing import NoReturn

BOOM_MESSAGE = "intentional boom"


class BoomError(RuntimeError):
    pass


def boom() -> NoReturn:
    raise BoomError(BOOM_MESSAGE)
