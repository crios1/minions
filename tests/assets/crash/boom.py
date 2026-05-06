BOOM_MESSAGE = "intentional boom"


class BoomError(RuntimeError):
    pass


def boom() -> None:
    raise BoomError(BOOM_MESSAGE)

