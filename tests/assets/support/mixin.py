from typing import Any, TypeVar


T_Mixin = TypeVar("T_Mixin", bound="Mixin")


class Mixin:
    def __new__(cls: type[T_Mixin], *args: Any, **kwargs: Any) -> T_Mixin:
        has_non_mixin = any(
            not issubclass(base, Mixin)
            for base in cls.__mro__[1:-1]  # skip `cls` and `object`
        )
        if not has_non_mixin:
            raise TypeError(
                f"{cls.__name__} is composed only of mixins. "
                "Include a non-Mixin base class in its composition."
            )
        return object.__new__(cls)
