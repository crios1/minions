class Mixin:
    def __new__(cls, *args, **kwargs):
        has_non_mixin = any(
            not issubclass(base, Mixin)
            for base in cls.__mro__[1:-1]  # skip `cls` and `object`
        )
        if not has_non_mixin:
            raise TypeError(
                f"{cls.__name__} is composed only of mixins. "
                "Include a non-Mixin base class in its composition."
            )
        return super().__new__(cls)
