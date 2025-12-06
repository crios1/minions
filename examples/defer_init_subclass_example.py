class Minion:
    def __init_subclass__(cls, *, skip_minion_validation=False, **kw):
        super().__init_subclass__(**kw)
        if skip_minion_validation:
            cls.validation_skipped = True
            return
        cls.validation_skipped = False

class SpyMixin: ...

class SpiedMinion(SpyMixin, Minion, skip_minion_validation=True):
    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)

class TestMinion(SpiedMinion):
    pass

assert getattr(Minion, "validation_skipped", None) is None
assert getattr(SpiedMinion, "validation_skipped", None) is True
assert getattr(TestMinion, "validation_skipped", None) is False
