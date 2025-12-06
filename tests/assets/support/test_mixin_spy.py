import asyncio
import pytest
from weakref import WeakSet
from tests.assets.support.mixin_spy import SpyMixin

@pytest.mark.asyncio
async def test_spymixin_denies_instantiation():
    with pytest.raises(TypeError):
        SpyMixin()

@pytest.mark.asyncio
async def test_spymixin_is_order_agnostic():
    class Base:
        async def foo(self): ...
        
    class A(Base, SpyMixin):
        async def bar(self): ...
    
    class B(SpyMixin, Base):
        async def bar(self): ...

    A.enable_spy(); B.enable_spy()

    a = A(); b = B()

    await a.foo(); await a.bar()
    await b.foo(); await b.bar()

    assert A.get_call_counts() == B.get_call_counts() == {
        "__init__": 1, "foo": 1, "bar": 1
    }

@pytest.mark.asyncio
async def test_spymixin_wraps_noncooperative_init():
    class Base:
        def __init__(self): ... # non-cooperative init (cooperative init call super and pass args and kwargs)
        async def foo(self): ...
        
    class A(Base, SpyMixin):
        async def bar(self): ...
    
    class B(SpyMixin, Base):
        async def bar(self): ...

    A.enable_spy(); B.enable_spy()

    a = A(); b = B()

    await a.foo(); await a.bar()
    await b.foo(); await b.bar()

    assert A.get_call_counts() == B.get_call_counts() == {
        "__init__": 1, "foo": 1, "bar": 1
    }

@pytest.mark.asyncio
async def test_wraps_class_and_static_methods():
    class Base:
        @classmethod
        def cm(cls): ...
        @staticmethod
        def sm(): ...
        def im(self): ...

    class Y(Base, SpyMixin):
        pass

    Y.enable_spy()

    Y.cm()
    Y.sm()

    y = Y()
    y.im()

    assert Y.get_call_counts() == {'__init__': 1, 'cm': 1, 'sm': 1, 'im': 1}

@pytest.mark.asyncio
async def test_property_and_pre_marked_not_wrapped():
    class Base:
        @property
        def p(self):
            return 1

        def foo(self): ...

    # simulate the base already having marked/wrapped functions
    setattr(Base, '_mspy_wrapped_fns', WeakSet())
    getattr(Base, '_mspy_wrapped_fns').add(Base.__dict__['foo'])

    class Z(Base, SpyMixin):
        pass

    Z.enable_spy()

    z = Z()
    _ = z.p
    z.foo()

    counts = Z.get_call_counts()
    assert counts.get('__init__', 0) == 1
    assert 'p' not in counts
    assert 'foo' not in counts

@pytest.mark.asyncio
async def test_spymixin_records_call_counts_and_resets():
    class LifeCycle:
        async def _startup(self): ...
        async def _run(self): ...
        async def _shutdown(self): ...
    
    class Service(LifeCycle, SpyMixin):
        def foo(self): ...
        def bar(self): ...
    
    Service.enable_spy()

    srv = Service()
    await srv._startup() 
    await srv._run()
    srv.foo()
    srv.bar()
    await srv._shutdown()

    assert Service.get_call_counts() == {
        '__init__': 1,
        '_startup': 1,
        '_run': 1,
        '_shutdown': 1,
        'foo': 1,
        'bar': 1,
    }

    Service.reset()

    assert not Service.get_call_counts()

@pytest.mark.asyncio
async def test_idempotent_enable_spy_and_reset():
    class Base():
        def foo(self): ...
    
    class X(Base, SpyMixin):
        ...
    
    X.enable_spy()
    X.enable_spy()

    x = X()
    x.foo()
    x.foo()

    assert X.get_call_counts() == {'__init__': 1, 'foo': 2}

    X.reset()
    X.reset()

    assert not X.get_call_counts()

@pytest.mark.asyncio
async def test_wait_for_call():
    class Base():
        async def foo(self): ...
    
    class X(Base, SpyMixin):
        ...
    
    X.enable_spy()

    x = X()

    await asyncio.gather(
        *[X.wait_for_call('foo', count=1, timeout=1), x.foo()]
    )

    await asyncio.gather(
        *[x.foo(), X.wait_for_call('foo', count=2, timeout=1)]
    )

    assert X.get_call_counts() == {'__init__': 1, 'foo': 2}

@pytest.mark.asyncio
async def test_wait_for_calls():
    class Base():
        async def foo(self): ...
        async def bar(self): ...
    
    class X(Base, SpyMixin):
        ...
    
    X.enable_spy()

    x = X()

    await asyncio.gather(*[
            x.foo(),
            x.bar(),
            x.wait_for_calls({'foo': 1, 'bar': 1}, timeout=1)
    ])

    await asyncio.gather(*[
            x.foo(),
            x.wait_for_calls({'foo': 2, 'bar': 2}, timeout=1),
            x.bar()
    ])

    assert X.get_call_counts() == {'__init__': 1, 'foo': 2, 'bar': 2}

@pytest.mark.asyncio
async def test_multiple_waiters_remaining_behavior():
    class Base(): ...
    class X(Base, SpyMixin):
        async def foo(self): ...

    X.enable_spy()
    x = X()

    waiter1 = asyncio.create_task(X.wait_for_call('foo', count=1, timeout=1))
    waiter2 = asyncio.create_task(X.wait_for_call('foo', count=2, timeout=1))

    await asyncio.sleep(0)

    await x.foo()
    await asyncio.wait_for(waiter1, timeout=1)
    assert not waiter2.done()

    await x.foo()
    await asyncio.wait_for(waiter2, timeout=1)

    assert X.get_call_counts() == {'__init__': 1, 'foo': 2}

@pytest.mark.asyncio
async def test__spy_bump_race_guard_skips_cancelled_waiter():
    """White-box test for _spy_bump's race-guard behavior.

    This test seeds a cancelled Future directly into _mspy_waiters to cover
    the internal race-guard branch:
        if fut.done() or fut.cancelled(): continue

    Under normal usage (public API via wait_for_call / wait_for_calls),
    these Futures are always cleaned up in a `finally:` block, so the branch
    cannot be reached deterministically. However, it exists to protect
    against a real race:
      - a waiter cancels or completes,
      - before cleanup occurs, _spy_bump runs and must skip that stale Future.

    This test ensures that cancelled Futures are skipped safely and cleaned up
    without exceptions or leaks.
    """
    class _S(SpyMixin):
        async def foo(self):
            return
    
    _S.enable_spy()
    _S.reset()

    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    # Manually register a cancelled waiter
    with _S._mspy_lock:
        _S._mspy_waiters.setdefault("foo", []).append((1, fut))
    fut.cancel()

    # Trigger the bump: it should increment counts and remove the cancelled waiter
    _S._spy_bump("foo")

    # Verify the counter incremented
    assert _S.get_call_counts().get("foo", 0) == 1

    # Verify the cancelled waiter was skipped and cleaned up
    with _S._mspy_lock:
        assert "foo" not in _S._mspy_waiters

@pytest.mark.asyncio
async def test_spymixin_records_and_resets_call_history():
    class Base:
        async def foo(self): ...
        def sync(self): ...

    class Sub(Base, SpyMixin):
        async def bar(self): ...

    Sub.enable_spy()

    assert not Sub.get_call_history()

    s = Sub()

    await s.foo()
    s.sync()
    await s.bar()

    hist = Sub.get_call_history()

    assert [n for n, ts, inst_tag in hist] == ['__init__', 'foo', 'sync', 'bar']

    # timestamps should be non-decreasing
    assert all(t1 <= t2 for (_, t1, _), (_, t2, _) in zip(hist, hist[1:]))

    Sub.reset()
    assert Sub.get_call_history() == []

@pytest.mark.asyncio
async def test_spymixin_records_and_resets_call_history_multiple_instances():
    class Base:
        async def foo(self): ...

    class Sub(Base, SpyMixin):
        pass

    Sub.enable_spy()

    assert not Sub.get_call_history()

    s1 = Sub()
    s2 = Sub()

    await s1.foo()
    await s2.foo()
    await s1.foo()

    hist = Sub.get_call_history()

    assert [n for n, ts, inst_tag in hist] == ['__init__', '__init__', 'foo', 'foo', 'foo']

    # timestamps should be non-decreasing
    assert all(t1 <= t2 for (_, t1, _), (_, t2, _) in zip(hist, hist[1:]))

    Sub.reset()
    assert Sub.get_call_history() == []

@pytest.mark.asyncio
async def test_await_and_pin_default():
    class Base:
        async def foo(self): ...
        async def bar(self): ...

    class Sub(Base, SpyMixin):
        pass

    Sub.enable_spy()

    s = Sub()

    unpin, _, _ = await asyncio.gather(
        Sub.await_and_pin_call_counts({"foo": 2}, timeout=1.0),
        s.foo(),
        s.foo()
    )

    with pytest.raises(AssertionError):
        await s.foo()
    
    with pytest.raises(AssertionError):
        await s.bar()
    
    unpin()
    await s.foo()
    await s.bar()

@pytest.mark.asyncio
async def test_await_and_pin_allow_unlisted():
    class Base:
        async def foo(self): ...
        async def bar(self): ...

    class Sub(Base, SpyMixin):
        pass

    Sub.enable_spy()

    s = Sub()

    unpin, _, _ = await asyncio.gather(
        Sub.await_and_pin_call_counts({"foo": 2}, timeout=1.0, allow_unlisted=True),
        s.foo(),
        s.foo()
    )

    await s.bar()

    with pytest.raises(AssertionError):
        await s.foo()

    unpin()
    await s.foo()
    await s.bar()

@pytest.mark.asyncio
async def test_await_and_pin_custom_on_extra():

    class Base:
        async def foo(self): ...
        async def bar(self): ...

    class Sub(Base, SpyMixin):
        pass

    def raise_runtime_err(*args, **kwargs):
        raise RuntimeError()

    Sub.enable_spy()

    s = Sub()

    await asyncio.gather(
        Sub.await_and_pin_call_counts({"foo": 2}, timeout=1.0, on_extra=raise_runtime_err),
        s.foo(),
        s.foo()
    )

    with pytest.raises(RuntimeError):
        await s.foo()

    with pytest.raises(RuntimeError):
        await s.bar()

# ---- Test fixtures ----

@pytest.fixture
def S():
    class Base:
        pass
    class S(SpyMixin, Base):
        def a(self): return "a"
        def b(self): return "b"
        def c(self): return "c"
        async def aa(self): return "aa"
    S.enable_spy()
    yield S
    S.reset()

@pytest.fixture
def T():
    class Base:
        pass
    class T(SpyMixin, Base):
        def x(self): return "x"
        def y(self): return "y"
    T.enable_spy()
    yield T
    T.reset()

# ---- Tests ----

def test_empty_subsequence_noop(S):
    S.assert_call_order([])

def test_contiguous_in_order(S):
    s = S()
    s.a(); s.b()
    S.assert_call_order(['__init__', 'a', 'b'])

def test_noncontiguous_in_order(S):
    s = S()
    s.a(); s.c(); s.b()
    S.assert_call_order(['__init__', 'a', 'b'])

def test_noncontiguous_in_order_multicalls(S):
    s = S()
    s.a(); s.a(); s.c(); s.b()
    S.assert_call_order(['__init__', 'a', 'b'])

def test_missing_raises_and_message_contains_tail_and_history(S):
    s = S()
    s.a()
    with pytest.raises(AssertionError) as e:
        S.assert_call_order(['a', 'b', 'c'])
    msg = str(e.value)
    assert "Missing from this point: ['b', 'c']" in msg
    assert "Full history names: " in msg
    assert "__init__" in msg and "a" in msg

def test_interleaved_histories_are_isolated(S, T):
    s = S(); t = T()
    s.a(); t.x(); s.b(); t.y()
    S.assert_call_order(['__init__', 'a', 'b'])
    T.assert_call_order(['__init__', 'x', 'y'])

@pytest.mark.asyncio
async def test_async_method_participates_in_ordering(S):
    s = S()
    await s.aa()
    s.a()
    S.assert_call_order(['__init__', 'aa', 'a'])
