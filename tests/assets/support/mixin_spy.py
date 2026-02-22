import asyncio
import inspect
import time
import threading
import itertools
from functools import wraps
from types import FunctionType
from typing import Any, Iterable, Callable
from weakref import WeakSet

from tests.assets.support.mixin import Mixin

class SpyMixin(Mixin):
    """Test mixin for tracking method call counts on a subclass.

    When enabled via `enable_spy()`, all instance and class methods of the
    subclass are wrapped so each call increments a counter. These counters
    are stored per subclass (not per instance) and can be queried using
    `get_call_counts()` or reset with `reset_spy()`.

    Async tests can also `await wait_for_call()` or `wait_for_calls()` to
    pause until the expected number of invocations has occurred. These methods
    effectively serve as async assertions, raising on timeout if expectations
    aren't met.

    The wait_for_call/wait_for_calls are useful for waiting for minion work to finish in a test.
    The assert_call_orer method is useful for asserting call order.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._mspy_enabled = False
        cls._mspy_lock = threading.RLock()
        cls._mspy_wrapped_fns: WeakSet[FunctionType] = WeakSet()
        cls._mspy_counts: dict[str, int] = {}
        cls._mspy_waiters: dict[str, list[tuple[int, asyncio.Future]]] = {} # (mapping name -> list[(target_count, Future)])
        cls._mspy_next_instance_id = itertools.count(1)
        cls._mspy_count_history: list[tuple[str, int, int | None]] = []
        # _mspy_count_history is in chronological order
        # and it's tuples are name, perf_counter_ns timestamp, instance id
        cls._mspy_limit: dict[str, int] | None = None
        cls._mspy_on_extra: Callable | None = None
        cls._mspy_fail_on_unlisted: bool = True

        # --- Eager-wrap ONLY __init__ as a pure passthrough anchor ---
        orig_init = cls.__dict__.get("__init__", getattr(cls, "__init__", object.__init__))
        @wraps(orig_init)
        def _mspy_init_anchor(self, *a, **k):
            # no counting here; just preserve a stable __wrapped__ chain
            return orig_init(self, *a, **k)
        setattr(cls, "__init__", _mspy_init_anchor)

    @classmethod
    def _spy_bump(cls, name: str, instance_tag: int | None = None):
        """increment the counter for `name` and notify waiters

        instance_tag is an optional integer identifying the instance that made the call.
        Non-instance or class/static calls will use None.
        """
        to_notify: list[asyncio.Future] = []

        with cls._mspy_lock:
            cls._mspy_counts[name] = cls._mspy_counts.get(name, 0) + 1
            current = cls._mspy_counts[name]
            cls._mspy_count_history.append((name, time.perf_counter_ns(), instance_tag))

            lim = cls._mspy_limit
            if lim is not None:
                if cls._mspy_fail_on_unlisted and name not in lim:
                    cb = cls._mspy_on_extra
                    if cb: cb(name, current, 0)
                    raise AssertionError(f"{cls.__name__}: unexpected call {name}")
                allowed = lim.get(name, float("inf"))
                if current > allowed:
                    cb = cls._mspy_on_extra
                    if cb: cb(name, current, allowed)
                    raise AssertionError(
                        f"{cls.__name__}: call overflow for {name}: {current} > {allowed}"
                    )

            waiters = cls._mspy_waiters.get(name, [])
            if not waiters:
                return

            remaining: list[tuple[int, asyncio.Future]] = []
            for target, fut in waiters:
                # A waiter can be cancelled/completed just before wait_for_call()'s finally removes it.
                # _spy_bump may run from another task/thread; skip stale futures to avoid set_result() on done/cancelled.
                if fut.done() or fut.cancelled():
                    continue
                if current >= target:
                    to_notify.append(fut)
                else:
                    remaining.append((target, fut))

            if remaining:
                cls._mspy_waiters[name] = remaining
            else:
                cls._mspy_waiters.pop(name, None)

        for fut in to_notify:
            fut.get_loop().call_soon_threadsafe(fut.set_result, None)

    @classmethod
    def enable_spy(cls) -> None:
        """enables class call spying"""

        with cls._mspy_lock:
            if getattr(cls, "_mspy_enabled", False):
                return

        # --- Rewrap __init__ once, as the single counting wrapper ---
        with cls._mspy_lock:
            f = cls.__dict__.get("__init__", getattr(cls, "__init__", object.__init__))
            base_fn = f
            # Walk through any prior wraps (our anchor, dataclass, etc.)
            while hasattr(base_fn, "__wrapped__"):
                base_fn = base_fn.__wrapped__

            @wraps(base_fn)
            def _mspy_init_counting(self, *a, __base=base_fn, **k):
                # assign a runtime instance tag before running __init__ body
                if not hasattr(self, "_mspy_instance_tag"):
                    self._mspy_instance_tag = next(cls._mspy_next_instance_id)
                try:
                    return __base(self, *a, **k)
                finally:
                    type(self)._spy_bump("__init__", getattr(self, "_mspy_instance_tag", None))

            setattr(cls, "__init__", _mspy_init_counting)

        def is_marked(fn: FunctionType) -> bool:
            for base in cls.__mro__:
                wrapped = base.__dict__.get("_mspy_wrapped_fns")
                if wrapped is not None and fn in wrapped:
                    return True
            return False

        def mark(fn: FunctionType) -> None:
            with cls._mspy_lock:
                cls._mspy_wrapped_fns.add(fn)

        def should_wrap(name: str, obj: Any) -> bool:
            if name.startswith("__") and name.endswith("__"):
                return False
            if isinstance(obj, property):
                return False
            fn = obj.__func__ if isinstance(obj, (classmethod, staticmethod)) else obj
            if not isinstance(fn, FunctionType):
                return False
            if is_marked(fn):
                return False
            return True

        def wrap_callable(name: str, obj: Any) -> Any:
            if isinstance(obj, classmethod):
                fn, rewrap, is_static = obj.__func__, classmethod, False
            elif isinstance(obj, staticmethod):
                fn, rewrap, is_static = obj.__func__, staticmethod, True
            else:
                fn, rewrap, is_static = obj, None, False

            if inspect.iscoroutinefunction(fn):
                @wraps(fn)
                async def _async_wrapper(*args, **kwargs):
                    owner = cls if is_static else (args[0] if args else cls)
                    typ = owner if inspect.isclass(owner) else type(owner)
                    instance_tag = getattr(owner, "_mspy_instance_tag", None)
                    typ._spy_bump(name, instance_tag)
                    return await fn(*args, **kwargs)

                mark(fn)
                return rewrap(_async_wrapper) if rewrap else _async_wrapper

            @wraps(fn)
            def _sync_wrapper(*args, **kwargs):
                owner = cls if is_static else (args[0] if args else cls)
                typ = owner if inspect.isclass(owner) else type(owner)
                instance_tag = getattr(owner, "_mspy_instance_tag", None)
                typ._spy_bump(name, instance_tag)
                return fn(*args, **kwargs)

            mark(fn)
            return rewrap(_sync_wrapper) if rewrap else _sync_wrapper

        seen: set[str] = set()
        for base_cls in cls.__mro__:
            if base_cls in (SpyMixin, object):
                continue
            for name, obj in base_cls.__dict__.items():
                if name in seen:
                    continue
                seen.add(name)
                if should_wrap(name, obj):
                    setattr(cls, name, wrap_callable(name, obj))

        with cls._mspy_lock:
            cls._mspy_enabled = True

    @classmethod
    def reset_spy(cls) -> None:
        """clears class call counts"""
        with cls._mspy_lock:
            cls._mspy_counts = {}
            cls._mspy_waiters.clear()
            cls._mspy_count_history.clear()

    @classmethod
    def get_call_history(cls) -> list[tuple[str, int, int | None]]:
        with cls._mspy_lock:
            return list(cls._mspy_count_history)

    @classmethod
    def assert_call_order(cls, sub_seq: Iterable[str]):
        """
        Asserts that the provided sequence of call names appears
        as a (possibly non-contiguous) subsequence of the call history,
        i.e., that the relative ordering of the expected calls is preserved.

        The provided sequence must be a (possibly non-contiguous) subsequence
        of the call history, meaning each name must appear in the same relative
        order, though other calls may appear in between. If the expected order
        is not found, raises AssertionError with a detailed message showing the
        missing portion and the full call history.

        Example:
            SubClass.assert_call_order([
                '__init__', 'get_all_contexts',
                'save_context', 'delete_context'
            ])
        """

        with cls._mspy_lock:
            names = [n for n, _, _ in cls._mspy_count_history]

        sub = list(sub_seq)
        if not sub:
            return

        def _seek(it, target):
            for x in it:
                if x == target:
                    return True
            return False

        it = iter(names)
        for i, expected in enumerate(sub):
            if _seek(it, expected):
                continue
            raise AssertionError(
                f"Expected subsequence {sub} not found in call history.\n"
                f"Missing from this point: {sub[i:]}\nFull history names: {names}"
        )

    @classmethod
    def assert_call_order_for_instance(cls, instance_tag: int, sub_seq: Iterable[str]):
        """Assert that the provided sequence appears (possibly non-contiguously)
        in the call history for a specific instance tag.
        """
        with cls._mspy_lock:
            names = [n for n, _, tag in cls._mspy_count_history if tag == instance_tag]

        sub = list(sub_seq)
        if not sub:
            return

        def _seek(it, target):
            for x in it:
                if x == target:
                    return True
            return False

        it = iter(names)
        for i, expected in enumerate(sub):
            if _seek(it, expected):
                continue
            raise AssertionError(
                f"Expected subsequence {sub} not found for tag {instance_tag}.\n"
                f"Missing from this point: {sub[i:]}\nFull history names for tag {instance_tag}: {names}"
            )

    @classmethod
    def get_instance_tags(cls) -> set[int]:
        with cls._mspy_lock:
            return {tag for _, _, tag in cls._mspy_count_history if tag is not None}

    @classmethod
    def get_call_counts(cls) -> dict[str, int]:
        with cls._mspy_lock:
            return dict(cls._mspy_counts)

    @classmethod
    async def wait_for_call(cls, name: str, *, count: int = 1, timeout: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        with cls._mspy_lock:
            current = getattr(cls, '_mspy_counts', {}).get(name, 0)
            if current >= count:
                return
            target = count
            cls._mspy_waiters.setdefault(name, []).append((target, fut))

        try:
            await asyncio.wait_for(fut, timeout)
        finally:
            with cls._mspy_lock:
                lst = cls._mspy_waiters.get(name, [])
                new_lst = [(t, f) for (t, f) in lst if f is not fut and not f.done()]
                if new_lst:
                    cls._mspy_waiters[name] = new_lst
                else:
                    cls._mspy_waiters.pop(name, None)

    @classmethod
    async def wait_for_calls(cls, expected: dict, *, timeout: float = 5.0) -> None:
        counts = cls.get_call_counts()
        if all(counts.get(name, 0) >= count for name, count in expected.items()):
            return
        await asyncio.gather(*[
            cls.wait_for_call(name, count=count, timeout=timeout)
            for name, count in expected.items()
        ])

    @classmethod
    async def await_and_pin_call_counts(
        cls,
        expected: dict[str, int],
        *,
        timeout: float = 5.0,
        on_extra: Callable | None = None,
        allow_unlisted: bool = False,
    ) -> Callable:
        """
        Atomically: (1) set per-method limits so extra/unlisted calls fail,
        (2) await EXACT 'expected' counts, (3) keep limits pinned.
        Returns an unpin() function to clear limits.
        """
        with cls._mspy_lock:
            cls._mspy_limit = dict(expected)
            cls._mspy_on_extra = on_extra
            cls._mspy_fail_on_unlisted = not allow_unlisted

        try:
            await cls.wait_for_calls(expected=expected, timeout=timeout)
        except Exception:
            with cls._mspy_lock:
                cls._mspy_limit = None
                cls._mspy_on_extra = None
                cls._mspy_fail_on_unlisted = True
            raise

        def unpin() -> None:
            with cls._mspy_lock:
                cls._mspy_limit = None
                cls._mspy_on_extra = None
                cls._mspy_fail_on_unlisted = True

        return unpin
