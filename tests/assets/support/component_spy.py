import asyncio
import inspect
import itertools
import threading
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from types import FunctionType
from typing import Any, Generic, TypeVar, cast
from weakref import ReferenceType, WeakKeyDictionary, ref

T_Component = TypeVar("T_Component", bound=object)


@dataclass(frozen=True, slots=True)
class _ObservedInstance:
    reference: ReferenceType[object]
    identity: int


@dataclass(frozen=True, slots=True)
class RecordedCall:
    """A component method call recorded by ComponentSpy."""

    method_name: str
    instance_identity: int | None


@dataclass(frozen=True, slots=True)
class CallCountLimitViolation:
    """A component method invocation that exceeded an enforced call-count limit."""

    component_cls: type[object]
    method_name: str
    observed_count: int
    allowed_count: int


@dataclass(frozen=True, slots=True)
class _CallCountLimits:
    limits: dict[str, int]
    unlisted_call_baselines: dict[str, int]
    on_limit_exceeded: Callable[[CallCountLimitViolation], None] | None
    allow_unlisted_calls: bool


@dataclass(frozen=True, slots=True)
class _CallCountWaiter:
    target_count: int
    event_loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]


class ComponentSpy(Generic[T_Component]):
    """Record method calls while keeping observation state outside component
    classes and instances.
    """

    def __init__(self, component_cls: type[T_Component]) -> None:
        self.component_cls = component_cls
        self._lock = threading.RLock()
        self._call_counts: dict[str, int] = {}
        self._call_history: list[RecordedCall] = []
        self._call_count_waiters_by_method: dict[str, list[_CallCountWaiter]] = {}
        self._instance_identity_counter = itertools.count(1)
        self._observed_instances_by_id: dict[int, _ObservedInstance] = {}
        self._active_call_count_limits: _CallCountLimits | None = None
        self._instrument()

    def _instance_identity(self, instance: object) -> int:
        instance_id = id(instance)
        with self._lock:
            existing = self._observed_instances_by_id.get(instance_id)
            if existing is not None and existing.reference() is instance:
                return existing.identity

            identity = next(self._instance_identity_counter)

            def remove_instance(instance_ref: ReferenceType[object]) -> None:
                with self._lock:
                    current = self._observed_instances_by_id.get(instance_id)
                    if current is not None and current.reference is instance_ref:
                        self._observed_instances_by_id.pop(instance_id, None)

            instance_ref = ref(instance, remove_instance)
            self._observed_instances_by_id[instance_id] = _ObservedInstance(
                reference=instance_ref,
                identity=identity,
            )
            return identity

    def instance_identity(self, instance: T_Component) -> int | None:
        """Return the component instance's identity in recorded calls."""
        with self._lock:
            existing = self._observed_instances_by_id.get(id(instance))
            if existing is None or existing.reference() is not instance:
                return None
            return existing.identity

    def instance_identities(self) -> set[int]:
        """Return the instance identities present in recorded calls."""
        with self._lock:
            return {
                observation.instance_identity
                for observation in self._call_history
                if observation.instance_identity is not None
            }

    @staticmethod
    def _resolve_waiter_if_pending(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)

    def _record(self, name: str, instance_identity: int | None = None) -> None:
        waiters_to_notify: list[_CallCountWaiter] = []

        with self._lock:
            self._call_counts[name] = self._call_counts.get(name, 0) + 1
            current = self._call_counts[name]
            self._call_history.append(
                RecordedCall(
                    method_name=name,
                    instance_identity=instance_identity,
                )
            )

            call_count_limits = self._active_call_count_limits
            if call_count_limits is not None:
                if (
                    not call_count_limits.allow_unlisted_calls
                    and name not in call_count_limits.limits
                ):
                    allowed = call_count_limits.unlisted_call_baselines.get(name, 0)
                    if call_count_limits.on_limit_exceeded is not None:
                        call_count_limits.on_limit_exceeded(
                            CallCountLimitViolation(
                                component_cls=self.component_cls,
                                method_name=name,
                                observed_count=current,
                                allowed_count=allowed,
                            )
                        )
                    raise AssertionError(f"{self.component_cls.__name__}: unexpected call {name}")
                allowed = call_count_limits.limits.get(name)
                if allowed is not None and current > allowed:
                    if call_count_limits.on_limit_exceeded is not None:
                        call_count_limits.on_limit_exceeded(
                            CallCountLimitViolation(
                                component_cls=self.component_cls,
                                method_name=name,
                                observed_count=current,
                                allowed_count=allowed,
                            )
                        )
                    raise AssertionError(
                        f"{self.component_cls.__name__}: recorded call count for "
                        f"{name} is {current}; limit is {allowed}"
                    )

            remaining_waiters: list[_CallCountWaiter] = []
            for waiter in self._call_count_waiters_by_method.get(name, []):
                if current >= waiter.target_count:
                    waiters_to_notify.append(waiter)
                else:
                    remaining_waiters.append(waiter)

            if remaining_waiters:
                self._call_count_waiters_by_method[name] = remaining_waiters
            else:
                self._call_count_waiters_by_method.pop(name, None)

        for waiter in waiters_to_notify:
            waiter.event_loop.call_soon_threadsafe(
                self._resolve_waiter_if_pending,
                waiter.future,
            )

    def _instance_identity_for_owner(
        self,
        owner: object,
        *,
        class_method: bool = False,
    ) -> int | None:
        if class_method:
            if isinstance(owner, type) and issubclass(owner, self.component_cls):
                return None
        elif isinstance(owner, self.component_cls):
            return self._instance_identity(owner)
        raise TypeError(
            f"{self.component_cls.__name__} spy wrapper received a "
            f"non-{self.component_cls.__name__} owner."
        )

    def _instrument(self) -> None:
        component_cls = self.component_cls
        original_init = component_cls.__dict__.get(
            "__init__", getattr(component_cls, "__init__", object.__init__)
        )

        @wraps(original_init)
        def init_wrapper(instance: object, *args: Any, **kwargs: Any) -> Any:
            identity = self._instance_identity_for_owner(instance)
            try:
                return original_init(instance, *args, **kwargs)
            finally:
                self._record("__init__", identity)

        setattr(component_cls, "__init__", init_wrapper)

        def should_wrap(name: str, descriptor: object) -> bool:
            if name == "__init__" or (name.startswith("__") and name.endswith("__")):
                return False
            if isinstance(descriptor, property):
                return False
            function = getattr(descriptor, "__func__", descriptor)
            return isinstance(function, FunctionType)

        def wrap(name: str, descriptor: object) -> object:
            if isinstance(descriptor, classmethod):
                is_class = True
                is_static = False
            elif isinstance(descriptor, staticmethod):
                is_class = False
                is_static = True
            else:
                is_class = False
                is_static = False
            attribute = cast(object, descriptor)
            function = cast(
                Callable[..., Any],
                getattr(attribute, "__func__", attribute),
            )

            if inspect.iscoroutinefunction(function):

                @wraps(function)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    owner = component_cls if not args else args[0]
                    instance_identity = (
                        None
                        if is_static
                        else self._instance_identity_for_owner(
                            owner,
                            class_method=is_class,
                        )
                    )
                    self._record(name, instance_identity)
                    return await function(*args, **kwargs)

                wrapped: object = async_wrapper
            else:

                @wraps(function)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    owner = component_cls if not args else args[0]
                    instance_identity = (
                        None
                        if is_static
                        else self._instance_identity_for_owner(
                            owner,
                            class_method=is_class,
                        )
                    )
                    self._record(name, instance_identity)
                    return function(*args, **kwargs)

                wrapped = sync_wrapper

            wrapped_callable = cast(Callable[..., Any], wrapped)
            if is_class:
                return classmethod(wrapped_callable)
            if is_static:
                return staticmethod(wrapped_callable)
            return wrapped_callable

        seen = {"__init__"}
        for base_cls in component_cls.__mro__:
            if base_cls is object:
                continue
            for name, descriptor in base_cls.__dict__.items():
                if name in seen:
                    continue
                seen.add(name)
                if should_wrap(name, descriptor):
                    setattr(component_cls, name, wrap(name, descriptor))

    def reset(self) -> None:
        """Clear recorded calls and instance identities.

        Raises ``RuntimeError`` while ``wait_for_call`` is pending or an
        ``enforce_call_count_limits`` context is active.
        """
        with self._lock:
            if self._call_count_waiters_by_method:
                raise RuntimeError(
                    f"{self.component_cls.__name__}: cannot reset while "
                    "wait_for_call is pending"
                )
            if self._active_call_count_limits is not None:
                raise RuntimeError(
                    f"{self.component_cls.__name__}: cannot reset within an "
                    "enforce_call_count_limits context"
                )
            self._call_counts = {}
            self._call_history.clear()
            self._observed_instances_by_id.clear()

    def call_counts(self) -> dict[str, int]:
        """Return a snapshot of recorded method call counts."""
        with self._lock:
            return dict(self._call_counts)

    def call_history(self) -> tuple[RecordedCall, ...]:
        """Return recorded calls in chronological order."""
        with self._lock:
            return tuple(self._call_history)

    def assert_call_order(self, subsequence: Iterable[str]) -> None:
        """Assert that methods were called in the given relative order."""
        with self._lock:
            actual = [observation.method_name for observation in self._call_history]

        remaining = iter(actual)
        expected_names = list(subsequence)
        for index, expected_name in enumerate(expected_names):
            if any(name == expected_name for name in remaining):
                continue
            raise AssertionError(
                f"Expected subsequence {expected_names} not found in call history.\n"
                f"Missing from this point: {expected_names[index:]}\n"
                f"Full history names: {actual}"
            )

    def assert_call_order_for_instance(
        self, instance_identity: int, subsequence: Iterable[str]
    ) -> None:
        """Assert that methods were called on the specified instance in the given relative order."""
        with self._lock:
            actual = [
                observation.method_name
                for observation in self._call_history
                if observation.instance_identity == instance_identity
            ]

        remaining = iter(actual)
        expected_names = list(subsequence)
        for index, expected_name in enumerate(expected_names):
            if any(name == expected_name for name in remaining):
                continue
            raise AssertionError(
                f"Expected subsequence {expected_names} not found for instance identity "
                f"{instance_identity}.\n"
                f"Missing from this point: {expected_names[index:]}\n"
                f"Full history names for instance identity {instance_identity}: {actual}"
            )

    async def wait_for_call(self, name: str, *, count: int = 1, timeout: float = 5.0) -> None:
        """Wait until the recorded call count for a method reaches ``count``."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        with self._lock:
            if self._call_counts.get(name, 0) >= count:
                return
            waiter = _CallCountWaiter(
                target_count=count,
                event_loop=loop,
                future=future,
            )
            self._call_count_waiters_by_method.setdefault(name, []).append(waiter)

        try:
            await asyncio.wait_for(future, timeout)
        finally:
            with self._lock:
                remaining_waiters = [
                    waiter
                    for waiter in self._call_count_waiters_by_method.get(name, [])
                    if waiter.future is not future and not waiter.future.done()
                ]
                if remaining_waiters:
                    self._call_count_waiters_by_method[name] = remaining_waiters
                else:
                    self._call_count_waiters_by_method.pop(name, None)

    async def wait_for_calls(self, expected: dict[str, int], *, timeout: float = 5.0) -> None:
        """Wait until each method's recorded call count reaches its requested count."""
        if all(self.call_counts().get(name, 0) >= count for name, count in expected.items()):
            return
        await asyncio.gather(
            *(
                self.wait_for_call(name, count=count, timeout=timeout)
                for name, count in expected.items()
            )
        )

    @contextmanager
    def enforce_call_count_limits(
        self,
        limits: dict[str, int],
        *,
        on_limit_exceeded: Callable[[CallCountLimitViolation], None] | None = None,
        allow_unlisted_calls: bool = False,
    ) -> Generator[None, None, None]:
        """Enforce call-count limits for the duration of the context.

        Each limit applies to the method's cumulative recorded count, not the
        number of additional calls allowed after entering the context. Unless
        ``allow_unlisted_calls`` is true, methods absent from ``limits``
        are limited to their count when the context is entered. A violating
        invocation is recorded and blocked before its wrapped method body runs.
        When a limit is exceeded, ``on_limit_exceeded``, if provided, receives a
        ``CallCountLimitViolation``. Its exception propagates; otherwise the
        violation raises ``AssertionError``. Raises ``RuntimeError`` if call-count
        limits are already active.
        """
        configured_limits = dict(limits)
        with self._lock:
            if self._active_call_count_limits is not None:
                raise RuntimeError(
                    f"{self.component_cls.__name__}: call-count limits are already active"
                )
            call_count_limits = _CallCountLimits(
                limits=configured_limits,
                unlisted_call_baselines={
                    name: count
                    for name, count in self._call_counts.items()
                    if name not in configured_limits
                },
                on_limit_exceeded=on_limit_exceeded,
                allow_unlisted_calls=allow_unlisted_calls,
            )
            self._active_call_count_limits = call_count_limits

        try:
            with self._lock:
                for name, allowed in call_count_limits.limits.items():
                    current = self._call_counts.get(name, 0)
                    if current <= allowed:
                        continue
                    if call_count_limits.on_limit_exceeded is not None:
                        call_count_limits.on_limit_exceeded(
                            CallCountLimitViolation(
                                component_cls=self.component_cls,
                                method_name=name,
                                observed_count=current,
                                allowed_count=allowed,
                            )
                        )
                    raise AssertionError(
                        f"{self.component_cls.__name__}: recorded call count for "
                        f"{name} is {current}; limit is {allowed}"
                    )
            yield
        finally:
            with self._lock:
                self._active_call_count_limits = None


_component_spies: WeakKeyDictionary[
    type[Any], ReferenceType[ComponentSpy[Any]]
] = WeakKeyDictionary()
_component_spies_lock = threading.RLock()


def _registered_component_spy(
    component_cls: type[T_Component],
) -> ComponentSpy[T_Component] | None:
    spy_ref = _component_spies.get(component_cls)
    spy = None if spy_ref is None else spy_ref()
    return cast(ComponentSpy[T_Component] | None, spy)


def enable_component_spy(
    component_cls: type[T_Component],
) -> ComponentSpy[T_Component]:
    """Install and return external observation for a Spied* test component."""
    with _component_spies_lock:
        spy = _registered_component_spy(component_cls)
        if spy is None:
            spy = ComponentSpy(component_cls)
            _component_spies[component_cls] = ref(spy)
        return spy


def component_spy_for(component_cls: type[T_Component]) -> ComponentSpy[T_Component]:
    """Return enabled external observation for a Spied* test component."""
    with _component_spies_lock:
        spy = _registered_component_spy(component_cls)
        if spy is None:
            raise RuntimeError(
                f"{component_cls.__name__} spy is not enabled. "
                f"Call {component_cls.__name__}.enable_spy() before using spy controls."
            )
        return spy
