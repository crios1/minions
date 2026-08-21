import asyncio
import inspect
import itertools
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import wraps
from types import FunctionType
from typing import Any, Generic, TypeVar, cast
from weakref import ReferenceType, WeakKeyDictionary, ref

T_Component = TypeVar("T_Component", bound=object)


@dataclass(frozen=True, slots=True)
class _ObservedInstance:
    reference: ReferenceType[object]
    tag: int


@dataclass(frozen=True, slots=True)
class _CallObservation:
    method_name: str
    timestamp_ns: int
    instance_tag: int | None


@dataclass(frozen=True, slots=True)
class _CallCountPin:
    limits: dict[str, int]
    on_extra_call: Callable[[str, int, int], object] | None
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
        self._call_history: list[_CallObservation] = []
        self._call_count_waiters_by_method: dict[str, list[_CallCountWaiter]] = {}
        self._instance_tag_counter = itertools.count(1)
        self._observed_instances_by_id: dict[int, _ObservedInstance] = {}
        self._call_count_pin: _CallCountPin | None = None
        self._instrument()

    def _instance_tag(self, instance: object) -> int:
        instance_id = id(instance)
        with self._lock:
            existing = self._observed_instances_by_id.get(instance_id)
            if existing is not None and existing.reference() is instance:
                return existing.tag

            tag = next(self._instance_tag_counter)

            def remove_instance(instance_ref: ReferenceType[object]) -> None:
                with self._lock:
                    current = self._observed_instances_by_id.get(instance_id)
                    if current is not None and current.reference is instance_ref:
                        self._observed_instances_by_id.pop(instance_id, None)

            instance_ref = ref(instance, remove_instance)
            self._observed_instances_by_id[instance_id] = _ObservedInstance(
                reference=instance_ref,
                tag=tag,
            )
            return tag

    def instance_tag(self, instance: T_Component) -> int | None:
        """Return the component instance's tag."""
        with self._lock:
            existing = self._observed_instances_by_id.get(id(instance))
            if existing is None or existing.reference() is not instance:
                return None
            return existing.tag

    def instance_tags(self) -> set[int]:
        """Return the instance tags present in recorded calls."""
        with self._lock:
            return {
                observation.instance_tag
                for observation in self._call_history
                if observation.instance_tag is not None
            }

    @staticmethod
    def _resolve_waiter_if_pending(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)

    def _record(self, name: str, instance_tag: int | None = None) -> None:
        waiters_to_notify: list[_CallCountWaiter] = []

        with self._lock:
            self._call_counts[name] = self._call_counts.get(name, 0) + 1
            current = self._call_counts[name]
            self._call_history.append(
                _CallObservation(
                    method_name=name,
                    timestamp_ns=time.perf_counter_ns(),
                    instance_tag=instance_tag,
                )
            )

            call_count_pin = self._call_count_pin
            if call_count_pin is not None:
                if (
                    not call_count_pin.allow_unlisted_calls
                    and name not in call_count_pin.limits
                ):
                    if call_count_pin.on_extra_call is not None:
                        call_count_pin.on_extra_call(name, current, 0)
                    raise AssertionError(f"{self.component_cls.__name__}: unexpected call {name}")
                allowed = call_count_pin.limits.get(name)
                if allowed is not None and current > allowed:
                    if call_count_pin.on_extra_call is not None:
                        call_count_pin.on_extra_call(name, current, allowed)
                    raise AssertionError(
                        f"{self.component_cls.__name__}: call overflow for {name}: "
                        f"{current} > {allowed}"
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

    def _instance_tag_for_owner(
        self,
        owner: object,
        *,
        class_method: bool = False,
    ) -> int | None:
        if class_method:
            if isinstance(owner, type) and issubclass(owner, self.component_cls):
                return None
        elif isinstance(owner, self.component_cls):
            return self._instance_tag(owner)
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
            tag = self._instance_tag_for_owner(instance)
            try:
                return original_init(instance, *args, **kwargs)
            finally:
                self._record("__init__", tag)

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
                    instance_tag = (
                        None
                        if is_static
                        else self._instance_tag_for_owner(
                            owner,
                            class_method=is_class,
                        )
                    )
                    self._record(name, instance_tag)
                    return await function(*args, **kwargs)

                wrapped: object = async_wrapper
            else:

                @wraps(function)
                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    owner = component_cls if not args else args[0]
                    instance_tag = (
                        None
                        if is_static
                        else self._instance_tag_for_owner(
                            owner,
                            class_method=is_class,
                        )
                    )
                    self._record(name, instance_tag)
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
        """Clear recorded calls and installed call-count limits."""
        with self._lock:
            self._call_counts = {}
            self._call_history.clear()
            self._call_count_waiters_by_method.clear()
            self._call_count_pin = None

    def call_counts(self) -> dict[str, int]:
        """Return a snapshot of recorded method call counts."""
        with self._lock:
            return dict(self._call_counts)

    def call_history(self) -> list[tuple[str, int, int | None]]:
        """Return recorded calls in chronological order."""
        with self._lock:
            return [
                (
                    observation.method_name,
                    observation.timestamp_ns,
                    observation.instance_tag,
                )
                for observation in self._call_history
            ]

    def assert_call_order(self, sub_seq: Iterable[str]) -> None:
        """Assert that methods were called in the given relative order."""
        with self._lock:
            actual = [observation.method_name for observation in self._call_history]

        remaining = iter(actual)
        expected_names = list(sub_seq)
        for index, expected_name in enumerate(expected_names):
            if any(name == expected_name for name in remaining):
                continue
            raise AssertionError(
                f"Expected subsequence {expected_names} not found in call history.\n"
                f"Missing from this point: {expected_names[index:]}\n"
                f"Full history names: {actual}"
            )

    def assert_call_order_for_instance(
        self, instance_tag: int, sub_seq: Iterable[str]
    ) -> None:
        """Assert that methods were called on the specified instance in the given relative order."""
        with self._lock:
            actual = [
                observation.method_name
                for observation in self._call_history
                if observation.instance_tag == instance_tag
            ]

        remaining = iter(actual)
        expected_names = list(sub_seq)
        for index, expected_name in enumerate(expected_names):
            if any(name == expected_name for name in remaining):
                continue
            raise AssertionError(
                f"Expected subsequence {expected_names} not found for instance tag "
                f"{instance_tag}.\n"
                f"Missing from this point: {expected_names[index:]}\n"
                f"Full history names for instance tag {instance_tag}: {actual}"
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

    async def await_and_pin_call_counts(
        self,
        expected: dict[str, int],
        *,
        timeout: float = 5.0,
        on_extra: Callable[[str, int, int], object] | None = None,
        allow_unlisted: bool = False,
    ) -> Callable[[], None]:
        """Wait for exact call counts and keep them as limits until released.

        Return a callback that releases the limits. Until released, exceeding an
        expected count raises ``AssertionError``. Calling a method absent from
        ``expected`` also raises unless ``allow_unlisted`` is true. When an expected
        count is exceeded or a method absent from ``expected`` is disallowed,
        ``on_extra``, if provided, receives the method name, recorded count, and
        allowed count.
        """
        call_count_pin = _CallCountPin(
            limits=dict(expected),
            on_extra_call=on_extra,
            allow_unlisted_calls=allow_unlisted,
        )
        with self._lock:
            self._call_count_pin = call_count_pin

        try:
            with self._lock:
                for name, allowed in call_count_pin.limits.items():
                    current = self._call_counts.get(name, 0)
                    if current <= allowed:
                        continue
                    if call_count_pin.on_extra_call is not None:
                        call_count_pin.on_extra_call(name, current, allowed)
                    raise AssertionError(
                        f"{self.component_cls.__name__}: call overflow for {name}: "
                        f"{current} > {allowed}"
                    )
            await self.wait_for_calls(call_count_pin.limits, timeout=timeout)
        except BaseException:
            self._clear_call_count_pin()
            raise

        return self._clear_call_count_pin

    def _clear_call_count_pin(self) -> None:
        with self._lock:
            self._call_count_pin = None


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
