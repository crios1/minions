from abc import ABCMeta
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager

from .component_spy import (
    CallCountLimitViolation,
    RecordedCall,
    T_Component,
    component_spy_for,
    enable_component_spy,
)


class ComponentSpyMeta(ABCMeta):
    """Expose method-call recording controls while keeping observation state
    outside component classes and instances.
    """

    def enable_spy(cls) -> None:
        """Enable call observation for this component class."""
        enable_component_spy(cls)

    def reset_spy(cls) -> None:
        """Clear recorded calls and instance identities.

        Raises ``RuntimeError`` while ``wait_for_call`` is pending or an
        ``enforce_call_count_limits`` context is active.
        """
        component_spy_for(cls).reset()

    def get_call_counts(cls) -> dict[str, int]:
        """Return a snapshot of recorded method call counts."""
        return component_spy_for(cls).call_counts()

    def get_call_history(cls) -> tuple[RecordedCall, ...]:
        """Return recorded calls in chronological order."""
        return component_spy_for(cls).call_history()

    def get_instance_identity(
        cls: type[T_Component], instance: T_Component
    ) -> int | None:
        """Return the component instance's identity in recorded calls."""
        return component_spy_for(cls).instance_identity(instance)

    def get_instance_identities(cls) -> set[int]:
        """Return the instance identities present in recorded calls."""
        return component_spy_for(cls).instance_identities()

    def assert_call_order(cls, subsequence: Iterable[str]) -> None:
        """Assert that methods were called in the given relative order."""
        component_spy_for(cls).assert_call_order(subsequence)

    def assert_call_order_for_instance(
        cls,
        instance_identity: int,
        subsequence: Iterable[str],
    ) -> None:
        """Assert that methods were called on the specified instance in the given relative order."""
        component_spy_for(cls).assert_call_order_for_instance(
            instance_identity,
            subsequence,
        )

    async def wait_for_call(
        cls,
        name: str,
        *,
        count: int = 1,
        timeout: float = 5.0,
    ) -> None:
        """Wait until the recorded call count for a method reaches ``count``."""
        await component_spy_for(cls).wait_for_call(
            name,
            count=count,
            timeout=timeout,
        )

    async def wait_for_calls(
        cls,
        expected: dict[str, int],
        *,
        timeout: float = 5.0,
    ) -> None:
        """Wait until each method's recorded call count reaches its requested count."""
        await component_spy_for(cls).wait_for_calls(
            expected,
            timeout=timeout,
        )

    def enforce_call_count_limits(
        cls,
        limits: dict[str, int],
        *,
        on_limit_exceeded: Callable[[CallCountLimitViolation], None] | None = None,
        allow_unlisted_calls: bool = False,
    ) -> AbstractContextManager[None]:
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
        return component_spy_for(cls).enforce_call_count_limits(
            limits,
            on_limit_exceeded=on_limit_exceeded,
            allow_unlisted_calls=allow_unlisted_calls,
        )
