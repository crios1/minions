from abc import ABCMeta
from collections.abc import Callable, Iterable

from .component_spy import T_Component, component_spy_for, enable_component_spy


class ComponentSpyMeta(ABCMeta):
    """Expose method-call recording controls while keeping observation state
    outside component classes and instances.
    """

    def enable_spy(cls) -> None:
        """Enable call observation for this component class."""
        enable_component_spy(cls)

    def reset_spy(cls) -> None:
        """Clear recorded calls and instance tags.

        Raises ``RuntimeError`` while call-count synchronization is active.
        """
        component_spy_for(cls).reset()

    def get_call_counts(cls) -> dict[str, int]:
        """Return a snapshot of recorded method call counts."""
        return component_spy_for(cls).call_counts()

    def get_call_history(cls) -> list[tuple[str, int, int | None]]:
        """Return recorded calls in chronological order."""
        return component_spy_for(cls).call_history()

    def get_instance_tag(
        cls: type[T_Component], instance: T_Component
    ) -> int | None:
        """Return the component instance's tag."""
        return component_spy_for(cls).instance_tag(instance)

    def get_instance_tags(cls) -> set[int]:
        """Return the instance tags present in recorded calls."""
        return component_spy_for(cls).instance_tags()

    def assert_call_order(cls, sub_seq: Iterable[str]) -> None:
        """Assert that methods were called in the given relative order."""
        component_spy_for(cls).assert_call_order(sub_seq)

    def assert_call_order_for_instance(
        cls,
        instance_tag: int,
        sub_seq: Iterable[str],
    ) -> None:
        """Assert that methods were called on the specified instance in the given relative order."""
        component_spy_for(cls).assert_call_order_for_instance(instance_tag, sub_seq)

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

    async def await_and_pin_call_counts(
        cls,
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
        allowed count. Raises ``RuntimeError`` if call-count limits are already active.
        """
        return await component_spy_for(cls).await_and_pin_call_counts(
            expected,
            timeout=timeout,
            on_extra=on_extra,
            allow_unlisted=allow_unlisted,
        )
