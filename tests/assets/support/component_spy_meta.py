from abc import ABCMeta
from collections.abc import Callable, Iterable

from .component_spy import T_Component, component_spy_for, enable_component_spy


class ComponentSpyMeta(ABCMeta):
    """Expose external spy controls on the test-only Spied* component bases."""

    def enable_spy(cls) -> None:
        enable_component_spy(cls)

    def reset_spy(cls) -> None:
        component_spy_for(cls).reset()

    def get_call_counts(cls) -> dict[str, int]:
        return component_spy_for(cls).call_counts()

    def get_call_history(cls) -> list[tuple[str, int, int | None]]:
        return component_spy_for(cls).call_history()

    def get_instance_tag(
        cls: type[T_Component], instance: T_Component
    ) -> int | None:
        return component_spy_for(cls).instance_tag(instance)

    def get_instance_tags(cls) -> set[int]:
        return component_spy_for(cls).instance_tags()

    def assert_call_order(cls, sub_seq: Iterable[str]) -> None:
        component_spy_for(cls).assert_call_order(sub_seq)

    def assert_call_order_for_instance(
        cls,
        instance_tag: int,
        sub_seq: Iterable[str],
    ) -> None:
        component_spy_for(cls).assert_call_order_for_instance(instance_tag, sub_seq)

    async def wait_for_call(
        cls,
        name: str,
        *,
        count: int = 1,
        timeout: float = 5.0,
    ) -> None:
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
        return await component_spy_for(cls).await_and_pin_call_counts(
            expected,
            timeout=timeout,
            on_extra=on_extra,
            allow_unlisted=allow_unlisted,
        )
