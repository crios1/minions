import asyncio

import pytest

from tests.assets.support.component_spy_meta import ComponentSpyMeta


class TestResetSpy:
    def test_clears_call_counts_and_history(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        component.method()

        SpiedComponent.reset_spy()

        assert SpiedComponent.get_call_counts() == {}
        assert SpiedComponent.get_call_history() == ()

    def test_repeated_calls_keep_call_counts_and_history_empty(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            pass

        SpiedComponent.enable_spy()

        SpiedComponent.reset_spy()
        SpiedComponent.reset_spy()

        assert SpiedComponent.get_call_counts() == {}
        assert SpiedComponent.get_call_history() == ()

    def test_clears_spy_instance_identities(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        first_component = SpiedComponent()
        second_component = SpiedComponent()
        assert SpiedComponent.get_spy_instance_identity(first_component) is not None
        assert SpiedComponent.get_spy_instance_identity(second_component) is not None

        SpiedComponent.reset_spy()

        assert SpiedComponent.get_spy_instance_identity(first_component) is None
        assert SpiedComponent.get_spy_instance_identity(second_component) is None
        second_component.method()
        assert SpiedComponent.get_spy_instance_identity(second_component) is not None

    @pytest.mark.asyncio
    async def test_is_rejected_while_wait_for_call_is_pending(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="cannot reset while wait_for_call is pending"):
            SpiedComponent.reset_spy()

        component.method()
        await waiter

    def test_is_rejected_within_enforce_call_count_limits_context(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()

        with SpiedComponent.enforce_call_count_limits({}):
            with pytest.raises(
                RuntimeError,
                match="cannot reset within an enforce_call_count_limits context",
            ):
                SpiedComponent.reset_spy()

            with pytest.raises(AssertionError, match="unexpected call method"):
                component.method()

        SpiedComponent.reset_spy()
