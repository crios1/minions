import asyncio

import pytest

from tests.assets.support.component_spy import CallCountLimitViolation
from tests.assets.support.component_spy_meta import ComponentSpyMeta


class TestEnforceCallCountLimits:
    def test_rejects_extra_call_without_running_method_body(self):
        method_body_calls = 0

        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                nonlocal method_body_calls
                method_body_calls += 1

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        component.method()

        with SpiedComponent.enforce_call_count_limits({"method": 1}):
            with pytest.raises(
                AssertionError,
                match="recorded call count for method is 2; limit is 1",
            ):
                component.method()
            assert method_body_calls == 1

        component.method()
        assert method_body_calls == 2

    def test_rejects_already_exceeded_limit_on_entry(self):
        limit_violations: list[CallCountLimitViolation] = []

        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        component.method()
        component.method()

        with pytest.raises(
            AssertionError,
            match="recorded call count for method is 2; limit is 1",
        ):
            with SpiedComponent.enforce_call_count_limits(
                {"method": 1},
                on_limit_exceeded=limit_violations.append,
            ):
                pass

        assert limit_violations == [
            CallCountLimitViolation(
                component_cls=SpiedComponent,
                method_name="method",
                observed_count=2,
                allowed_count=1,
            )
        ]
        component.method()

    def test_rejects_only_new_calls_to_unlisted_methods(self):
        limit_violations: list[CallCountLimitViolation] = []

        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method_a(self) -> None:
                return

            def method_b(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        component.method_b()
        component.method_a()

        with SpiedComponent.enforce_call_count_limits(
            {"method_a": 1},
            on_limit_exceeded=limit_violations.append,
        ):
            with pytest.raises(AssertionError, match="unexpected call method_b"):
                component.method_b()

        assert limit_violations == [
            CallCountLimitViolation(
                component_cls=SpiedComponent,
                method_name="method_b",
                observed_count=2,
                allowed_count=1,
            )
        ]

    def test_allows_unlisted_calls_when_configured(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method_a(self) -> None:
                return

            def method_b(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()

        with SpiedComponent.enforce_call_count_limits(
            {"method_a": 1},
            allow_unlisted_calls=True,
        ):
            component.method_b()
            component.method_a()

    def test_on_limit_exceeded_receives_listed_call_count_violation(
        self,
    ):
        limit_violations: list[CallCountLimitViolation] = []

        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        component.method()

        with SpiedComponent.enforce_call_count_limits(
            {"method": 1},
            on_limit_exceeded=limit_violations.append,
        ):
            with pytest.raises(
                AssertionError,
                match="recorded call count for method is 2; limit is 1",
            ):
                component.method()

        assert limit_violations == [
            CallCountLimitViolation(
                component_cls=SpiedComponent,
                method_name="method",
                observed_count=2,
                allowed_count=1,
            )
        ]

    def test_on_limit_exceeded_errors_propagate_for_listed_and_unlisted_calls(
        self,
    ):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method_a(self) -> None:
                return

            def method_b(self) -> None:
                return

        def raise_limit_violation(_: CallCountLimitViolation) -> None:
            raise RuntimeError("limit violation")

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        component.method_a()

        with SpiedComponent.enforce_call_count_limits(
            {"method_a": 1},
            on_limit_exceeded=raise_limit_violation,
        ):
            with pytest.raises(RuntimeError, match="limit violation"):
                component.method_a()
            with pytest.raises(RuntimeError, match="limit violation"):
                component.method_b()

    def test_rejects_second_context_while_one_is_active(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()

        with SpiedComponent.enforce_call_count_limits({}):
            with pytest.raises(RuntimeError, match="call-count limits are already active"):
                with SpiedComponent.enforce_call_count_limits({}):
                    pass

            with pytest.raises(AssertionError, match="unexpected call method"):
                component.method()

    @pytest.mark.asyncio
    async def test_cancelling_task_stops_enforcing_call_count_limits(self):
        class SpiedComponent(metaclass=ComponentSpyMeta):
            def method(self) -> None:
                return

        SpiedComponent.enable_spy()
        component = SpiedComponent()
        SpiedComponent.reset_spy()
        limits_entered = asyncio.Event()

        async def wait_with_call_count_limits() -> None:
            with SpiedComponent.enforce_call_count_limits({}):
                limits_entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(wait_with_call_count_limits())
        await limits_entered.wait()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        component.method()
