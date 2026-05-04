import pytest

from minions._internal._framework.async_component import AsyncComponent
from minions._internal._framework.logger_noop import NoOpLogger
from tests.assets.support.logger_inmemory import InMemoryLogger


class DemoComponent(AsyncComponent):
    def sync_instance_method(self, value: int) -> int:
        return value + 1

    async def async_instance_method(self, value: int) -> int:
        return value + 2

    @classmethod
    def class_method(cls, value: int) -> int:
        return value + 3

    @staticmethod
    def static_method(value: int) -> int:
        return value + 4


class OtherComponent(AsyncComponent):
    def sync_instance_method(self, value: int) -> int:
        return value + 10


def free_function(value: int) -> int:
    return value + 5


class FailingComponent(AsyncComponent):
    def boom(self) -> None:
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_safe_run_and_log_accepts_bound_instance_method():
    comp = DemoComponent(NoOpLogger())

    result = await comp._mn_safe_run_and_log_failure(
        comp.sync_instance_method,
        method_args=[1],
    )

    assert result == 2


@pytest.mark.asyncio
async def test_safe_run_and_log_accepts_bound_async_instance_method():
    comp = DemoComponent(NoOpLogger())

    result = await comp._mn_safe_run_and_log_failure(
        comp.async_instance_method,
        method_args=[1],
    )

    assert result == 3


@pytest.mark.asyncio
async def test_safe_run_and_log_accepts_bound_class_method():
    comp = DemoComponent(NoOpLogger())

    result = await comp._mn_safe_run_and_log_failure(
        DemoComponent.class_method,
        method_args=[1],
    )

    assert result == 4


@pytest.mark.asyncio
async def test_safe_run_and_log_rejects_free_function():
    comp = DemoComponent(NoOpLogger())

    with pytest.raises(TypeError, match="require a method bound to this component instance or its class"):
        await comp._mn_safe_run_and_log_failure(free_function, method_args=[1])


@pytest.mark.asyncio
async def test_safe_run_and_log_rejects_static_method():
    comp = DemoComponent(NoOpLogger())

    with pytest.raises(TypeError, match="require a method bound to this component instance or its class"):
        await comp._mn_safe_run_and_log_failure(
            DemoComponent.static_method,
            method_args=[1],
        )


@pytest.mark.asyncio
async def test_safe_run_and_log_rejects_method_bound_to_other_component():
    comp = DemoComponent(NoOpLogger())
    other = OtherComponent(NoOpLogger())

    with pytest.raises(TypeError, match="require a method bound to this component instance or its class"):
        await comp._mn_safe_run_and_log_failure(
            other.sync_instance_method,
            method_args=[1],
        )


@pytest.mark.asyncio
async def test_safe_run_and_log_failure_logs_default_message_and_rel_modpath():
    logger = InMemoryLogger()
    comp = FailingComponent(logger)

    result = await comp._mn_safe_run_and_log_failure(comp.boom)

    assert result is None
    assert logger.logs[-1].msg == "FailingComponent.boom failed (tests/minions/_internal/_framework/test_async_component.py)"
    assert str(logger.logs[-1].kwargs["rel_modpath"]) == "tests/minions/_internal/_framework/test_async_component.py"


@pytest.mark.asyncio
async def test_safe_run_and_log_failure_accepts_log_message_override():
    logger = InMemoryLogger()
    comp = FailingComponent(logger)

    result = await comp._mn_safe_run_and_log_failure(
        comp.boom,
        log_msg="Builder-facing failure message",
    )

    assert result is None
    assert logger.logs[-1].msg == "Builder-facing failure message"
    assert str(logger.logs[-1].kwargs["rel_modpath"]) == "tests/minions/_internal/_framework/test_async_component.py"


@pytest.mark.asyncio
async def test_safe_run_and_log_failure_rel_modpath_overrides_log_kwargs_collision():
    logger = InMemoryLogger()
    comp = FailingComponent(logger)

    result = await comp._mn_safe_run_and_log_failure(
        comp.boom,
        log_kwargs={"rel_modpath": "wrong/path.py"},
    )

    assert result is None
    assert str(logger.logs[-1].kwargs["rel_modpath"]) == "tests/minions/_internal/_framework/test_async_component.py"


@pytest.mark.asyncio
async def test_run_and_log_failure_returns_successful_result():
    comp = DemoComponent(NoOpLogger())

    result = await comp._mn_run_and_log_failure(
        comp.async_instance_method,
        method_args=[1],
    )

    assert result == 3


@pytest.mark.asyncio
async def test_run_and_log_failure_logs_and_reraises_failure():
    logger = InMemoryLogger()
    comp = FailingComponent(logger)

    with pytest.raises(RuntimeError, match="boom"):
        await comp._mn_run_and_log_failure(comp.boom)

    assert (
        logger.logs[-1].msg
        == "FailingComponent.boom failed (tests/minions/_internal/_framework/test_async_component.py)"
    )
    assert logger.logs[-1].kwargs["error_type"] == "RuntimeError"
    assert logger.logs[-1].kwargs["error_message"] == "boom"
    assert (
        str(logger.logs[-1].kwargs["rel_modpath"])
        == "tests/minions/_internal/_framework/test_async_component.py"
    )


@pytest.mark.asyncio
async def test_run_and_log_failure_accepts_log_message_and_kwargs():
    logger = InMemoryLogger()
    comp = FailingComponent(logger)

    with pytest.raises(RuntimeError, match="boom"):
        await comp._mn_run_and_log_failure(
            comp.boom,
            log_msg="Builder-facing failure message",
            log_kwargs={"operation": "demo"},
        )

    assert logger.logs[-1].msg == "Builder-facing failure message"
    assert logger.logs[-1].kwargs["operation"] == "demo"
    assert (
        str(logger.logs[-1].kwargs["rel_modpath"])
        == "tests/minions/_internal/_framework/test_async_component.py"
    )
