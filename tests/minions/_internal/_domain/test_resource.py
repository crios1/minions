# pyright: reportUnusedClass=false

import asyncio
import os
import sys

import pytest

from minions import Resource
from minions._internal._domain.exceptions import UnsupportedUserCode
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics


def test_lifecycle_hooks_are_not_wrapped_as_resource_methods(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    class MyResource(Resource):
        async def startup(self) -> None:
            pass

        async def run(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

        async def request(self) -> None:
            pass

    resource = MyResource(logger, metrics, "dummy-path", "dummy-id")

    resource._mn_validate_and_wrap_public_async_methods()

    assert "startup" not in vars(resource)
    assert "run" not in vars(resource)
    assert "shutdown" not in vars(resource)
    assert "request" in vars(resource)


def test_subclass_rejects_reserved_class_variable_name():
    with pytest.raises(UnsupportedUserCode):
        class MyResource(Resource):
            _mn_cls_var = 0


def test_subclass_rejects_reserved_method_name():
    with pytest.raises(UnsupportedUserCode):
        class MyResource(Resource):
            def _mn_method(self):
                ...


def test_untracked_decorator_rejects_sync_method():
    with pytest.raises(
        TypeError,
        match="@untracked must be used on async functions, got: sync_do",
    ):
        class MyResource(Resource):
            @Resource.untracked # pyright: ignore[reportArgumentType]
            def sync_do(self):
                ...


def test_subclass_rejects_asyncio_create_task_in_method():
    with pytest.raises(
        UnsupportedUserCode,
        match=r"Unsupported use of `asyncio\.create_task`",
    ):
        class MyResource(Resource):
            async def sync_do(self):
                import asyncio

                async def async_do():
                    ...

                asyncio.create_task(async_do())


def test_subclass_rejects_asyncio_ensure_future_in_method():
    with pytest.raises(
        UnsupportedUserCode,
        match=r"Unsupported use of `asyncio\.ensure_future`",
    ):

        class MyResource(Resource):
            async def do(self):
                async def async_do(): ...

                asyncio.ensure_future(async_do())


def test_subclass_rejects_sys_exit_in_method():
    with pytest.raises(
        UnsupportedUserCode,
        match=r"Unsupported use of `sys\.exit`",
    ):

        class MyResource(Resource):
            async def do(self):
                sys.exit()


def test_subclass_rejects_os_exit_in_method():
    with pytest.raises(
        UnsupportedUserCode,
        match=r"Unsupported use of `os\._exit`",
    ):

        class MyResource(Resource):
            async def do(self):
                os._exit(1)


def test_subclass_rejects_indirect_reserved_attribute_assignment():
    with pytest.raises(
        UnsupportedUserCode,
        match=r"Invalid attribute assignment: `self\._mn_value`",
    ):

        class MyResource(Resource):
            async def do(self):
                setattr(self, "_mn_value", 1)
