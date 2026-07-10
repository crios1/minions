# pyright: reportUnusedClass=false

import asyncio
import os
import sys

import pytest

from minions import Resource
from minions._internal._domain.exceptions import UnsupportedUserCode


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
