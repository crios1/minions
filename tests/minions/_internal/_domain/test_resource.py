# pyright: reportUnusedClass=false

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
