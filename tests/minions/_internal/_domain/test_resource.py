# pyright: reportUnusedClass=false

import asyncio
import contextlib
import os
import sys
from collections.abc import Callable

import pytest

from minions import Resource
from minions._internal._domain.exceptions import UnsupportedUserCode
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics


def test_lifecycle_hooks_are_not_wrapped_as_resource_methods():
    class MyResource(Resource):
        async def startup(self):
            pass

        async def run(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

        async def request(self) -> None:
            pass

    resource = MyResource(NoOpLogger(), NoOpMetrics(), "dummy-path", "dummy-id")

    resource._mn_validate_and_wrap_public_async_methods()

    assert "startup" not in vars(resource)
    assert "run" not in vars(resource)
    assert "shutdown" not in vars(resource)
    assert "request" in vars(resource)


@pytest.mark.asyncio
async def test_method_failure_log_contains_only_safe_argument_metadata_for_fixed_signature(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    running_resource_context: Callable[
        [Resource], contextlib.AbstractAsyncContextManager[Resource]
    ],
):
    class AuthResource(Resource):
        async def authenticate(self, access_token: str, *, account: str) -> bool:
            raise PermissionError("upstream rejected credentials")

    resource = AuthResource(
        logger,
        metrics,
        "tests.resources.AuthResource",
        "auth-resource",
    )

    async with running_resource_context(resource):
        with pytest.raises(PermissionError, match="upstream rejected credentials"):
            await resource.authenticate(
                "bearer-prod-7f4c-secret",
                account="customer-4821",
            )

    failure_log = logger.find_first_log("Resource method failed")
    assert failure_log is not None
    assert failure_log.kwargs["resource_id"] == "auth-resource"
    assert failure_log.kwargs["resource_method"] == "authenticate"
    assert failure_log.kwargs["error_type"] == "PermissionError"
    assert failure_log.kwargs["resource_arguments"] == [
        {"kind": "positional", "name": "access_token", "type": "str"},
        {"kind": "keyword", "name": "account", "type": "str"},
    ]
    rendered_kwargs = repr(failure_log.kwargs)
    assert "bearer-prod-7f4c-secret" not in rendered_kwargs
    assert "customer-4821" not in rendered_kwargs


@pytest.mark.asyncio
async def test_method_failure_log_contains_only_safe_argument_metadata_for_variadic_signature(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    running_resource_context: Callable[
        [Resource], contextlib.AbstractAsyncContextManager[Resource]
    ],
):
    class LookupResource(Resource):
        async def lookup(self, query: str, *tokens: str, **options: object) -> None:
            raise RuntimeError("lookup failed")

    resource = LookupResource(
        logger,
        metrics,
        "tests.resources.LookupResource",
        "lookup-resource",
    )

    async with running_resource_context(resource):
        with pytest.raises(RuntimeError, match="lookup failed"):
            await resource.lookup(
                "customer-search",
                "bearer-prod-7f4c-secret",
                "session-prod-9a2d-secret",
                account="customer-4821",
                timeout=30,
            )

    failure_log = logger.find_first_log("Resource method failed")
    assert failure_log is not None
    assert failure_log.kwargs["resource_id"] == "lookup-resource"
    assert failure_log.kwargs["resource_method"] == "lookup"
    assert failure_log.kwargs["error_type"] == "RuntimeError"
    assert failure_log.kwargs["resource_arguments"] == [
        {"kind": "positional", "name": "query", "type": "str"},
        {"kind": "positional", "name": "tokens", "type": "str"},
        {"kind": "positional", "name": "tokens", "type": "str"},
        {"kind": "keyword", "name": "account", "type": "str"},
        {"kind": "keyword", "name": "timeout", "type": "int"},
    ]
    rendered_kwargs = repr(failure_log.kwargs)
    assert "customer-search" not in rendered_kwargs
    assert "bearer-prod-7f4c-secret" not in rendered_kwargs
    assert "session-prod-9a2d-secret" not in rendered_kwargs
    assert "customer-4821" not in rendered_kwargs


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
