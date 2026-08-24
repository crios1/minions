"""Ordinary private names belong to user code; Minions-private members use `_mn_`."""

from minions import Minion, Pipeline, Resource
from minions._internal._framework.async_component import AsyncComponent
from minions._internal._framework.async_service import AsyncService
from minions._internal._framework.logger import Logger
from minions._internal._framework.logger_backed_async_component import (
    LoggerBackedAsyncComponent,
)
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.state_store import StateStore


def test_user_extensible_component_classes_do_not_define_user_private_attributes() -> None:
    user_extensible_component_classes = [
        AsyncComponent,
        LoggerBackedAsyncComponent,
        AsyncService,
        Minion,
        Pipeline,
        Resource,
        Logger,
        Metrics,
        StateStore,
    ]

    bad: dict[str, list[str]] = {}
    for cls in user_extensible_component_classes:
        names = {**cls.__dict__, **getattr(cls, "__annotations__", {})}
        private_names = sorted(
            name
            for name in names
            if isinstance(name, str)
            and name
            and name.startswith("_")
            and not name.startswith("__")
            and not name.startswith("_mn_")
            and name != "_abc_impl"
        )
        if private_names:
            bad[f"{cls.__module__}.{cls.__qualname__}"] = private_names

    assert bad == {}
