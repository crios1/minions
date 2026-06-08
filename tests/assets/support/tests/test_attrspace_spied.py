import pytest

from minions._internal._domain.exceptions import UnsupportedUserCode
from minions._internal._domain.minion import Minion
from minions._internal._domain.minion_step import minion_step
from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.resource import Resource
from minions._internal._framework.async_component import AsyncComponent
from minions._internal._framework.async_lifecycle import AsyncLifecycle
from minions._internal._framework.async_service import AsyncService
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.state_store import StateStore
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource


def test_spied_minion_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[RecordEvent, CounterContext]):  # pyright: ignore[reportUnusedClass]
            _mn_bad_cls_atrr = 1


def test_spied_minion_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[RecordEvent, CounterContext]):  # pyright: ignore[reportUnusedClass]
            async def _mn_bad_method(self):
                ...


def test_spied_pipeline_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[RecordEvent]):  # pyright: ignore[reportUnusedClass]
            _mn_bad_cls_atrr = 1


def test_spied_pipeline_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[RecordEvent]):  # pyright: ignore[reportUnusedClass]
            async def _mn_bad_method(self):
                ...


def test_spied_resource_attrspace_rejects_mn_class_attr():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):  # pyright: ignore[reportUnusedClass]
            _mn_bad_cls_atrr = 1


def test_spied_resource_attrspace_rejects_mn_method_name():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):  # pyright: ignore[reportUnusedClass]
            async def _mn_bad_method(self):
                ...


def test_spied_minion_attrspace_rejects_mn_assignment_in_lifecycle_hook():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[RecordEvent, CounterContext]):  # pyright: ignore[reportUnusedClass]
            async def startup(self):
                self._mn_bad_attr = 1


def test_spied_minion_attrspace_rejects_mn_assignment_in_workflow_step():
    with pytest.raises(UnsupportedUserCode):
        class BadMinion(SpiedMinion[RecordEvent, CounterContext]):  # pyright: ignore[reportUnusedClass]
            @minion_step
            async def step(self):
                self._mn_bad_attr = 1


def test_spied_pipeline_attrspace_rejects_mn_assignment_in_lifecycle_hook():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[RecordEvent]):  # pyright: ignore[reportUnusedClass]
            async def startup(self):
                self._mn_bad_attr = 1


def test_spied_pipeline_attrspace_rejects_mn_assignment_in_emit_event_loop():
    with pytest.raises(UnsupportedUserCode):
        class BadPipeline(SpiedPipeline[RecordEvent]):  # pyright: ignore[reportUnusedClass]
            async def run(self):
                self._mn_bad_attr = 1


def test_spied_resource_attrspace_rejects_mn_assignment_in_lifecycle_hook():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):  # pyright: ignore[reportUnusedClass]
            async def startup(self):
                self._mn_bad_attr = 1


def test_spied_resource_attrspace_rejects_mn_assignment_in_user_method():
    with pytest.raises(UnsupportedUserCode):
        class BadResource(SpiedResource):  # pyright: ignore[reportUnusedClass]
            async def fetch(self):
                self._mn_bad_attr = 1


def test_shipped_domain_classes_do_not_use_user_private_attrspace():
    shipped_classes = [
        AsyncLifecycle,
        AsyncComponent,
        AsyncService,
        Minion,
        Pipeline,
        Resource,
        Logger,
        Metrics,
        StateStore,
    ]

    bad: dict[str, list[str]] = {}
    for cls in shipped_classes:
        names = {**cls.__dict__, **getattr(cls, "__annotations__", {})}
        private_names = sorted(
            name
            for name in names
            if isinstance(name, str)
            and name
            and not name[0].isalpha()
            and not name.startswith("__")
            and not name.startswith("_mn_")
            and name != "_abc_impl"
        )
        if private_names:
            bad[f"{cls.__module__}.{cls.__qualname__}"] = private_names

    assert bad == {}
