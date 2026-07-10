# pyright: reportUnusedClass=false

from dataclasses import is_dataclass

import msgspec

from minions import Minion, Resource, minion_step
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.mixin import Mixin


def test_accepts_dataclass_and_msgspec_struct_event_and_context_types():
    assert is_dataclass(SimpleEvent)
    assert is_dataclass(SimpleContext)
    assert issubclass(EmptyEvent, msgspec.Struct)
    assert issubclass(EmptyContext, msgspec.Struct)

    class DataclassEventDataclassContextMinion(Minion[SimpleEvent, SimpleContext]): ...

    class DataclassEventMsgspecStructContextMinion(Minion[SimpleEvent, EmptyContext]): ...

    class MsgspecStructEventDataclassContextMinion(Minion[EmptyEvent, SimpleContext]): ...

    class MsgspecStructEventMsgspecStructContextMinion(Minion[EmptyEvent, EmptyContext]): ...


def test_distinct_resource_dependencies():
    class MyResource1(Resource): ...

    class MyResource2(Resource): ...

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        r1: MyResource1
        r2: MyResource2


def test_mixins_allowed_in_any_order():
    class MyMinion1(Mixin, Minion[EmptyEvent, EmptyContext]): ...

    class MyMinion2(Minion[EmptyEvent, EmptyContext], Mixin): ...


def test_minion_workflow_created_properly():
    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self): ...
        @minion_step
        async def step_2(self): ...
        async def not_a_step(self): ...

    assert MyMinion._mn_workflow_spec
    assert len(MyMinion._mn_workflow_spec) == 2
    assert MyMinion._mn_workflow_spec == ("step_1", "step_2")

    m = MyMinion(
        "",
        "",
        "",
        "",
        NoOpStateStore(),
        NoOpMetrics(),
        NoOpLogger(),
        minion_id="test-minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )

    assert len(m._mn_workflow) == 2
