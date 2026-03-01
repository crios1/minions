import pytest
from dataclasses import dataclass
from typing import TypeVar
from minions import Minion, Resource, minion_step

from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore

from tests.assets.support.mixin import Mixin

# compositional validation happens on instantiation of each domain object
# so each domain object has it's own test

T_E = TypeVar('T_E')
T_C = TypeVar('T_C')

@dataclass
class MyEvent:
    ts: int

@dataclass
class MyContext:
    ts: int

class TestMinionSubclassingValid:
    def test_valid_event_and_context_types(self):
        class MyMinion1(Minion[dict, dict]):
            ...
        class MyMinion2(Minion[MyEvent, MyContext]):
            ...

    def test_distinct_resource_dependencies(self):
        class MyResource1(Resource):
            ...
        class MyResource2(Resource):
            ...
        class MyMinion(Minion[dict, dict]):
            r1: MyResource1
            r2: MyResource2
 
    def test_mixins_allowed_in_any_order(self):
        'mixins are used in testing suite, not intended for end user use'
        class MyMinion1(Mixin, Minion[MyEvent, MyContext]):
            ...
        class MyMinion2(Minion[MyEvent, MyContext], Mixin):
            ...

    def test_minion_workflow_created_properly(self):
        class MyMinion(Minion[MyEvent, MyContext]):
            @minion_step
            async def step_1(self):
                ...
            @minion_step
            async def step_2(self):
                ...
            async def not_a_step(self):
                ...
        assert MyMinion._mn_workflow_spec
        assert len(MyMinion._mn_workflow_spec) == 2
        assert MyMinion._mn_workflow_spec == ('step_1', 'step_2')

        m = MyMinion('','','','',NoOpStateStore(),NoOpMetrics(),NoOpLogger())

        assert len(m._mn_workflow) == 2

class TestMinionSubclassingInvalid:
    def test_missing_event_and_context_types(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion):
                ...
        assert str(excinfo.value) == (
            "MyMinion must declare both event and workflow context types. "
            "Example: class MyMinion(Minion[MyPipelineEvent, MyWorkflowCtx])"
        )

    def test_invalid_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[int, MyContext]):
                ...
        assert str(excinfo.value) == "MyMinion: event type must be a structured type, not a primitive"

    def test_invalid_context_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, int]):
                ...
        assert str(excinfo.value) == "MyMinion: workflow context type must be a structured type, not a primitive"

    def test_invalid_event_and_context_types(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[int, int]):
                ...
        assert str(excinfo.value) == "MyMinion: event type must be a structured type, not a primitive"
    
    def test_reject_multiple_minion_bases(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, MyContext], Minion[dict, dict]): # type: ignore
                ...
        assert str(excinfo.value) == "duplicate base class Minion"
    
    def test_reject_subclassing_minion_subclasses(self):
        class MinionSub(Minion[MyEvent, MyContext]):
            ...
        with pytest.raises(TypeError) as excinfo:
            class MinionSubSub(MinionSub):
                ...
        assert str(excinfo.value) == (
            "MinionSubSub must subclass Minion directly. "
            "Subclasses of Minion subclasses are not supported."
        )

    def test_invalid_name(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[dict, dict]):
                name = set('invalid_name') # only kebab-case is valid
            MyMinion(
                minion_instance_id="mock",
                minion_composite_key="mock",
                minion_modpath="mock",
                config_path="mock",
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics(),
                logger=NoOpLogger()
            )
        assert str(excinfo.value) == "MyMinion.name must be a string, got set"

    def test_duplicate_resource_dependency(self):
        class MyResource(Resource):
            ...
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[dict, dict]):
                r1: MyResource
                r2: MyResource
        resource_id = f"{MyResource.__module__}.{MyResource.__name__}"
        assert str(excinfo.value) == (
            "MyMinion declares multiple class attributes with the same Resource type: "
            f"{resource_id} -> ['r1', 'r2']. "
            "Define only one class-level Resource per Resource type."
        )

    def test_reject_direct_step_to_step_call_on_self(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, MyContext]):
                @minion_step
                async def step_1(self):
                    await self.step_2()

                @minion_step
                async def step_2(self):
                    ...

        assert str(excinfo.value) == (
            "MyMinion.step_1 cannot call workflow step 'step_2'; "
            "minion steps must be orchestrated only by the runtime workflow engine."
        )
