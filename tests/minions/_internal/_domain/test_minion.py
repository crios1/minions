# pyright: reportUnusedClass=false

import asyncio
from dataclasses import FrozenInstanceError, dataclass
from typing import Any, TypedDict, TypeVar

import msgspec
import pytest

from minions import Minion, Resource, minion_step
from minions._internal._domain.exceptions import UnsupportedUserCode
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from minions._internal._utils.serialization import SERIALIZABLE_PRIMITIVE_TYPES
from minions.types import MinionWorkflowHandle
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.mixin import Mixin
from tests.assets.support.state_store_inmemory import InMemoryStateStore

# compositional validation happens on instantiation of each domain object
# so each domain object has it's own test

T_E = TypeVar("T_E")
T_C = TypeVar("T_C")


@dataclass
class MyEvent:
    ts: int


@dataclass
class MyContext:
    ts: int


@dataclass
class EmptyContext:
    pass


class MyStructEvent(msgspec.Struct):
    ts: int


class MyStructContext(msgspec.Struct):
    ts: int


class MyTypedDictEvent(TypedDict):
    ts: int


class TestMinionSubclassingValid:
    def test_valid_event_and_context_types(self):
        class MyMinion1(Minion[MyEvent, MyContext]):
            ...
        class MyMinion2(Minion[MyStructEvent, MyStructContext]):
            ...

    def test_distinct_resource_dependencies(self):
        class MyResource1(Resource):
            ...
        class MyResource2(Resource):
            ...
        class MyMinion(Minion[MyEvent, MyContext]):
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
        assert MyMinion._mn_workflow_spec == ("step_1", "step_2")

        m = MyMinion("", "", "", "", NoOpStateStore(), NoOpMetrics(), NoOpLogger())

        assert len(m._mn_workflow) == 2

    def test_minion_without_config_does_not_expose_config(self):
        class MyMinion(Minion[MyEvent, MyContext]):
            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        assert not hasattr(m, "config")

    @pytest.mark.asyncio
    async def test_workflow_handle_is_available_during_workflow_steps(self):
        captured_handles: list[MinionWorkflowHandle] = []

        class MyMinion(Minion[MyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                captured_handles.append(self.workflow_handle)

        m = MyMinion(
            minion_instance_id="instance-1",
            orchestration_id="orchestration-1",
            minion_module_path="mock",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )
        m._mn_started.set()

        await m._mn_handle_event(MyEvent(ts=1))
        await m._mn_wait_until_tasks_idle(timeout=1.0, timeout_msg="workflow did not finish")

        assert len(captured_handles) == 1
        assert captured_handles[0].orchestration_id == "orchestration-1"
        assert isinstance(captured_handles[0].workflow_id, str)
        assert captured_handles[0].workflow_id

    @pytest.mark.asyncio
    async def test_workflow_handle_values_match_framework_log_and_state_identity(self):
        logger = InMemoryLogger()
        state_store = InMemoryStateStore(logger=logger)

        captured_handles: list[MinionWorkflowHandle] = []
        captured_stored_contexts: list[Any] = []

        class MyMinion(Minion[MyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                captured_handles.append(self.workflow_handle)
                captured_stored_contexts.extend(await state_store.get_all_contexts())

        m = MyMinion(
            minion_instance_id="instance-1",
            orchestration_id="orchestration-1",
            minion_module_path="mock",
            config_path=None,
            state_store=state_store,
            metrics=NoOpMetrics(),
            logger=logger,
        )
        m._mn_started.set()

        await m._mn_handle_event(MyEvent(ts=1))
        await m._mn_wait_until_tasks_idle(timeout=1.0, timeout_msg="workflow did not finish")

        assert len(captured_handles) == 1
        handle = captured_handles[0]
        assert captured_stored_contexts
        assert all(
            stored.orchestration_id == handle.orchestration_id
            for stored in captured_stored_contexts
        )
        assert all(stored.workflow_id == handle.workflow_id for stored in captured_stored_contexts)
        assert await state_store.get_all_contexts() == []
        workflow_started = next(log for log in logger.logs if log.msg == "Workflow started")
        assert workflow_started.kwargs["orchestration_id"] == handle.orchestration_id
        assert workflow_started.kwargs["workflow_id"] == handle.workflow_id

    def test_workflow_handle_is_read_only(self):
        handle = MinionWorkflowHandle(
            orchestration_id="orchestration-1",
            workflow_id="workflow-1",
        )

        with pytest.raises(FrozenInstanceError):
            handle.workflow_id = "workflow-2"  # pyright: ignore[reportAttributeAccessIssue]

    def test_workflow_handle_outside_active_workflow_raises_clear_error(self):
        class MyMinion(Minion[MyEvent, MyContext]):
            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="instance-1",
            orchestration_id="orchestration-1",
            minion_module_path="mock",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        with pytest.raises(
            RuntimeError,
            match="No workflow handle is currently bound to this workflow",
        ):
            m.workflow_handle

    @pytest.mark.asyncio
    async def test_minion_binds_loaded_config_to_declared_attribute(self):
        @dataclass
        class MyConfig:
            name: str

        class MyMinion(Minion[MyEvent, MyContext]):
            config: MyConfig

            async def load_config(self, config_path: str) -> MyConfig:
                return MyConfig(name=config_path)

            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path="mock",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        await m._mn_load_config("alpha")
        assert m.config == MyConfig(name="alpha")

    @pytest.mark.asyncio
    async def test_minion_requires_config_attribute_annotation(self):
        @dataclass
        class MyConfig:
            name: str

        class MyMinion(Minion[MyEvent, MyContext]):
            async def load_config(self, config_path: str) -> MyConfig:
                return MyConfig(name=config_path)

            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path="mock",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        with pytest.raises(
            TypeError,
            match="MyMinion must declare a `config` type annotation",
        ):
            await m._mn_load_config("mock")

    @pytest.mark.asyncio
    async def test_minion_rejects_config_type_mismatch(self):
        @dataclass
        class DeclaredConfig:
            name: str

        @dataclass
        class LoadedConfig:
            name: str

        class MyMinion(Minion[MyEvent, MyContext]):
            config: DeclaredConfig

            async def load_config(self, config_path: str) -> LoadedConfig:
                return LoadedConfig(name=config_path)

            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path="mock",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        with pytest.raises(
            TypeError,
            match="MyMinion.config expects .*DeclaredConfig.* got LoadedConfig",
        ):
            await m._mn_load_config("mock")

    @pytest.mark.asyncio
    async def test_minion_rejects_non_model_config_loads(self):
        class MyMinion(Minion[MyEvent, MyContext]):
            async def load_config(self, config_path: str) -> object:
                return {"name": "alpha"}

            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path="mock",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        with pytest.raises(
            TypeError,
            match="MyMinion.load_config: config type must be a dataclass or msgspec Struct type.",
        ):
            await m._mn_load_config("mock")

    @pytest.mark.asyncio
    async def test_minion_requires_file_config_loader_override(self):
        class MyMinion(Minion[MyEvent, MyContext]):
            @minion_step
            async def step_1(self):
                ...

        m = MyMinion(
            minion_instance_id="mock",
            orchestration_id="mock",
            minion_module_path="mock",
            config_path="mock",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
        )

        with pytest.raises(
            NotImplementedError,
            match="MyMinion.load_config must be overridden",
        ):
            await m.load_config("mock")


class TestMinionSubclassingInvalid:
    def test_missing_event_and_context_types(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion):  # pyright: ignore[reportMissingTypeArgument]
                ...
        assert str(excinfo.value) == (
            "MyMinion must declare both event and workflow context types. "
            "Example: class MyMinion(Minion[MyPipelineEvent, MyWorkflowCtx])"
        )

    @pytest.mark.parametrize("event_type", SERIALIZABLE_PRIMITIVE_TYPES)
    def test_reject_primitive_event_type(self, event_type: type[object]):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[event_type, MyContext]):
                ...
        assert str(excinfo.value) == (
            "MyMinion: event type must be a structured type, not a primitive"
        )

    @pytest.mark.parametrize("context_type", SERIALIZABLE_PRIMITIVE_TYPES)
    def test_reject_primitive_context_type(self, context_type: type[object]):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, context_type]):
                ...
        assert str(excinfo.value) == (
            "MyMinion: workflow context type must be a structured type, not a primitive"
        )

    def test_invalid_event_and_context_types(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[int, int]):
                ...
        assert str(excinfo.value) == (
            "MyMinion: event type must be a structured type, not a primitive"
        )

    def test_reject_bare_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[dict, MyContext]):  # pyright: ignore[reportMissingTypeArgument]
                ...
        assert str(excinfo.value) == (
            "MyMinion: event type must be a dataclass or msgspec Struct type."
        )

    def test_reject_bare_dict_context_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, dict]):  # pyright: ignore[reportMissingTypeArgument]
                ...
        assert str(excinfo.value) == (
            "MyMinion: workflow context type must be a dataclass or msgspec Struct type."
        )

    def test_reject_parameterized_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[dict[str, int], MyContext]):
                ...
        assert str(excinfo.value) == (
            "MyMinion: event type must be a dataclass or msgspec Struct type."
        )

    def test_reject_typed_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyTypedDictEvent, MyContext]):
                ...
        assert str(excinfo.value) == (
            "MyMinion: event type must be a dataclass or msgspec Struct type."
        )
    
    def test_reject_multiple_minion_bases(self):
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, MyContext], Minion[MyStructEvent, MyStructContext]): # type: ignore
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

    def test_duplicate_resource_dependency(self):
        class MyResource(Resource):
            ...
        with pytest.raises(TypeError) as excinfo:
            class MyMinion(Minion[MyEvent, MyContext]):
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

    def test_reject_direct_raise_asyncio_cancelled_error_in_step(self):
        with pytest.raises(UnsupportedUserCode) as excinfo:

            class MyMinion(Minion[MyEvent, MyContext]):
                @minion_step
                async def step_1(self):
                    raise asyncio.CancelledError()

        assert "Unsupported use of `raise asyncio.CancelledError`" in str(excinfo.value)
        assert "raise AbortWorkflow instead" in str(excinfo.value)

    def test_reject_direct_raise_cancelled_error_name_in_step(self):
        from asyncio import CancelledError

        with pytest.raises(UnsupportedUserCode) as excinfo:
            class MyMinion(Minion[MyEvent, MyContext]):
                @minion_step
                async def step_1(self):
                    raise CancelledError()

        assert "Unsupported use of `raise CancelledError`" in str(excinfo.value)
        assert "raise AbortWorkflow instead" in str(excinfo.value)
