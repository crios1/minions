# pyright: reportUnusedClass=false

import asyncio
from typing import Any, TypedDict

import pytest

from minions import Minion, Resource, minion_step, resource_id
from minions._internal._domain.component_identity import generate_component_id
from minions._internal._domain.exceptions import UnsupportedUserCode
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from minions._internal._utils.serialization import SERIALIZABLE_PRIMITIVE_TYPES
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent


def test_missing_event_and_context_types():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion):  # pyright: ignore[reportMissingTypeArgument]
            ...

    assert str(excinfo.value) == (
        "MyMinion must declare both event and workflow context types. "
        "Example: class MyMinion(Minion[MyPipelineEvent, MyWorkflowCtx])"
    )


@pytest.mark.parametrize("event_type", SERIALIZABLE_PRIMITIVE_TYPES)
def test_reject_primitive_event_type(event_type: type[object]):
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[event_type, EmptyContext]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


@pytest.mark.parametrize("context_type", SERIALIZABLE_PRIMITIVE_TYPES)
def test_reject_primitive_context_type(context_type: type[object]):
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, context_type]): ...

    assert str(excinfo.value) == (
        "MyMinion: workflow context type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_invalid_event_and_context_types():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[int, int]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_any_event_type():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[Any, EmptyContext]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_any_context_type():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, Any]): ...

    assert str(excinfo.value) == (
        "MyMinion: workflow context type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_any_event_and_context_types():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[Any, Any]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_bare_dict_event_type():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[dict, EmptyContext]):  # pyright: ignore[reportMissingTypeArgument]
            ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_bare_dict_context_type():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, dict]):  # pyright: ignore[reportMissingTypeArgument]
            ...

    assert str(excinfo.value) == (
        "MyMinion: workflow context type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_parameterized_dict_event_type():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[dict[str, int], EmptyContext]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_typed_dict_event_type():
    class MyTypedDictEvent(TypedDict):
        pass

    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[MyTypedDictEvent, EmptyContext]): ...

    assert str(excinfo.value) == (
        "MyMinion: event type is not supported. "
        "Supported user-declared types: (dataclass, msgspec.Struct)."
    )


def test_reject_subclassing_minion_subclasses():
    class MinionSub(Minion[EmptyEvent, EmptyContext]): ...

    with pytest.raises(TypeError) as excinfo:

        class MinionSubSub(MinionSub): ...

    assert str(excinfo.value) == (
        "MinionSubSub must subclass Minion directly. "
        "Subclasses of Minion subclasses are not supported."
    )


def test_reject_minion_step_on_staticmethod():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @staticmethod
            @minion_step
            async def step_1(): ...

    assert str(excinfo.value) == (
        "MyMinion.step_1: @minion_step must decorate an **instance** method, not a staticmethod."
    )


def test_reject_staticmethod_decorated_with_minion_step():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            @staticmethod
            async def step_1(): ...

    assert str(excinfo.value) == "minion_step must decorate async functions, got: step_1"


def test_reject_minion_step_on_classmethod():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @classmethod
            @minion_step
            async def step_1(cls): ...

    assert str(excinfo.value) == (
        "MyMinion.step_1: @minion_step must decorate an **instance** method, not a classmethod."
    )


def test_reject_classmethod_decorated_with_minion_step():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            @classmethod
            async def step_1(cls): ...

    assert str(excinfo.value) == "minion_step must decorate async functions, got: step_1"


def test_reject_instantiating_minion_without_steps():
    class MyMinion(Minion[EmptyEvent, EmptyContext]): ...

    with pytest.raises(TypeError) as excinfo:
        MyMinion(
            "minion-instance-id",
            "orchestration-id",
            "tests.MyMinion",
            None,
            NoOpStateStore(),
            NoOpMetrics(),
            NoOpLogger(),
            minion_id="minion-id",
            minion_config_id="minion-config-id",
            pipeline_id="pipeline-id",
        )

    assert str(excinfo.value) == (
        "No @minion_step methods found in MyMinion. "
        "Define at least one step to form a valid Minion subclass."
    )


def test_duplicate_resource_dependency():
    resource_component_id = generate_component_id()

    @resource_id(resource_component_id)
    class MyResource(Resource): ...

    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            r1: MyResource
            r2: MyResource

    assert str(excinfo.value) == (
        "MyMinion declares multiple class attributes with the same Resource type: "
        f"{resource_component_id} -> ['r1', 'r2']. "
        "Define only one class-level Resource per Resource type."
    )


def test_reject_direct_step_to_step_call_on_self():
    with pytest.raises(TypeError) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                await self.step_2()

            @minion_step
            async def step_2(self): ...

    assert str(excinfo.value) == (
        "MyMinion.step_1 cannot call workflow step 'step_2'; "
        "minion steps must be orchestrated only by the runtime workflow engine."
    )


def test_reject_direct_raise_asyncio_cancelled_error_in_step():
    with pytest.raises(UnsupportedUserCode) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                raise asyncio.CancelledError()

    assert "Unsupported use of `raise asyncio.CancelledError`" in str(excinfo.value)
    assert "raise AbortWorkflow instead" in str(excinfo.value)


def test_reject_direct_raise_cancelled_error_name_in_step():
    from asyncio import CancelledError

    with pytest.raises(UnsupportedUserCode) as excinfo:

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                raise CancelledError()

    assert "Unsupported use of `raise CancelledError`" in str(excinfo.value)
    assert "raise AbortWorkflow instead" in str(excinfo.value)
