from dataclasses import dataclass
from typing import Any

import pytest
import msgspec

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    _normalize_workflow_context_data,
    CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
    PersistedMinionWorkflowContext,
    WorkflowContextData,
    WorkflowContextSchemaError,
    decode_persisted_workflow_context_typed,
    deserialize_workflow_context_blob,
    deserialize_workflow_context,
    serialize_persisted_workflow_context,
    serialize_workflow_context,
)
from minions._internal._framework.state_store_payload_types import StateStorePayload
from minions._internal._utils.serialization import deserialize, serialize


@dataclass
class EventDC:
    value: int


@dataclass
class ContextDC:
    count: int = 0


class EventStruct(msgspec.Struct):
    value: int


class ContextStruct(msgspec.Struct):
    count: int = 0


@pytest.mark.parametrize(
    ("event", "context", "event_cls", "context_cls"),
    [
        (EventDC(1), ContextDC(2), EventDC, ContextDC),
        (EventStruct(1), ContextStruct(2), EventStruct, ContextStruct),
    ],
)
def test_adapter_payload_roundtrip_restores_typed_event_and_context(
    event: Any,
    context: Any,
    event_cls: type[Any],
    context_cls: type[Any],
) -> None:
    ctx: MinionWorkflowContext[Any, Any] = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-typed-blob",
        event=event,
        context=context,
        context_cls=context_cls,
        next_step_index=1,
    )

    encoded_adapter_payload = serialize(dict(serialize_workflow_context(ctx)))
    payload = deserialize(encoded_adapter_payload, dict[str, object])
    loaded_ctx = deserialize_workflow_context(payload, event_cls=event_cls)

    assert isinstance(loaded_ctx.event, event_cls)
    assert isinstance(loaded_ctx.context, context_cls)
    assert loaded_ctx == ctx


@pytest.mark.parametrize(
    ("event", "context", "event_cls", "context_cls"),
    [
        (EventDC(1), ContextDC(2), EventDC, ContextDC),
        (EventStruct(1), ContextStruct(2), EventStruct, ContextStruct),
    ],
)
def test_direct_typed_decoder_accepts_persisted_workflow_context(
    event: Any,
    context: Any,
    event_cls: type[Any],
    context_cls: type[Any],
) -> None:
    ctx: MinionWorkflowContext[Any, Any] = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-direct-typed",
        event=event,
        context=context,
        context_cls=context_cls,
        next_step_index=1,
    )

    blob = serialize_persisted_workflow_context(ctx)
    loaded_ctx = decode_persisted_workflow_context_typed(
        blob,
        event_cls=event_cls,
        context_cls=context_cls,
    )

    assert isinstance(loaded_ctx.event, event_cls)
    assert isinstance(loaded_ctx.context, context_cls)
    assert loaded_ctx == ctx


def test_serialize_workflow_context_writes_adapter_shape():
    ctx = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-1",
        event={"v": 1},
        context={"c": 1},
        context_cls=dict,
        next_step_index=2,
    )

    payload = serialize_workflow_context(ctx)

    assert payload["orchestration_id"] == ctx.orchestration_id
    assert payload["context_cls"] == "builtins.dict"
    assert payload["next_step_index"] == 2
    assert payload["schema_version"] == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION


def test_serialize_persisted_workflow_context_writes_blob_contract():
    ctx = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-blob-contract",
        event={"v": 1},
        context={"c": 1},
        context_cls=dict,
        next_step_index=2,
    )

    blob = serialize_persisted_workflow_context(ctx)
    persisted = deserialize(blob, PersistedMinionWorkflowContext)
    loaded_ctx = deserialize_workflow_context_blob(blob)

    assert persisted.workflow_id == ctx.workflow_id
    assert persisted.context_cls == "builtins.dict"
    assert persisted.schema_version == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    assert loaded_ctx == ctx


def test_adapter_payload_roundtrips_msgspec_struct_payloads():
    ctx = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-structs",
        event=EventStruct(1),
        context=ContextStruct(2),
        context_cls=ContextStruct,
        next_step_index=3,
    )

    adapter_payload = serialize_workflow_context(ctx)
    loaded_ctx = deserialize_workflow_context(adapter_payload)

    assert adapter_payload["context_cls"] == f"{ContextStruct.__module__}.{ContextStruct.__qualname__}"
    assert loaded_ctx == ctx


def test_normalize_workflow_context_data_rejects_legacy_unversioned_payload():
    v1_payload: StateStorePayload = {
        "minion_modpath": "tests.assets.minions.sample",
        "workflow_id": "wf-legacy",
        "event": {"v": 1},
        "context": {"c": 1},
        "context_cls": "builtins.dict",
        "step_index": 1,
    }

    with pytest.raises(WorkflowContextSchemaError, match="missing schema_version"):
        _normalize_workflow_context_data(
            {
                **v1_payload,
                "context_cls": dict,
            }
        )

    with pytest.raises(WorkflowContextSchemaError, match="missing schema_version"):
        deserialize_workflow_context(v1_payload)


def test_normalize_workflow_context_data_rejects_future_schema_version():
    payload: WorkflowContextData = {
        "schema_version": 999,
        "orchestration_id": "tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        "minion_modpath": "tests.assets.minions.sample",
        "workflow_id": "wf-future",
        "event": {"v": 1},
        "context": {"c": 1},
        "context_cls": dict,
        "next_step_index": 0,
    }

    with pytest.raises(WorkflowContextSchemaError):
        _normalize_workflow_context_data(payload)


def test_adapter_payload_roundtrips_context_cls_and_schema_version():
    ctx = MinionWorkflowContext(
        orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-storage",
        event={"v": 1},
        context={"c": 1},
        context_cls=dict,
        next_step_index=0,
    )
    adapter_payload = serialize_workflow_context(ctx)
    loaded_ctx = deserialize_workflow_context(adapter_payload)

    assert adapter_payload["context_cls"] == "builtins.dict"
    assert adapter_payload["schema_version"] == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    assert loaded_ctx == ctx


def test_deserialize_workflow_context_rejects_invalid_context_cls_string():
    payload = serialize_workflow_context(
        MinionWorkflowContext(
            orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
            minion_modpath="tests.assets.minions.sample",
            workflow_id="wf-invalid-context-cls",
            event={"v": 1},
            context={"c": 1},
            context_cls=dict,
            next_step_index=0,
        )
    )
    payload["context_cls"] = "not-a-real.module.Class"

    with pytest.raises(WorkflowContextSchemaError, match="Invalid workflow context context_cls"):
        deserialize_workflow_context(payload)


def test_deserialize_workflow_context_accepts_integer_started_at():
    payload = serialize_workflow_context(
        MinionWorkflowContext(
            orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
            minion_modpath="tests.assets.minions.sample",
            workflow_id="wf-int-started-at",
            event={"v": 1},
            context={"c": 1},
            context_cls=dict,
            next_step_index=0,
        )
    )
    payload["started_at"] = 123

    loaded_ctx = deserialize_workflow_context(payload)

    assert loaded_ctx.started_at == 123.0
    assert isinstance(loaded_ctx.started_at, float)


def test_deserialize_workflow_context_rejects_unknown_leftover_fields():
    payload = serialize_workflow_context(
        MinionWorkflowContext(
            orchestration_id="tests.assets.minions.sample|cfg-a|tests.assets.pipelines.sample",
            minion_modpath="tests.assets.minions.sample",
            workflow_id="wf-unknown-leftover",
            event={"v": 1},
            context={"c": 1},
            context_cls=dict,
            next_step_index=0,
        )
    )
    payload["retired_field"] = "unexpected"

    with pytest.raises(WorkflowContextSchemaError, match="Unexpected workflow context fields"):
        deserialize_workflow_context(payload)
