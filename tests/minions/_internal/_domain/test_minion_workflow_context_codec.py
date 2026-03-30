import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    _normalize_workflow_context_data,
    CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
    WorkflowContextSchemaError,
    deserialize_workflow_context,
    serialize_workflow_context,
)
from minions._internal._framework.state_store_payload_types import StateStorePayload


def test_serialize_workflow_context_writes_store_shape():
    ctx = MinionWorkflowContext(
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-1",
        event={"v": 1},
        context={"c": 1},
        context_cls=dict,
        next_step_index=2,
    )

    payload = serialize_workflow_context(ctx)

    assert payload["context_cls"] == "builtins.dict"
    assert payload["next_step_index"] == 2
    assert payload["schema_version"] == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION


def test_normalize_workflow_context_data_v1_defaults_and_migrates_step_index():
    v1_payload: StateStorePayload = {
        "minion_modpath": "tests.assets.minions.sample",
        "workflow_id": "wf-legacy",
        "event": {"v": 1},
        "context": {"c": 1},
        "context_cls": "builtins.dict",
        "step_index": 1,
    }

    normalized = _normalize_workflow_context_data(
        {
            **v1_payload,
            "context_cls": dict,
        }
    )
    ctx = deserialize_workflow_context(v1_payload)

    assert isinstance(ctx, MinionWorkflowContext)
    assert ctx.next_step_index == 1
    assert ctx.workflow_id == "wf-legacy"
    assert normalized["schema_version"] == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    assert "step_index" not in normalized


def test_normalize_workflow_context_data_rejects_future_schema_version():
    payload = {
        "schema_version": 999,
        "minion_modpath": "tests.assets.minions.sample",
        "workflow_id": "wf-future",
        "event": {"v": 1},
        "context": {"c": 1},
        "context_cls": dict,
        "next_step_index": 0,
    }

    with pytest.raises(WorkflowContextSchemaError):
        _normalize_workflow_context_data(payload)


def test_storage_adapter_roundtrips_context_cls_and_schema_version():
    ctx = MinionWorkflowContext(
        minion_modpath="tests.assets.minions.sample",
        workflow_id="wf-storage",
        event={"v": 1},
        context={"c": 1},
        context_cls=dict,
        next_step_index=0,
    )
    stored_payload = serialize_workflow_context(ctx)
    loaded_ctx = deserialize_workflow_context(stored_payload)

    assert stored_payload["context_cls"] == "builtins.dict"
    assert stored_payload["schema_version"] == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    assert loaded_ctx == ctx


def test_deserialize_workflow_context_rejects_unknown_leftover_fields():
    payload = serialize_workflow_context(
        MinionWorkflowContext(
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
