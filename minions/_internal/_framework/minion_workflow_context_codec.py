"""Workflow-context normalization and state-store round-trip helpers."""

import importlib
from dataclasses import fields
from typing import Any, Callable, TypeAlias, cast

from .state_store_payload_types import SerializableValue, StateStorePayload
from .._domain.minion_workflow_context import MinionWorkflowContext

CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION = 2

_WORKFLOW_CONTEXT_FIELD_NAMES = {field.name for field in fields(MinionWorkflowContext)}
_CODEC_METADATA_KEYS = {"schema_version"}

WorkflowContextData: TypeAlias = dict[
    str,
    SerializableValue | type
]


def _migrate_v1_to_v2(
    payload: WorkflowContextData,
) -> WorkflowContextData:
    # v1 used `step_index` and did not include an explicit schema_version.
    out = dict(payload)
    if "next_step_index" not in out and "step_index" in out:
        out["next_step_index"] = out.pop("step_index")
    out["schema_version"] = 2
    return out

_MIGRATIONS: dict[
    int,
    Callable[[WorkflowContextData], WorkflowContextData],
] = {
    1: _migrate_v1_to_v2,
}


class WorkflowContextSchemaError(ValueError):
    pass


def _normalize_workflow_context_data(
    data: WorkflowContextData,
) -> WorkflowContextData:
    """normalizes to latest schema version"""

    version = data.get("schema_version", 1)

    if not isinstance(version, int):
        raise WorkflowContextSchemaError(
            "Invalid workflow context schema_version type: "
            f"{type(version).__name__}."
        )
    if version > CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION:
        raise WorkflowContextSchemaError(
            f"Unsupported future workflow context schema_version={version}; "
            f"current={CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION}."
        )
    if version < 1:
        raise WorkflowContextSchemaError(
            "Unsupported workflow context schema_version="
            f"{version}; minimum supported=1."
        )

    normalized = dict(data)
    while version < CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION:
        migrator = _MIGRATIONS.get(version)
        if migrator is None:
            raise WorkflowContextSchemaError(
                f"No workflow context migration path from schema_version={version} "
                f"to schema_version={version + 1}."
            )
        normalized = migrator(normalized)
        version += 1
    return normalized


def _serialize_context_cls(t: type) -> str:
    return f"{t.__module__}.{t.__qualname__}"


def _deserialize_context_cls(s: str) -> type:
    module, _, cls = s.rpartition(".")
    return getattr(importlib.import_module(module), cls)


def serialize_workflow_context(ctx: MinionWorkflowContext) -> StateStorePayload:
    data = _normalize_workflow_context_data(ctx.as_dict())
    store_payload: StateStorePayload = {}
    for key, value in data.items():
        if key == "context_cls" and isinstance(value, type):
            store_payload[key] = _serialize_context_cls(value)
            continue
        if isinstance(value, type):
            raise WorkflowContextSchemaError(
                f"Unexpected type-valued workflow context field during serialization: {key!r}."
            )
        store_payload[key] = value
    store_payload["schema_version"] = CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    return store_payload


def deserialize_workflow_context(payload: StateStorePayload) -> MinionWorkflowContext:
    data: WorkflowContextData = dict(payload)
    context_cls = payload.get("context_cls")
    if isinstance(context_cls, str):
        data["context_cls"] = _deserialize_context_cls(context_cls)
    data = _normalize_workflow_context_data(data)
    kwargs = {
        k: v
        for k, v in data.items()
        if k in _WORKFLOW_CONTEXT_FIELD_NAMES
    }
    unknown_keys = (
        set(data)
        - _WORKFLOW_CONTEXT_FIELD_NAMES
        - _CODEC_METADATA_KEYS
    )
    if unknown_keys:
        raise WorkflowContextSchemaError(
            "Unexpected workflow context fields after normalization: "
            f"{sorted(unknown_keys)!r}."
        )
    # After filtering to declared MinionWorkflowContext fields and rejecting
    # unknown leftovers, the remaining kwargs are constructor-safe even though
    # the type checker can't infer that from the dict comprehension alone.
    return MinionWorkflowContext(**cast(dict[str, Any], kwargs))
