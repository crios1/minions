"""Workflow-context normalization and state-store round-trip helpers."""

import importlib
from dataclasses import fields
from functools import lru_cache
from typing import Any, TypeAlias, cast

import msgspec

from .state_store_payload_types import SerializableValue, StateStorePayload
from .._domain.minion_workflow_context import MinionWorkflowContext
from .._utils.serialization import deserialize, serialize

# Schema versions only advance for supported user-facing persistence epochs.
# Pre-adoption format churn can reuse the current schema while the shape settles.
CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION = 2

_WORKFLOW_CONTEXT_FIELD_NAMES = {field.name for field in fields(MinionWorkflowContext)}
_CODEC_METADATA_KEYS = {"schema_version"}

WorkflowContextData: TypeAlias = dict[
    str,
    SerializableValue | type
]


class PersistedMinionWorkflowContext(msgspec.Struct, forbid_unknown_fields=True):
    minion_composite_key: str
    minion_modpath: str
    workflow_id: str
    event: object
    context: object
    context_cls: str
    next_step_index: int = 0
    error_msg: str | None = None
    started_at: float | None = None
    schema_version: int = CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION

class WorkflowContextSchemaError(ValueError):
    pass


def _normalize_workflow_context_data(
    data: WorkflowContextData,
) -> WorkflowContextData:
    """normalizes to latest schema version"""

    version = data.get("schema_version")

    if version is None:
        raise WorkflowContextSchemaError(
            "Unsupported legacy workflow context payload missing schema_version; "
            f"minimum supported={CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION}."
        )
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
    if version < CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION:
        raise WorkflowContextSchemaError(
            "Unsupported legacy workflow context schema_version="
            f"{version}; minimum supported={CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION}."
        )

    return dict(data)


def _serialize_context_cls(t: type) -> str:
    return f"{t.__module__}.{t.__qualname__}"


@lru_cache(maxsize=128)
def _deserialize_context_cls(s: str) -> type:
    module, _, cls = s.rpartition(".")
    return getattr(importlib.import_module(module), cls)


def _restore_typed_value(
    value: SerializableValue,
    type_: Any,
    *,
    field_name: str,
) -> Any:
    try:
        if isinstance(type_, type) and isinstance(value, type_):
            return value
    except TypeError:
        pass
    try:
        return msgspec.convert(value, type=type_)
    except Exception as e:
        name = getattr(type_, "__qualname__", getattr(type_, "__name__", repr(type_)))
        raise WorkflowContextSchemaError(
            f"Invalid persisted workflow {field_name} for {name}: {e}"
        ) from e


def serialize_workflow_context(ctx: MinionWorkflowContext) -> StateStorePayload:
    data = _normalize_workflow_context_data(
        {
            **ctx.as_dict(),
            "schema_version": CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
        }
    )
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


def persist_workflow_context(
    ctx: MinionWorkflowContext,
) -> PersistedMinionWorkflowContext:
    return PersistedMinionWorkflowContext(
        minion_composite_key=ctx.minion_composite_key,
        minion_modpath=ctx.minion_modpath,
        workflow_id=ctx.workflow_id,
        event=ctx.event,
        context=ctx.context,
        context_cls=_serialize_context_cls(ctx.context_cls),
        next_step_index=ctx.next_step_index,
        error_msg=ctx.error_msg,
        started_at=ctx.started_at,
        schema_version=CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
    )


def serialize_persisted_workflow_context(
    ctx: MinionWorkflowContext,
) -> bytes:
    return serialize(persist_workflow_context(ctx))


def restore_workflow_context_types(
    ctx: MinionWorkflowContext,
    *,
    event_cls: Any | None = None,
) -> MinionWorkflowContext:
    event = ctx.event
    context = ctx.context

    if isinstance(ctx.context_cls, type):
        context = _restore_typed_value(
            cast(SerializableValue, ctx.context),
            ctx.context_cls,
            field_name="context",
        )
    if event_cls is not None:
        event = _restore_typed_value(
            cast(SerializableValue, ctx.event),
            event_cls,
            field_name="event",
        )

    return MinionWorkflowContext(
        minion_composite_key=ctx.minion_composite_key,
        minion_modpath=ctx.minion_modpath,
        workflow_id=ctx.workflow_id,
        event=event,
        context=context,
        context_cls=ctx.context_cls,
        next_step_index=ctx.next_step_index,
        error_msg=ctx.error_msg,
        started_at=ctx.started_at,
    )


def hydrate_persisted_workflow_context(
    persisted: PersistedMinionWorkflowContext,
    *,
    event_cls: Any | None = None,
) -> MinionWorkflowContext:
    if persisted.schema_version != CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION:
        raise WorkflowContextSchemaError(
            "Unsupported persisted workflow context schema_version="
            f"{persisted.schema_version}; current="
            f"{CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION}."
        )
    return restore_workflow_context_types(
        MinionWorkflowContext(
            minion_composite_key=persisted.minion_composite_key,
            minion_modpath=persisted.minion_modpath,
            workflow_id=persisted.workflow_id,
            event=cast(Any, persisted.event),
            context=cast(Any, persisted.context),
            context_cls=_deserialize_context_cls(persisted.context_cls),
            next_step_index=persisted.next_step_index,
            error_msg=persisted.error_msg,
            started_at=persisted.started_at,
        ),
        event_cls=event_cls,
    )


@lru_cache(maxsize=64)
def _typed_persisted_workflow_context_decoder(
    event_cls: Any,
    context_cls: type,
) -> msgspec.msgpack.Decoder:
    decoder_type = msgspec.defstruct(
        "TypedPersistedMinionWorkflowContext",
        [
            ("minion_composite_key", str),
            ("minion_modpath", str),
            ("workflow_id", str),
            ("event", event_cls),
            ("context", context_cls),
            ("context_cls", str),
            ("next_step_index", int, 0),
            ("error_msg", str | None, None),
            ("started_at", float | None, None),
            ("schema_version", int, CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION),
        ],
        forbid_unknown_fields=True,
    )
    return msgspec.msgpack.Decoder(type=decoder_type)


def decode_persisted_workflow_context_typed(
    payload: bytes,
    *,
    event_cls: Any,
    context_cls: type,
) -> MinionWorkflowContext:
    try:
        persisted = _typed_persisted_workflow_context_decoder(
            event_cls,
            context_cls,
        ).decode(payload)
    except (msgspec.DecodeError, msgspec.ValidationError) as e:
        raise WorkflowContextSchemaError(
            f"Invalid persisted workflow context payload: {e}"
        ) from e

    expected_context_cls = _serialize_context_cls(context_cls)
    if persisted.context_cls != expected_context_cls:
        raise WorkflowContextSchemaError(
            "Persisted workflow context_cls mismatch: "
            f"expected {expected_context_cls!r}, got {persisted.context_cls!r}."
        )
    if persisted.schema_version != CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION:
        raise WorkflowContextSchemaError(
            "Unsupported persisted workflow context schema_version="
            f"{persisted.schema_version}; current="
            f"{CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION}."
        )

    return MinionWorkflowContext(
        minion_composite_key=persisted.minion_composite_key,
        minion_modpath=persisted.minion_modpath,
        workflow_id=persisted.workflow_id,
        event=cast(Any, persisted.event),
        context=cast(Any, persisted.context),
        context_cls=context_cls,
        next_step_index=persisted.next_step_index,
        error_msg=persisted.error_msg,
        started_at=persisted.started_at,
    )


def deserialize_workflow_context_blob(
    payload: bytes,
    *,
    event_cls: Any | None = None,
    context_cls: type | None = None,
) -> MinionWorkflowContext:
    # todo: i think it could be reasonably expect that every payload
    # has a event_cls and context_cls, so we should consider simplifing the logic below
    if event_cls is not None and context_cls is not None:
        try:
            return decode_persisted_workflow_context_typed(
                payload,
                event_cls=event_cls,
                context_cls=context_cls,
            )
        except WorkflowContextSchemaError:
            pass

    try:
        persisted = deserialize(payload, PersistedMinionWorkflowContext)
    except ValueError:
        ctx = deserialize_workflow_context(
            deserialize(payload, dict),
            event_cls=event_cls,
        )
    else:
        ctx = hydrate_persisted_workflow_context(persisted, event_cls=event_cls)

    if context_cls is not None and ctx.context_cls is not context_cls:
        raise WorkflowContextSchemaError(
            "Persisted workflow context_cls mismatch: "
            f"expected {context_cls!r}, got {ctx.context_cls!r}."
        )
    return ctx


def deserialize_workflow_context(
    payload: StateStorePayload,
    *,
    event_cls: Any | None = None,
) -> MinionWorkflowContext:
    data: WorkflowContextData = dict(payload)
    context_cls = payload.get("context_cls")
    if isinstance(context_cls, str):
        data["context_cls"] = _deserialize_context_cls(context_cls)
    data = _normalize_workflow_context_data(data)
    resolved_context_cls = data.get("context_cls")
    if isinstance(resolved_context_cls, type) and "context" in data:
        data["context"] = _restore_typed_value(
            cast(SerializableValue, data["context"]),
            resolved_context_cls,
            field_name="context",
        )
    if event_cls is not None and "event" in data:
        data["event"] = _restore_typed_value(
            cast(SerializableValue, data["event"]),
            event_cls,
            field_name="event",
        )
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
    return restore_workflow_context_types(
        MinionWorkflowContext(**cast(dict[str, Any], kwargs)),
        event_cls=event_cls,
    )
