"""Workflow-context normalization and StateStore blob helpers.

This module has two related but distinct shapes:

* `serialize_workflow_context` / `deserialize_workflow_context` operate on the
  normalized dict adapter shape used by codec tests and schema normalization.
* `serialize_persisted_workflow_context` / `deserialize_workflow_context_blob`
  operate on the canonical bytes stored by `StateStore` implementations.
"""

import importlib
from collections.abc import Mapping
from dataclasses import fields
from functools import lru_cache
from typing import Any, TypeAlias, overload

import msgspec

from .._domain.minion_workflow_context import MinionWorkflowContext
from .._domain.types import T_Ctx, T_Event
from .._utils.serialization import deserialize, serialize

# Schema versions only advance for supported user-facing persistence epochs.
# Pre-adoption format churn can reuse the current schema while the shape settles.
CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION = 2

_WORKFLOW_CONTEXT_FIELD_NAMES = {field.name for field in fields(MinionWorkflowContext)}
_CODEC_METADATA_KEYS = {"schema_version"}

# Naming rule: keep codec-only helpers neutral, but keep explicit `Minion...`
# names for structs that mirror the runtime/domain envelope shape.
# Mutable adapter data while decoding/normalizing. `event` and `context` may
# be live user objects, msgspec structs, dataclasses, or primitive payloads.
WorkflowContextData: TypeAlias = dict[str, object]
_MsgspecFieldSpec: TypeAlias = (
    str
    | tuple[str, Any]
    | tuple[str, Any, Any]
)


class PersistedMinionWorkflowContext(msgspec.Struct, forbid_unknown_fields=True):
    """Versioned StateStore payload for one runtime MinionWorkflowContext."""

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
    """Validate and normalize the dict adapter shape to the current schema."""

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


def _convert_typed_value(value: object, type_: Any) -> object:
    return msgspec.convert(value, type=type_)

def _restore_typed_value(
    value: object,
    type_: Any,
    *,
    field_name: str,
) -> object:
    try:
        if isinstance(type_, type) and isinstance(value, type_):
            return value
    except TypeError:
        pass
    try:
        return _convert_typed_value(value, type_)
    except Exception as e:
        name = getattr(type_, "__qualname__", getattr(type_, "__name__", repr(type_)))
        raise WorkflowContextSchemaError(
            f"Invalid persisted workflow {field_name} for {name}: {e}"
        ) from e


def serialize_workflow_context(ctx: MinionWorkflowContext[Any, Any]) -> WorkflowContextData:
    """Return the normalized dict adapter shape for a runtime context.

    This is not the StateStore blob contract. Runtime persistence should use
    `serialize_persisted_workflow_context`, which wraps this logical data in a
    versioned msgspec payload and encodes it to bytes.
    """

    workflow_context_data = {
        **ctx.as_dict(),
        "schema_version": CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
    }
    data = _normalize_workflow_context_data(workflow_context_data)
    store_payload: WorkflowContextData = {}
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
    ctx: MinionWorkflowContext[Any, Any],
) -> PersistedMinionWorkflowContext:
    """Build the versioned StateStore payload object for a runtime context."""

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
    ctx: MinionWorkflowContext[Any, Any],
) -> bytes:
    """Encode the canonical StateStore blob for a runtime context."""

    return serialize(persist_workflow_context(ctx))


def restore_workflow_context_types(
    ctx: MinionWorkflowContext[Any, Any],
    *,
    event_cls: Any | None = None,
) -> MinionWorkflowContext[Any, Any]:
    event = ctx.event
    context = ctx.context

    context = _restore_typed_value(
        ctx.context,
        ctx.context_cls,
        field_name="context",
    )
    if event_cls is not None:
        event = _restore_typed_value(
            ctx.event,
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
) -> MinionWorkflowContext[Any, Any]:
    """Validate and hydrate a versioned StateStore payload into runtime shape."""

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
            event=persisted.event,
            context=persisted.context,
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
) -> msgspec.msgpack.Decoder[Any]:
    fields: list[_MsgspecFieldSpec] = [
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
    ]
    decoder_type = msgspec.defstruct(
        "TypedPersistedMinionWorkflowContext",
        fields,
        forbid_unknown_fields=True,
    )
    return msgspec.msgpack.Decoder(type=decoder_type)


def decode_persisted_workflow_context_typed(
    payload: bytes,
    *,
    event_cls: type[T_Event],
    context_cls: type[T_Ctx],
) -> MinionWorkflowContext[T_Event, T_Ctx]:
    """Decode a StateStore blob with typed event/context validation."""

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
        event=persisted.event,
        context=persisted.context,
        context_cls=context_cls,
        next_step_index=persisted.next_step_index,
        error_msg=persisted.error_msg,
        started_at=persisted.started_at,
    )


@overload
def deserialize_workflow_context_blob(
    payload: bytes,
    *,
    event_cls: type[T_Event],
    context_cls: type[T_Ctx],
) -> MinionWorkflowContext[T_Event, T_Ctx]:
    ...


@overload
def deserialize_workflow_context_blob(
    payload: bytes,
    *,
    event_cls: Any | None = ...,
    context_cls: type | None = ...,
) -> MinionWorkflowContext[Any, Any]:
    ...


def deserialize_workflow_context_blob(
    payload: bytes,
    *,
    event_cls: Any | None = None,
    context_cls: type | None = None,
) -> MinionWorkflowContext[Any, Any]:
    """Decode the canonical StateStore blob into runtime context shape."""

    if event_cls is not None and context_cls is not None:
        return decode_persisted_workflow_context_typed(
            payload,
            event_cls=event_cls,
            context_cls=context_cls,
        )

    try:
        persisted = deserialize(payload, PersistedMinionWorkflowContext)
    except ValueError as e:
        raise WorkflowContextSchemaError(
            f"Invalid persisted workflow context payload: {e}"
        ) from e
    ctx = hydrate_persisted_workflow_context(persisted, event_cls=event_cls)

    if context_cls is not None:
        if ctx.context_cls is context_cls:
            return ctx
        raise WorkflowContextSchemaError(
            "Persisted workflow context_cls mismatch: "
            f"expected {context_cls!r}, got {ctx.context_cls!r}."
        )
    return ctx


def deserialize_workflow_context(
    payload: Mapping[str, object],
    *,
    event_cls: Any | None = None,
) -> MinionWorkflowContext[Any, Any]:
    """Hydrate the normalized dict adapter shape into runtime context shape."""

    data: WorkflowContextData = dict(payload)
    context_cls = payload.get("context_cls")
    if isinstance(context_cls, str):
        try:
            data["context_cls"] = _deserialize_context_cls(context_cls)
        except Exception as e:
            raise WorkflowContextSchemaError(
                f"Invalid workflow context context_cls: {context_cls!r}."
            ) from e
    data = _normalize_workflow_context_data(data)
    resolved_context_cls = data.get("context_cls")
    if isinstance(resolved_context_cls, type) and "context" in data:
        data["context"] = _restore_typed_value(
            data["context"],
            resolved_context_cls,
            field_name="context",
        )
    if event_cls is not None and "event" in data:
        data["event"] = _restore_typed_value(
            data["event"],
            event_cls,
            field_name="event",
        )
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

    minion_composite_key = data.get("minion_composite_key")
    minion_modpath = data.get("minion_modpath")
    workflow_id = data.get("workflow_id")
    context_cls = data.get("context_cls")
    next_step_index = data.get("next_step_index", 0)
    error_msg = data.get("error_msg")
    started_at = data.get("started_at")

    if not isinstance(minion_composite_key, str):
        raise WorkflowContextSchemaError("Invalid workflow context minion_composite_key.")
    if not isinstance(minion_modpath, str):
        raise WorkflowContextSchemaError("Invalid workflow context minion_modpath.")
    if not isinstance(workflow_id, str):
        raise WorkflowContextSchemaError("Invalid workflow context workflow_id.")
    if "event" not in data:
        raise WorkflowContextSchemaError("Invalid workflow context event.")
    if "context" not in data:
        raise WorkflowContextSchemaError("Invalid workflow context context.")
    if not isinstance(context_cls, type):
        raise WorkflowContextSchemaError("Invalid workflow context context_cls.")
    if not isinstance(next_step_index, int):
        raise WorkflowContextSchemaError("Invalid workflow context next_step_index.")
    if error_msg is not None and not isinstance(error_msg, str):
        raise WorkflowContextSchemaError("Invalid workflow context error_msg.")
    if started_at is not None:
        if isinstance(started_at, bool) or not isinstance(started_at, (int, float)):
            raise WorkflowContextSchemaError("Invalid workflow context started_at.")
        started_at = float(started_at)

    return MinionWorkflowContext(
        minion_composite_key=minion_composite_key,
        minion_modpath=minion_modpath,
        workflow_id=workflow_id,
        event=data["event"],
        context=data["context"],
        context_cls=context_cls,
        next_step_index=next_step_index,
        error_msg=error_msg,
        started_at=started_at,
    )
