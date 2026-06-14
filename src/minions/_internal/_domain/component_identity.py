"""Durable component identity decorators used by Gru orchestration identity.

The file lives outside gru.py so the public decorators and registry logic stay
small and importable, but the concept currently belongs to Gru's identity
boundary. Gru is the runtime consumer that turns these component IDs into stable
minion, pipeline, and resource identities.

If Gru is later split into a package/module hierarchy, this module should likely
move under that identity boundary instead of remaining as general domain code.
"""

from __future__ import annotations

import uuid
import weakref
from typing import Any, Callable, Literal, TypeVar

from .minion import Minion
from .pipeline import Pipeline
from .resource import Resource

T_Component = TypeVar("T_Component", bound=type[Any])
ComponentKind = Literal["minion", "pipeline", "resource"]

# Tracks loaded classes so duplicate durable IDs fail early within a process.
_COMPONENT_ID_REGISTRY: dict[tuple[ComponentKind, str], weakref.ReferenceType[type[Any]]] = {}


def generate_component_id() -> str:
    return str(uuid.uuid4())


def validate_component_id(component_id: object) -> str:
    if not isinstance(component_id, str):
        raise TypeError("component id must be a string")
    component_id = component_id.strip()
    if not component_id:
        raise ValueError("component id must not be empty")
    try:
        parsed = uuid.UUID(component_id)
    except ValueError as e:
        raise ValueError("component id must be a canonical UUID string") from e
    if str(parsed) != component_id:
        raise ValueError("component id must be a canonical lowercase UUID string")
    return component_id


def _component_ref(cls: type[Any]) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


def _register_component_id(kind: ComponentKind, component_id: str, cls: type[Any]) -> None:
    key = (kind, component_id)
    existing_ref = _COMPONENT_ID_REGISTRY.get(key)
    existing = existing_ref() if existing_ref is not None else None
    if (
        existing is not None
        and existing is not cls
        and _component_ref(existing) != _component_ref(cls)
    ):
        raise ValueError(
            f"Duplicate {kind} component id {component_id!r}: "
            f"{_component_ref(existing)} and {_component_ref(cls)}"
        )
    _COMPONENT_ID_REGISTRY[key] = weakref.ref(cls)


def _attach_component_id(kind: ComponentKind, component_id: str, cls: T_Component) -> T_Component:
    expected: dict[ComponentKind, type[Any]] = {
        "minion": Minion,
        "pipeline": Pipeline,
        "resource": Resource,
    }
    expected_cls = expected[kind]
    if not issubclass(cls, expected_cls):
        raise TypeError(f"@{kind}_id can only decorate {expected_cls.__name__} subclasses")

    normalized_id = validate_component_id(component_id)
    existing_kind = cls.__dict__.get("_mn_component_kind")
    existing_id = cls.__dict__.get("_mn_component_id")
    if existing_kind is not None and existing_kind != kind:
        raise TypeError(
            f"{_component_ref(cls)} already has a {existing_kind} component id"
        )
    if existing_id is not None and existing_id != normalized_id:
        raise ValueError(
            f"{_component_ref(cls)} already has component id {existing_id!r}"
        )

    setattr(cls, "_mn_component_kind", kind)
    setattr(cls, "_mn_component_id", normalized_id)
    _register_component_id(kind, normalized_id, cls)
    return cls


def minion_id(component_id: str) -> Callable[[T_Component], T_Component]:
    def decorator(cls: T_Component) -> T_Component:
        return _attach_component_id("minion", component_id, cls)
    return decorator


def pipeline_id(component_id: str) -> Callable[[T_Component], T_Component]:
    def decorator(cls: T_Component) -> T_Component:
        return _attach_component_id("pipeline", component_id, cls)
    return decorator


def resource_id(component_id: str) -> Callable[[T_Component], T_Component]:
    def decorator(cls: T_Component) -> T_Component:
        return _attach_component_id("resource", component_id, cls)
    return decorator


def get_component_id(cls: type[Any]) -> str | None:
    component_id = cls.__dict__.get("_mn_component_id")
    if isinstance(component_id, str):
        return component_id
    return None
