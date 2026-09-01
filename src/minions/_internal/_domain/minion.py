import ast
import asyncio
import contextvars
import inspect
import random
import sys
import textwrap
import time
import traceback
import uuid
from collections.abc import Awaitable, Coroutine
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Literal,
    Type,
    get_args,
    get_origin,
    get_type_hints,
)

from .._framework.async_component import LifecycleCallback
from .._framework.async_service import AsyncService
from .._framework.logger import DEBUG, ERROR, INFO, WARNING, Logger
from .._framework.metrics import Metrics
from .._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POINT,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATE_STORE,
    LABEL_STATUS,
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_ABORTED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_INFLIGHT_GAUGE,
    MINION_WORKFLOW_STEP_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
)
from .._framework.metrics_context import ResourceMetricContext
from .._framework.state_store import PersistenceOperationResult, StateStore
from .._utils.get_original_bases import get_original_bases
from .._utils.get_type_from_hint import get_type_from_hint
from .._utils.serialization import (
    require_user_declared_type,
)
from .._utils.validation import (
    ensure_nonnegative_number,
    ensure_number_at_least,
    ensure_number_in_closed_range,
    ensure_positive_number,
)
from .exceptions import AbortWorkflow
from .minion_workflow_context import MinionWorkflowContext
from .minion_workflow_handle import MinionWorkflowHandle
from .resource import Resource
from .types import T_Ctx, T_Event

ExecutionStatus = Literal["undefined", "interrupted", "aborted", "failed", "succeeded"]

WorkflowFailurePolicy = Literal["delete"]
_ALLOWED_WORKFLOW_FAILURE_POLICIES: tuple[WorkflowFailurePolicy, ...] = ("delete",)

WorkflowPersistenceFailurePolicy = Literal[
    "continue-on-failure",
    "idle-until-persisted",
]
_ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES: tuple[WorkflowPersistenceFailurePolicy, ...] = (
    "continue-on-failure",
    "idle-until-persisted",
)

WorkflowPersistencePoint = Literal[
    "workflow_start",
    "before_step",
    "workflow_resolve",
]
_ALLOWED_WORKFLOW_PERSISTENCE_POINTS: tuple[WorkflowPersistencePoint, ...] = (
    "workflow_start",
    "before_step",
    "workflow_resolve",
)

WorkflowPersistenceRiskKind = Literal[
    "missing_checkpoint",
    "stale_checkpoint",
    "unresolved_delete",
]


@dataclass(frozen=True, slots=True)
class WorkflowPersistenceState:
    persisted_next_step_index: int | None
    next_step_index: int
    delete_pending: bool = False

    @property
    def risk_kind(self) -> WorkflowPersistenceRiskKind | None:
        if self.delete_pending:
            return (
                "unresolved_delete"
                if self.persisted_next_step_index is not None
                else None
            )
        if self.persisted_next_step_index is None:
            return "missing_checkpoint"
        if self.persisted_next_step_index != self.next_step_index:
            return "stale_checkpoint"
        return None


@dataclass(frozen=True, slots=True)
class WorkflowPersistenceRisk:
    workflow_id: str
    kind: WorkflowPersistenceRiskKind
    persisted_next_step_index: int | None
    next_step_index: int


class WorkflowPersistenceNonRetryableError(RuntimeError):
    pass


class Minion(AsyncService, Generic[T_Event, T_Ctx]):
    _mn_user_facing = True
    _mn_event_var: contextvars.ContextVar[T_Event] = (
        contextvars.ContextVar(
            "minion_pipeline_event"
        )
    )
    _mn_context_var: contextvars.ContextVar[T_Ctx] = (
        contextvars.ContextVar(
            "minion_workflow_context"
        )
    )
    _mn_workflow_handle_var: contextvars.ContextVar[MinionWorkflowHandle] = (
        contextvars.ContextVar(
            "minion_workflow_handle"
        )
    )
    _mn_event_cls: Type[T_Event]
    _mn_workflow_ctx_cls: Type[T_Ctx]

    _mn_workflow_spec: ClassVar[tuple[str, ...] | None] = (
        None  # tuple of ordered workflow step names
    )
    _mn_defer_minion_setup: ClassVar[bool] = False

    # Subclass Construction and Validation

    @staticmethod
    def _mn_is_minion_class(typ: type[Any]) -> bool:
        return issubclass(typ, Minion)

    @staticmethod
    def _mn_get_descriptor_func(descriptor: Any) -> Any:
        return descriptor.__func__

    def __init_subclass__(cls, *, defer_minion_setup: bool = False, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        cls._mn_defer_minion_setup = bool(defer_minion_setup)
        cls._mn_workflow_spec = None

        if defer_minion_setup:
            return

        no_event_or_ctx_types_err = TypeError(
            f"{cls.__name__} must declare both event and workflow context types. "
            f"Example: class MyMinion(Minion[MyPipelineEvent, MyWorkflowCtx])"
        )

        multi_inheritance_err = TypeError(
            "When subclassing Minion, declare exactly one Minion[...] base with "
            "concrete Event and WorkflowCtx types."
        )

        nearest_minion: type[Any] | None = None
        for base in cls.__mro__[1:]:
            if cls._mn_is_minion_class(base):
                nearest_minion = base
                break
        if nearest_minion is None:
            raise TypeError(f"{cls.__name__} must subclass Minion.")
        if nearest_minion is not Minion and not getattr(
            nearest_minion, "_mn_defer_minion_setup", False
        ):
            raise TypeError(
                f"{cls.__name__} must subclass Minion directly. "
                "Subclasses of Minion subclasses are not supported."
            )

        bases = get_original_bases(cls)
        minionish = [
            b
            for b in bases
            if (origin := get_origin(b)) is not None and issubclass(origin, Minion)
        ]

        if not minionish:
            raise no_event_or_ctx_types_err

        if len(minionish) > 1:
            raise multi_inheritance_err

        args = get_args(minionish[0])
        if len(args) < 2:
            raise no_event_or_ctx_types_err

        cls._mn_event_cls = args[0]
        cls._mn_workflow_ctx_cls = args[1]

        require_user_declared_type(
            cls._mn_event_cls,
            owner=cls.__name__,
            type_label="event",
        )

        require_user_declared_type(
            cls._mn_workflow_ctx_cls,
            owner=cls.__name__,
            type_label="workflow context",
        )

        # Local import avoids a circular import while component identity lives
        # outside Gru but still imports Minion for decorator validation.
        from .component_identity import get_component_id

        resource_attrs_by_type: dict[type[Resource], list[str]] = {}
        for attr, hint in get_type_hints(cls).items():
            typ = get_type_from_hint(hint)
            if typ is not None and issubclass(typ, Resource):
                resource_attrs_by_type.setdefault(typ, []).append(attr)

        duplicates = {
            typ: names for typ, names in resource_attrs_by_type.items() if len(names) > 1
        }
        if duplicates:
            details = "; ".join(
                f"{get_component_id(typ) or f'{typ.__module__}.{typ.__name__}'} -> {names!r}"
                for typ, names in duplicates.items()
            )
            raise TypeError(
                f"{cls.__name__} declares multiple class attributes with the same "
                f"Resource type: {details}. "
                "Define only one class-level Resource per Resource type."
            )

        steps: list[tuple[int, str]] = []
        sources: dict[type[Any], list[str]] = {}

        for c in reversed(cls.__mro__):
            if not cls._mn_is_minion_class(c):
                continue
            for name, obj in c.__dict__.items():
                step = obj
                step_kind = "instance"
                if isinstance(obj, staticmethod):
                    step = cls._mn_get_descriptor_func(obj)
                    step_kind = "staticmethod"
                elif isinstance(obj, classmethod):
                    step = cls._mn_get_descriptor_func(obj)
                    step_kind = "classmethod"

                if not callable(step):
                    continue

                step = inspect.unwrap(step)
                if not inspect.isfunction(step):
                    continue

                if getattr(step, "__minion_step__", False):
                    if step_kind != "instance":
                        raise TypeError(
                            f"{cls.__name__}.{name}: @minion_step must decorate an "
                            f"**instance** method, not a {step_kind}."
                        )
                    lineno = inspect.getsourcelines(step)[1]
                    steps.append((lineno, name))
                    source_cls: type[Any] = c
                    sources.setdefault(source_cls, []).append(name)

        if len(sources) > 1:
            details = ", ".join(
                f"{c.__name__}: ({', '.join(names)})" for c, names in sources.items()
            )
            raise TypeError(
                f"Invalid Minion composition: @minion_step methods found in "
                f"multiple classes ({details}). Exactly one subclass may declare steps."
            )

        steps.sort()

        cls._mn_workflow_spec = tuple(name for _, name in steps)

        module_path = cls.__module__
        step_names = set(cls._mn_workflow_spec)
        for name in cls._mn_workflow_spec:
            fn = cls.__dict__[name]
            cls._mn_validate_no_step_to_step_calls(
                step_name=name,
                step_fn=fn,
                step_names=step_names,
            )
            cls._mn_validate_user_code(fn, module_path)

    @classmethod
    def _mn_validate_no_step_to_step_calls(
        cls,
        *,
        step_name: str,
        step_fn: Callable[..., Any],
        step_names: set[str],
    ) -> None:
        raw_fn = inspect.unwrap(step_fn)
        try:
            source = inspect.getsource(raw_fn)
        except (OSError, TypeError):
            return

        tree = ast.parse(textwrap.dedent(source))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            if not isinstance(target, ast.Attribute):
                continue
            if not isinstance(target.value, ast.Name) or target.value.id != "self":
                continue
            if target.attr not in step_names:
                continue
            raise TypeError(
                f"{cls.__name__}.{step_name} cannot call workflow step '{target.attr}'; "
                "minion steps must be orchestrated only by the runtime workflow engine."
            )

    # Runtime Construction, Identity, and Policy

    def __init__(
        self,
        minion_instance_id: str,
        orchestration_id: str,
        minion_module_path: str,
        config_path: str | None,
        state_store: StateStore,
        metrics: Metrics,
        logger: Logger,
        minion_id: str,
        minion_config_id: str,
        pipeline_id: str,
        inline_config: object | None = None,
        workflow_failure_policy: WorkflowFailurePolicy = "delete",
        workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = (
            "continue-on-failure"
        ),
        workflow_persistence_retry_delay_seconds: float = 1.0,
        workflow_persistence_retry_max_delay_seconds: float = 60.0,
        workflow_persistence_retry_backoff_multiplier: float = 2.0,
        workflow_persistence_retry_jitter_ratio: float = 0.1,
        workflow_persistence_retry_warning_interval_seconds: float = 30.0,
        workflow_persistence_retry_error_after_seconds: float | None = 60.0,
    ):
        super().__init__(logger)

        # Instance id identifies this live runtime object; minion id identifies
        # the stable component.
        self._mn_minion_id = minion_id
        self._mn_minion_instance_id = minion_instance_id
        self._mn_orchestration_id = orchestration_id
        self._mn_minion_config_id = minion_config_id
        self._mn_pipeline_id = pipeline_id
        self._mn_minion_module_path = minion_module_path
        self._mn_config_path = config_path
        self._mn_config: object | None = None
        if inline_config is not None:
            self._mn_bind_config(inline_config, source="Gru.start_orchestration minion_config")
        self._mn_config_lock = asyncio.Lock()
        self._mn_state_store = state_store
        self._mn_metrics = metrics
        self._mn_workflow_persistence_blocked_counts: dict[tuple[tuple[str, str], ...], int] = {}
        self._mn_workflow_persistence_blocked_counts_lock = asyncio.Lock()
        self._mn_workflow_persistence_states: dict[str, WorkflowPersistenceState] = {}
        self._mn_workflow_persistence_states_lock = asyncio.Lock()
        self._mn_workflow_step_inflight_counts: dict[str, int] = {}
        self._mn_workflow_step_inflight_counts_lock = asyncio.Lock()
        self._mn_workflow_failure_policy = self._mn_validate_workflow_failure_policy(
            workflow_failure_policy,
        )
        self._mn_workflow_persistence_failure_policy = (
            self._mn_validate_workflow_persistence_failure_policy(
                workflow_persistence_failure_policy,
            )
        )
        self._mn_workflow_persistence_retry_delay_seconds = ensure_positive_number(
            workflow_persistence_retry_delay_seconds,
            label="workflow_persistence_retry_delay_seconds",
        )
        self._mn_workflow_persistence_retry_max_delay_seconds = ensure_positive_number(
            workflow_persistence_retry_max_delay_seconds,
            label="workflow_persistence_retry_max_delay_seconds",
        )
        if (
            self._mn_workflow_persistence_retry_max_delay_seconds
            < self._mn_workflow_persistence_retry_delay_seconds
        ):
            raise ValueError(
                "workflow_persistence_retry_max_delay_seconds must be greater than or equal to "
                "workflow_persistence_retry_delay_seconds"
            )
        self._mn_workflow_persistence_retry_backoff_multiplier = (
            ensure_number_at_least(
                workflow_persistence_retry_backoff_multiplier,
                1,
                label="workflow_persistence_retry_backoff_multiplier",
            )
        )
        self._mn_workflow_persistence_retry_jitter_ratio = ensure_number_in_closed_range(
            workflow_persistence_retry_jitter_ratio,
            minimum=0,
            maximum=1,
            label="workflow_persistence_retry_jitter_ratio",
        )
        self._mn_workflow_persistence_retry_warning_interval_seconds = (
            ensure_positive_number(
                workflow_persistence_retry_warning_interval_seconds,
                label="workflow_persistence_retry_warning_interval_seconds",
            )
        )
        self._mn_workflow_persistence_retry_error_after_seconds = (
            None
            if workflow_persistence_retry_error_after_seconds is None
            else ensure_nonnegative_number(
                workflow_persistence_retry_error_after_seconds,
                label="workflow_persistence_retry_error_after_seconds",
            )
        )
        self._mn_workflow_tasks: set[asyncio.Task[None]] = set()
        self._mn_event_acceptance_lock = asyncio.Lock()
        self._mn_accepting_events = True
        self._mn_shutting_down = False

        cls = type(self)

        if cls._mn_defer_minion_setup:
            raise RuntimeError("Minion setup is deferred for this class.")

        if cls._mn_workflow_spec is None:
            raise RuntimeError(f"{cls.__name__}: workflow spec missing")

        if len(cls._mn_workflow_spec) == 0:
            raise TypeError(
                f"No @minion_step methods found in {cls.__name__}. "
                "Define at least one step to form a valid Minion subclass."
            )

        self._mn_workflow: tuple[Callable[..., Any], ...] = tuple(
            getattr(self, name) for name in cls._mn_workflow_spec
        )

    def _mn_identity_log_kwargs(self) -> dict[str, object]:
        return {
            "minion_instance_id": self._mn_minion_instance_id,
            "minion_id": self._mn_minion_id,
            "minion_config_id": self._mn_minion_config_id,
            "minion_module_path": self._mn_minion_module_path,
        }

    def _mn_orchestration_log_kwargs(self) -> dict[str, object]:
        return {
            **self._mn_identity_log_kwargs(),
            "orchestration_id": self._mn_orchestration_id,
            "pipeline_id": self._mn_pipeline_id,
        }

    def _mn_workflow_base_metric_labels(self) -> dict[str, str]:
        return {
            LABEL_ORCHESTRATION_ID: self._mn_orchestration_id,
            LABEL_MINION: self._mn_minion_id,
        }

    @staticmethod
    def _mn_validate_workflow_failure_policy(
        policy: str,
    ) -> WorkflowFailurePolicy:
        if policy not in _ALLOWED_WORKFLOW_FAILURE_POLICIES:
            policies = " or ".join(
                f"'{value}'" for value in _ALLOWED_WORKFLOW_FAILURE_POLICIES
            )
            raise ValueError(f"workflow_failure_policy must be {policies}")
        return policy

    @staticmethod
    def _mn_validate_workflow_persistence_failure_policy(
        policy: str,
    ) -> WorkflowPersistenceFailurePolicy:
        if policy not in _ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES:
            policies = " or ".join(
                f"'{value}'" for value in _ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES
            )
            raise ValueError(f"workflow_persistence_failure_policy must be {policies}")
        return policy

    def _mn_apply_workflow_persistence_retry_jitter(self, delay_seconds: float) -> float:
        jitter_ratio = self._mn_workflow_persistence_retry_jitter_ratio
        if jitter_ratio == 0:
            return delay_seconds
        jitter_seconds = delay_seconds * jitter_ratio
        return max(0.0, delay_seconds + random.uniform(-jitter_seconds, jitter_seconds))

    # Workflow Context Access

    @property
    def event(self) -> T_Event:
        try:
            return self._mn_event_var.get()
        except LookupError:
            raise RuntimeError("No event is currently bound to this workflow")

    @property
    def context(self) -> T_Ctx:
        try:
            return self._mn_context_var.get()
        except LookupError:
            raise RuntimeError("No context is currently bound to this workflow")

    @property
    def workflow_handle(self) -> MinionWorkflowHandle:
        try:
            return self._mn_workflow_handle_var.get()
        except LookupError:
            raise RuntimeError("No workflow handle is currently bound to this workflow")

    # Startup and Configuration

    async def _mn_startup(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None,
    ) -> None:
        async def _pre():
            self._mn_validate_user_code(self._mn_load_config, type(self).__module__)
            if self._mn_config_path and self._mn_config is None:
                await self._mn_load_config(self._mn_config_path)

        async def _post():
            contexts = (
                await self._mn_state_store._mn_get_decoded_contexts_for_orchestration(
                    self._mn_orchestration_id,
                    event_cls=type(self)._mn_event_cls,
                    context_cls=type(self)._mn_workflow_ctx_cls,
                )
            )
            if contexts:
                await asyncio.gather(
                    *(
                        self._mn_create_and_register_workflow_task_and_publish_inflight_gauge(
                            lambda ctx=ctx: self._mn_run_workflow(ctx)
                        )
                        for ctx in contexts
                    ),
                    return_exceptions=True
                )
            async with self._mn_tasks_gate:
                await self._mn_publish_workflow_inflight_gauge()

        return await super()._mn_startup(
            log_kwargs=self._mn_identity_log_kwargs(),
            pre=_pre,
            post=_post
        )

    async def _mn_load_config(self, config_path: str) -> object:
        async with self._mn_config_lock:
            config = await self.load_config(config_path)
            self._mn_bind_config(config, source=f"{type(self).__name__}.load_config")
            return config

    def _mn_bind_config(self, config: object, *, source: str) -> None:
        config_type = type(config)
        require_user_declared_type(
            config_type,
            owner=source,
            type_label="config",
        )

        config_hint = get_type_hints(type(self)).get("config")
        if config_hint is None:
            raise TypeError(
                f"{type(self).__name__} must declare a `config` type annotation "
                "when using minion config."
            )
        try:
            valid_config = isinstance(config, config_hint)
        except TypeError as e:
            raise TypeError(
                f"{type(self).__name__}.config annotation must support runtime config type checks."
            ) from e
        if not valid_config:
            raise TypeError(
                f"{type(self).__name__}.config expects {config_hint!r}, got {config_type.__name__}."
            )

        self._mn_config = config
        setattr(self, "config", config)

    async def load_config(self, config_path: str) -> object:
        raise NotImplementedError(
            f"{type(self).__name__}.load_config must be overridden to load "
            "file config into a dataclass or msgspec Struct instance."
        )

    # Workflow Persistence

    async def _mn_register_new_workflow_persistence_state(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
    ) -> None:
        async with self._mn_workflow_persistence_states_lock:
            if ctx.workflow_id in self._mn_workflow_persistence_states:
                raise RuntimeError(
                    "workflow persistence state is already registered for "
                    f"workflow {ctx.workflow_id!r}"
                )
            self._mn_workflow_persistence_states[ctx.workflow_id] = (
                WorkflowPersistenceState(
                    persisted_next_step_index=None,
                    next_step_index=ctx.next_step_index,
                )
            )

    async def _mn_register_resumed_workflow_persistence_state_if_absent(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
    ) -> None:
        async with self._mn_workflow_persistence_states_lock:
            if ctx.workflow_id in self._mn_workflow_persistence_states:
                return
            self._mn_workflow_persistence_states[ctx.workflow_id] = (
                WorkflowPersistenceState(
                    persisted_next_step_index=ctx.next_step_index,
                    next_step_index=ctx.next_step_index,
                )
            )

    async def _mn_update_workflow_persistence_state_for_operation(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        operation: Literal["save", "delete"],
    ) -> None:
        async with self._mn_workflow_persistence_states_lock:
            current = self._mn_workflow_persistence_states[ctx.workflow_id]
            self._mn_workflow_persistence_states[ctx.workflow_id] = (
                WorkflowPersistenceState(
                    persisted_next_step_index=current.persisted_next_step_index,
                    next_step_index=ctx.next_step_index,
                    delete_pending=(operation == "delete"),
                )
            )

    async def _mn_update_workflow_persistence_state_from_operation_result(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
    ) -> None:
        if not result.persisted:
            return
        async with self._mn_workflow_persistence_states_lock:
            current = self._mn_workflow_persistence_states[ctx.workflow_id]
            if operation == "delete":
                del self._mn_workflow_persistence_states[ctx.workflow_id]
                return
            self._mn_workflow_persistence_states[ctx.workflow_id] = (
                WorkflowPersistenceState(
                    persisted_next_step_index=current.next_step_index,
                    next_step_index=current.next_step_index,
                )
            )

    async def _mn_remove_workflow_persistence_state(self, workflow_id: str) -> None:
        async with self._mn_workflow_persistence_states_lock:
            self._mn_workflow_persistence_states.pop(workflow_id, None)

    async def _mn_workflow_persistence_state_snapshot(
        self,
    ) -> dict[str, WorkflowPersistenceState]:
        async with self._mn_workflow_persistence_states_lock:
            return dict(self._mn_workflow_persistence_states)

    @staticmethod
    def _mn_validate_workflow_persistence_point(
        persistence_point: WorkflowPersistencePoint,
        step_name: str | None,
    ) -> None:
        if persistence_point not in _ALLOWED_WORKFLOW_PERSISTENCE_POINTS:
            points = ", ".join(repr(point) for point in _ALLOWED_WORKFLOW_PERSISTENCE_POINTS)
            raise ValueError(f"persistence_point must be one of: {points}")
        if persistence_point == "before_step":
            if not step_name:
                raise ValueError("step_name is required for the 'before_step' persistence point")
        elif step_name is not None:
            raise ValueError("step_name is only valid for the 'before_step' persistence point")

    async def _mn_run_workflow_persistence_attempt(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        persistence_point: WorkflowPersistencePoint,
        operation: Literal["save", "delete"],
    ) -> PersistenceOperationResult:
        attempt_started_at = time.perf_counter()
        if operation == "save":
            result = await self._mn_state_store._mn_serialize_and_save_context(ctx)
        else:
            result = await self._mn_state_store._mn_delete_context(ctx.workflow_id)
        await self._mn_update_workflow_persistence_state_from_operation_result(
            ctx,
            operation=operation,
            result=result,
        )
        attempt_duration_seconds = time.perf_counter() - attempt_started_at
        await self._mn_record_workflow_persistence_attempt_metrics(
            persistence_point=persistence_point,
            operation=operation,
            result=result,
            duration_seconds=attempt_duration_seconds,
        )
        return result

    async def _mn_run_workflow_persistence_operation(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        persistence_point: WorkflowPersistencePoint,
        step_name: str | None = None,
    ) -> bool:
        self._mn_validate_workflow_persistence_point(
            persistence_point,
            step_name,
        )
        persistence_location_log_kwargs: dict[str, object] = {
            "persistence_point": persistence_point,
        }
        if step_name is not None:
            persistence_location_log_kwargs["step_name"] = step_name
        operation: Literal["save", "delete"] = (
            "delete" if persistence_point == "workflow_resolve" else "save"
        )
        block_on_retryable_failure = (
            operation == "delete"
            or self._mn_workflow_persistence_failure_policy == "idle-until-persisted"
        )
        attempts = 0
        first_failure_at: float | None = None
        last_warning_at: float | None = None
        last_logged_level: int | None = None
        retry_delay_seconds = self._mn_workflow_persistence_retry_delay_seconds
        blocked_labels: dict[str, str] | None = None
        try:
            await self._mn_update_workflow_persistence_state_for_operation(
                ctx,
                operation=operation,
            )
            while True:
                attempts += 1
                # An in-progress persistence attempt must finish before cancellation propagates.
                attempt_task = asyncio.create_task(
                    self._mn_run_workflow_persistence_attempt(
                        ctx,
                        persistence_point=persistence_point,
                        operation=operation,
                    ),
                    name=(
                        f"{type(self).__name__}:workflow-persistence:"
                        f"{persistence_point}:{ctx.workflow_id}"
                    ),
                )
                try:
                    result = await asyncio.shield(attempt_task)
                except asyncio.CancelledError:
                    while not attempt_task.done():
                        try:
                            await asyncio.shield(attempt_task)
                        except asyncio.CancelledError:
                            continue
                        except Exception:
                            break
                    if not attempt_task.cancelled():
                        attempt_error = attempt_task.exception()
                        if attempt_error is not None:
                            await self._mn_logger._mn_log_exception(
                                ERROR,
                                "Workflow persistence attempt failed during cancellation",
                                attempt_error,
                                workflow_id=ctx.workflow_id,
                                **persistence_location_log_kwargs,
                                persistence_operation=operation,
                                **self._mn_orchestration_log_kwargs(),
                            )
                    raise
                if result.persisted:
                    if blocked_labels is not None:
                        await self._mn_decrement_workflow_persistence_blocked_count(blocked_labels)
                        blocked_labels = None
                    if attempts > 1:
                        await self._mn_logger._mn_log(
                            INFO,
                            "Workflow persistence resumed"
                            if operation == "save"
                            else "Workflow checkpoint delete resumed",
                            workflow_id=ctx.workflow_id,
                            **persistence_location_log_kwargs,
                            persistence_operation=operation,
                            persistence_failure_policy=self._mn_workflow_persistence_failure_policy,
                            persistence_retry_attempts=attempts,
                            persistence_retry_elapsed_seconds=(
                                0.0
                                if first_failure_at is None
                                else time.monotonic() - first_failure_at
                            ),
                            persistence_retryable=True,
                            **self._mn_orchestration_log_kwargs(),
                        )
                    return True

                now = time.monotonic()
                if first_failure_at is None:
                    first_failure_at = now
                elapsed_seconds = now - first_failure_at

                if not result.retryable:
                    await self._mn_log_workflow_persistence_failure(
                        "Workflow persistence failed with non-retryable error",
                        ctx=ctx,
                        persistence_point=persistence_point,
                        step_name=step_name,
                        operation=operation,
                        result=result,
                        attempts=attempts,
                        elapsed_seconds=elapsed_seconds,
                        retry_delay_seconds=None,
                        level=ERROR,
                    )
                    persistence_location = persistence_point
                    if step_name is not None:
                        persistence_location += f" for step {step_name!r}"
                    raise WorkflowPersistenceNonRetryableError(
                        f"Workflow persistence failed during {result.failure_stage or 'unknown'} "
                        f"at {persistence_location}"
                    ) from result.error

                if not block_on_retryable_failure:
                    await self._mn_log_workflow_persistence_failure(
                        "Workflow continuing after persistence failure",
                        ctx=ctx,
                        persistence_point=persistence_point,
                        step_name=step_name,
                        operation=operation,
                        result=result,
                        attempts=attempts,
                        elapsed_seconds=elapsed_seconds,
                        retry_delay_seconds=None,
                        level=WARNING,
                    )
                    return False

                if blocked_labels is None:
                    blocked_labels = self._mn_workflow_persistence_blocked_metric_labels(
                        persistence_point=persistence_point,
                        operation=operation,
                        result=result,
                    )
                    await self._mn_increment_workflow_persistence_blocked_count(blocked_labels)

                error_after_seconds = self._mn_workflow_persistence_retry_error_after_seconds
                level = (
                    ERROR
                    if error_after_seconds is not None and elapsed_seconds >= error_after_seconds
                    else WARNING
                )
                should_log = (
                    attempts == 1
                    or last_warning_at is None
                    or (
                        now - last_warning_at
                        >= self._mn_workflow_persistence_retry_warning_interval_seconds
                    )
                    or (level == ERROR and last_logged_level != ERROR)
                )
                if should_log:
                    sleep_delay_seconds = self._mn_apply_workflow_persistence_retry_jitter(
                        retry_delay_seconds
                    )
                    await self._mn_log_workflow_persistence_failure(
                        "Workflow idled waiting for persistence"
                        if operation == "save"
                        else "Workflow idled waiting for checkpoint delete",
                        ctx=ctx,
                        persistence_point=persistence_point,
                        step_name=step_name,
                        operation=operation,
                        result=result,
                        attempts=attempts,
                        elapsed_seconds=elapsed_seconds,
                        retry_delay_seconds=sleep_delay_seconds,
                        level=level,
                    )
                    last_warning_at = now
                    last_logged_level = level
                else:
                    sleep_delay_seconds = self._mn_apply_workflow_persistence_retry_jitter(
                        retry_delay_seconds
                    )

                await asyncio.sleep(sleep_delay_seconds)
                retry_delay_seconds = min(
                    self._mn_workflow_persistence_retry_max_delay_seconds,
                    retry_delay_seconds * self._mn_workflow_persistence_retry_backoff_multiplier,
                )
        finally:
            if blocked_labels is not None:
                await self._mn_decrement_workflow_persistence_blocked_count(blocked_labels)

    async def _mn_log_workflow_persistence_failure(
        self,
        message: str,
        *,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        persistence_point: WorkflowPersistencePoint,
        step_name: str | None,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
        attempts: int,
        elapsed_seconds: float,
        retry_delay_seconds: float | None,
        level: int,
    ) -> None:
        error = result.error
        suggestion_by_stage = {
            "serialize": (
                "Ensure workflow event and context values are supported by the "
                "Minions persistence codec."
            ),
            "save": (
                "Ensure the configured StateStore is available and can persist "
                "workflow context blobs."
            ),
            "delete": (
                "Ensure the configured StateStore is available so completed "
                "workflow contexts can be removed."
            ),
            None: "Inspect the persistence failure details and runtime configuration.",
        }
        log_kwargs = {
            "workflow_id": ctx.workflow_id,
            "persistence_point": persistence_point,
            "persistence_operation": operation,
            "persistence_failure_policy": self._mn_workflow_persistence_failure_policy,
            "persistence_retry_attempts": attempts,
            "persistence_retry_delay_seconds": retry_delay_seconds,
            "persistence_retry_elapsed_seconds": elapsed_seconds,
            "persistence_failure_stage": result.failure_stage,
            "persistence_retryable": result.retryable,
            "suggestion": suggestion_by_stage[result.failure_stage],
            "state_store": type(self._mn_state_store).__name__,
            "event_type": type(ctx.event).__name__,
            "context_type": type(ctx.context).__name__,
            **self._mn_orchestration_log_kwargs(),
        }
        if step_name is not None:
            log_kwargs["step_name"] = step_name
        if error is not None:
            await self._mn_logger._mn_log_exception(level, message, error, **log_kwargs)
        else:
            await self._mn_logger._mn_log(level, message, **log_kwargs)

    # Workflow Metrics and Task Tracking

    def _mn_workflow_persistence_base_metric_labels(
        self,
        *,
        persistence_point: WorkflowPersistencePoint,
        operation: Literal["save", "delete"],
    ) -> dict[str, str]:
        return {
            **self._mn_workflow_base_metric_labels(),
            LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: persistence_point,
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: operation,
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: self._mn_workflow_persistence_failure_policy,
            LABEL_STATE_STORE: type(self._mn_state_store).__name__,
        }

    def _mn_workflow_persistence_failure_metric_labels(
        self,
        *,
        persistence_point: WorkflowPersistencePoint,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
    ) -> dict[str, str]:
        return {
            **self._mn_workflow_persistence_base_metric_labels(
                persistence_point=persistence_point,
                operation=operation,
            ),
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: result.failure_stage or "none",
            LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: str(result.retryable).lower(),
        }

    def _mn_workflow_persistence_blocked_metric_labels(
        self,
        *,
        persistence_point: WorkflowPersistencePoint,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
    ) -> dict[str, str]:
        return {
            **self._mn_workflow_base_metric_labels(),
            LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: persistence_point,
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: operation,
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: result.failure_stage or "none",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: self._mn_workflow_persistence_failure_policy,
            LABEL_STATE_STORE: type(self._mn_state_store).__name__,
        }

    @staticmethod
    def _mn_metric_label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(labels.items()))

    async def _mn_increment_workflow_persistence_blocked_count(
        self,
        labels: dict[str, str],
    ) -> None:
        key = self._mn_metric_label_key(labels)
        async with self._mn_workflow_persistence_blocked_counts_lock:
            value = self._mn_workflow_persistence_blocked_counts.get(key, 0) + 1
            self._mn_workflow_persistence_blocked_counts[key] = value
        await self._mn_metrics._mn_set(
            metric_name=MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
            value=value,
            labels=labels,
        )

    async def _mn_decrement_workflow_persistence_blocked_count(
        self,
        labels: dict[str, str],
    ) -> None:
        key = self._mn_metric_label_key(labels)
        async with self._mn_workflow_persistence_blocked_counts_lock:
            value = max(0, self._mn_workflow_persistence_blocked_counts.get(key, 0) - 1)
            if value:
                self._mn_workflow_persistence_blocked_counts[key] = value
            else:
                self._mn_workflow_persistence_blocked_counts.pop(key, None)
        await self._mn_metrics._mn_set(
            metric_name=MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
            value=value,
            labels=labels,
        )

    async def _mn_register_workflow_step_inflight(
        self,
        *,
        step_name: str,
    ) -> None:
        labels = {
            **self._mn_workflow_base_metric_labels(),
            LABEL_MINION_WORKFLOW_STEP: step_name,
        }
        async with self._mn_workflow_step_inflight_counts_lock:
            value = self._mn_workflow_step_inflight_counts.get(step_name, 0) + 1
            self._mn_workflow_step_inflight_counts[step_name] = value
            try:
                await self._mn_metrics._mn_set(
                    metric_name=MINION_WORKFLOW_STEP_INFLIGHT_GAUGE,
                    value=value,
                    labels=labels,
                )
            except BaseException:
                previous_value = value - 1
                if previous_value:
                    self._mn_workflow_step_inflight_counts[step_name] = previous_value
                else:
                    self._mn_workflow_step_inflight_counts.pop(step_name, None)
                raise

    async def _mn_unregister_workflow_step_inflight(
        self,
        *,
        step_name: str,
    ) -> None:
        labels = {
            **self._mn_workflow_base_metric_labels(),
            LABEL_MINION_WORKFLOW_STEP: step_name,
        }
        async with self._mn_workflow_step_inflight_counts_lock:
            current_value = self._mn_workflow_step_inflight_counts[step_name]
            value = current_value - 1
            if value:
                self._mn_workflow_step_inflight_counts[step_name] = value
            else:
                self._mn_workflow_step_inflight_counts.pop(step_name, None)
            await self._mn_metrics._mn_set(
                metric_name=MINION_WORKFLOW_STEP_INFLIGHT_GAUGE,
                value=value,
                labels=labels,
            )

    async def _mn_create_and_register_workflow_task_and_publish_inflight_gauge(
        self,
        workflow_runner: Callable[[], Coroutine[Any, Any, None]],
    ) -> asyncio.Task[None]:
        async def run_and_unregister_workflow_task() -> None:
            try:
                await workflow_runner()
            finally:
                task = asyncio.current_task()
                if task is not None:
                    await self._mn_unregister_workflow_task_and_publish_inflight_gauge(
                        task
                    )

        task: asyncio.Task[None] | None = None
        workflow_coro: Coroutine[Any, Any, None] | None = None
        try:
            async with self._mn_tasks_gate:
                workflow_coro = run_and_unregister_workflow_task()
                task = self.safe_create_task(workflow_coro)
                self._mn_workflow_tasks.add(task)
                await self._mn_publish_workflow_inflight_gauge()
                return task
        except BaseException:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                if workflow_coro is not None and inspect.getcoroutinestate(
                    workflow_coro
                ) == inspect.CORO_CREATED:
                    workflow_coro.close()
                async with self._mn_tasks_gate:
                    self._mn_workflow_tasks.discard(task)
            raise

    async def _mn_unregister_workflow_task_and_publish_inflight_gauge(
        self,
        task: asyncio.Task[None],
    ) -> None:
        async with self._mn_tasks_gate:
            self._mn_workflow_tasks.discard(task)
            await self._mn_publish_workflow_inflight_gauge()

    async def _mn_clear_workflow_tasks_and_publish_inflight_gauge(self) -> None:
        async with self._mn_tasks_gate:
            self._mn_workflow_tasks.clear()
            await self._mn_publish_workflow_inflight_gauge()

    async def _mn_publish_workflow_inflight_gauge(self) -> None:
        """Publish the task registry size while the caller holds `_mn_tasks_gate`."""
        await self._mn_metrics._mn_set(
            metric_name=MINION_WORKFLOW_INFLIGHT_GAUGE,
            value=len(self._mn_workflow_tasks),
            labels=self._mn_workflow_base_metric_labels(),
        )

    async def _mn_record_workflow_persistence_attempt_metrics(
        self,
        *,
        persistence_point: WorkflowPersistencePoint,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
        duration_seconds: float,
    ) -> None:
        base_labels = self._mn_workflow_persistence_base_metric_labels(
            persistence_point=persistence_point,
            operation=operation,
        )
        result_metric_name = (
            MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL
            if result.persisted
            else MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL
        )
        result_labels = (
            base_labels
            if result.persisted
            else self._mn_workflow_persistence_failure_metric_labels(
                persistence_point=persistence_point,
                operation=operation,
                result=result,
            )
        )
        await asyncio.gather(
            self._mn_metrics._mn_inc(
                metric_name=MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
                labels=base_labels,
            ),
            self._mn_metrics._mn_observe(
                metric_name=MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
                value=duration_seconds,
                labels=base_labels,
            ),
            self._mn_metrics._mn_inc(
                metric_name=result_metric_name,
                labels=result_labels,
            ),
        )

    # Workflow Execution

    @staticmethod
    async def _mn_shielded_gather(*aws: Awaitable[object]) -> list[object]:
        return await asyncio.shield(asyncio.gather(*aws))

    async def _mn_run_workflow(self, ctx: MinionWorkflowContext[T_Event, T_Ctx]) -> None:
        if self._mn_shutting_down:
            await self._mn_remove_workflow_persistence_state(ctx.workflow_id)
            return

        async def run_workflow() -> None:
            event_token = self._mn_event_var.set(ctx.event)
            context_token = self._mn_context_var.set(ctx.context)
            workflow_handle_token = self._mn_workflow_handle_var.set(
                MinionWorkflowHandle(
                    orchestration_id=ctx.orchestration_id,
                    workflow_id=ctx.workflow_id,
                )
            )

            metric_context = ExitStack()
            try:
                metric_context.enter_context(
                    ResourceMetricContext(
                        orchestration_id=self._mn_orchestration_id,
                        caller_kind="minion",
                        caller=self._mn_minion_id,
                    )
                )

                workflow_status: ExecutionStatus = "undefined"
                delete_persisted_context_on_exit = True
                terminal_workflow_log_level: int | None = None
                terminal_workflow_log_message: str | None = None
                terminal_workflow_error: Exception | None = None

                try:
                    if ctx.next_step_index == 0:
                        await self._mn_shielded_gather(
                            *[
                                self._mn_logger._mn_log(
                                    INFO,
                                    "Workflow started",
                                    workflow_id=ctx.workflow_id,
                                    **self._mn_orchestration_log_kwargs(),
                                ),
                                self._mn_metrics._mn_inc(
                                    metric_name=MINION_WORKFLOW_STARTED_TOTAL,
                                    labels=self._mn_workflow_base_metric_labels(),
                                ),
                            ]
                        )
                    else:
                        await self._mn_logger._mn_log(
                            INFO,
                            "Workflow resumed",
                            workflow_id=ctx.workflow_id,
                            **self._mn_orchestration_log_kwargs(),
                        )

                    for i in range(ctx.next_step_index, len(self._mn_workflow)):
                        ctx.next_step_index = i
                        await self._mn_execute_workflow_step(
                            ctx,
                            step_index=i,
                        )
                except asyncio.CancelledError:
                    workflow_status = "interrupted"
                    raise
                except AbortWorkflow:
                    workflow_status = "aborted"
                    terminal_workflow_log_level = INFO
                    terminal_workflow_log_message = "Workflow aborted"
                except Exception as e:
                    workflow_status = "failed"
                    if isinstance(e, WorkflowPersistenceNonRetryableError):
                        delete_persisted_context_on_exit = False
                    else:
                        delete_persisted_context_on_exit = (
                            self._mn_workflow_failure_policy == "delete"
                        )
                    terminal_workflow_log_level = ERROR
                    terminal_workflow_log_message = "Workflow failed"
                    terminal_workflow_error = e
                else:
                    workflow_status = "succeeded"
                    terminal_workflow_log_level = INFO
                    terminal_workflow_log_message = "Workflow succeeded"
                finally:
                    # remove resolved workflow context from the state store
                    if (
                        workflow_status in ("succeeded", "failed", "aborted")
                        and delete_persisted_context_on_exit
                    ):
                        try:
                            await self._mn_run_workflow_persistence_operation(
                                ctx,
                                persistence_point="workflow_resolve",
                            )
                        except asyncio.CancelledError:
                            workflow_status = "interrupted"
                            terminal_workflow_log_level = None
                            terminal_workflow_log_message = None
                            terminal_workflow_error = None
                            raise

                    # log and measure terminal workflow outcome

                    if workflow_status == "aborted" and terminal_workflow_log_message is not None:
                        await self._mn_shielded_gather(
                            self._mn_logger._mn_log(
                                terminal_workflow_log_level or INFO,
                                terminal_workflow_log_message,
                                workflow_id=ctx.workflow_id,
                                **self._mn_orchestration_log_kwargs(),
                            ),
                            self._mn_metrics._mn_inc(
                                metric_name=MINION_WORKFLOW_ABORTED_TOTAL,
                                labels=self._mn_workflow_base_metric_labels(),
                            ),
                        )
                    elif (
                        workflow_status == "failed"
                        and terminal_workflow_log_message is not None
                    ):
                        failure_error = terminal_workflow_error
                        if failure_error is None:
                            failure_error = RuntimeError("workflow failed")
                        await self._mn_shielded_gather(
                            self._mn_logger._mn_log_exception(
                                terminal_workflow_log_level or ERROR,
                                terminal_workflow_log_message,
                                failure_error,
                                workflow_id=ctx.workflow_id,
                                **self._mn_orchestration_log_kwargs(),
                            ),
                            self._mn_metrics._mn_inc(
                                metric_name=MINION_WORKFLOW_FAILED_TOTAL,
                                labels={
                                    **self._mn_workflow_base_metric_labels(),
                                    LABEL_ERROR_TYPE: type(failure_error).__name__,
                                },
                            ),
                        )
                    elif (
                        workflow_status == "succeeded"
                        and terminal_workflow_log_message is not None
                    ):
                        await self._mn_shielded_gather(
                            self._mn_logger._mn_log(
                                terminal_workflow_log_level or INFO,
                                terminal_workflow_log_message,
                                workflow_id=ctx.workflow_id,
                                **self._mn_orchestration_log_kwargs(),
                            ),
                            self._mn_metrics._mn_inc(
                                metric_name=MINION_WORKFLOW_SUCCEEDED_TOTAL,
                                labels=self._mn_workflow_base_metric_labels(),
                            ),
                        )

                    # measure workflow duration

                    duration = (
                        time.time() - ctx.started_at
                        if ctx.started_at is not None
                        else -1.0  # use sentinel in case of external modification
                    )
                    await self._mn_metrics._mn_observe(
                        metric_name=MINION_WORKFLOW_DURATION_SECONDS,
                        value=duration,
                        labels={
                            **self._mn_workflow_base_metric_labels(),
                            LABEL_STATUS: workflow_status,
                        },
                    )
            finally:
                try:
                    await self._mn_remove_workflow_persistence_state(ctx.workflow_id)
                finally:
                    self._mn_event_var.reset(event_token)
                    self._mn_context_var.reset(context_token)
                    self._mn_workflow_handle_var.reset(workflow_handle_token)
                    metric_context.close()

        try:
            await self._mn_register_resumed_workflow_persistence_state_if_absent(ctx)
            await run_workflow()
        except BaseException:
            await self._mn_remove_workflow_persistence_state(ctx.workflow_id)
            raise

    async def _mn_execute_workflow_step(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        step_index: int,
    ) -> None:
        step = self._mn_workflow[step_index]
        step_name = step.__name__
        step_start = time.time()
        step_status: ExecutionStatus = "undefined"

        await self._mn_shielded_gather(
            self._mn_logger._mn_log(
                DEBUG,
                "Workflow Step started",
                workflow_id=ctx.workflow_id,
                step_name=step_name,
                step_index=step_index,
                **self._mn_orchestration_log_kwargs(),
            ),
            self._mn_metrics._mn_inc(
                metric_name=MINION_WORKFLOW_STEP_STARTED_TOTAL,
                labels={
                    **self._mn_workflow_base_metric_labels(),
                    LABEL_MINION_WORKFLOW_STEP: step_name,
                },
            ),
        )
        await self._mn_register_workflow_step_inflight(step_name=step_name)

        try:
            if step_index > 0:
                await self._mn_run_workflow_persistence_operation(
                    ctx,
                    persistence_point="before_step",
                    step_name=step_name,
                )
            await step()
        except asyncio.CancelledError:
            step_status = "interrupted"
            raise
        except AbortWorkflow:
            step_status = "aborted"
            await self._mn_shielded_gather(
                self._mn_logger._mn_log(
                    INFO,
                    "Workflow Step aborted",
                    workflow_id=ctx.workflow_id,
                    step_name=step_name,
                    step_index=step_index,
                    **self._mn_orchestration_log_kwargs(),
                ),
                self._mn_metrics._mn_inc(
                    metric_name=MINION_WORKFLOW_STEP_ABORTED_TOTAL,
                    labels={
                        **self._mn_workflow_base_metric_labels(),
                        LABEL_MINION_WORKFLOW_STEP: step_name,
                    },
                ),
            )
            raise
        except Exception as e:
            step_status = "failed"
            log_kwargs: dict[str, object] = {
                "workflow_id": ctx.workflow_id,
                "step_name": step_name,
                "step_index": step_index,
                **self._mn_orchestration_log_kwargs(),
            }
            err_loc = self._mn_get_user_error_location(sys.exc_info()[2])
            if err_loc:
                log_kwargs.update(
                    {
                        "filepath": err_loc["filepath"],
                        "lineno": err_loc["lineno"],
                        "line": err_loc["line"],
                    }
                )
            await self._mn_shielded_gather(
                self._mn_logger._mn_log_exception(
                    ERROR,
                    "Workflow Step failed",
                    e,
                    **log_kwargs,
                ),
                self._mn_metrics._mn_inc(
                    metric_name=MINION_WORKFLOW_STEP_FAILED_TOTAL,
                    labels={
                        **self._mn_workflow_base_metric_labels(),
                        LABEL_MINION_WORKFLOW_STEP: step_name,
                        LABEL_ERROR_TYPE: type(e).__name__,
                    },
                ),
            )
            raise
        else:
            step_status = "succeeded"
            await self._mn_shielded_gather(
                self._mn_logger._mn_log(
                    DEBUG,
                    "Workflow Step succeeded",
                    workflow_id=ctx.workflow_id,
                    step_name=step_name,
                    step_index=step_index,
                    **self._mn_orchestration_log_kwargs(),
                ),
                self._mn_metrics._mn_inc(
                    metric_name=MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
                    labels={
                        **self._mn_workflow_base_metric_labels(),
                        LABEL_MINION_WORKFLOW_STEP: step_name,
                    },
                ),
            )
        finally:
            duration = time.time() - step_start
            await self._mn_shielded_gather(
                self._mn_metrics._mn_observe(
                    metric_name=MINION_WORKFLOW_STEP_DURATION_SECONDS,
                    value=duration,
                    labels={
                        **self._mn_workflow_base_metric_labels(),
                        LABEL_MINION_WORKFLOW_STEP: step_name,
                        LABEL_STATUS: step_status,
                    },
                ),
                self._mn_unregister_workflow_step_inflight(step_name=step_name),
            )

    def _mn_get_user_error_location(
        self,
        tb: TracebackType | None,
    ) -> dict[str, object] | None:
        if not tb:
            return None
        cwd = Path.cwd()
        for frame in reversed(traceback.extract_tb(tb)):
            try:
                rel_path = Path(frame.filename).resolve().relative_to(cwd)
            except ValueError:
                continue  # skip frames not under cwd
            if str(rel_path).startswith(str(self._mn_minion_module_path)):
                return {
                    "filepath": str(rel_path),
                    "lineno": frame.lineno,
                    "line": frame.line,
                }
        return None

    # Lifecycle and Event Handling

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict[str, object] | None = None,
        pre: LifecycleCallback | None = None,
        pre_args: list[object] | None = None,
        post: LifecycleCallback | None = None,
        post_args: list[object] | None = None,
    ) -> None:
        # Close event acceptance atomically with the shutdown transition.
        async with self._mn_event_acceptance_lock:
            self._mn_accepting_events = False
            self._mn_shutting_down = True

        try:
            return await super()._mn_shutdown(
                log_kwargs=self._mn_identity_log_kwargs(),
            )
        finally:
            # A stopped Minion owns no active workflow tasks. Persisted unresolved
            # contexts remain represented by the state store for future resumption.
            await self._mn_clear_workflow_tasks_and_publish_inflight_gauge()

    async def _mn_accept_event(self, event: T_Event) -> bool:
        """Return a bool indicating whether the event was accepted."""

        async with self._mn_event_acceptance_lock:
            if not self._mn_accepting_events:
                return False

            # Live events must wait for startup workflow resume to finish; otherwise
            # an event can be persisted before startup completes and then resumed.
            await self._mn_wait_until_running()

            ctx: MinionWorkflowContext[T_Event, T_Ctx] = MinionWorkflowContext(
                orchestration_id=self._mn_orchestration_id,
                workflow_id=uuid.uuid4().hex,
                event=event,
                context=type(self)._mn_workflow_ctx_cls(),
                started_at=time.time(),
            )
            await self._mn_register_new_workflow_persistence_state(ctx)

            async def run_workflow_from_event() -> None:
                try:
                    await self._mn_run_workflow_persistence_operation(
                        ctx,
                        persistence_point="workflow_start",
                    )
                except BaseException:
                    await self._mn_remove_workflow_persistence_state(ctx.workflow_id)
                    raise
                await self._mn_run_workflow(ctx)

            try:
                await self._mn_create_and_register_workflow_task_and_publish_inflight_gauge(
                    run_workflow_from_event
                )
            except BaseException:
                await self._mn_remove_workflow_persistence_state(ctx.workflow_id)
                raise

            return True

    async def _mn_request_stop(
        self,
        *,
        force: bool = False,
    ) -> tuple[bool, tuple[WorkflowPersistenceRisk, ...]]:
        """Return whether the stop request was accepted and its persistence risks."""
        async with self._mn_event_acceptance_lock:
            async with self._mn_workflow_persistence_states_lock:
                risks = tuple(
                    WorkflowPersistenceRisk(
                        workflow_id=workflow_id,
                        kind=state.risk_kind,
                        persisted_next_step_index=state.persisted_next_step_index,
                        next_step_index=state.next_step_index,
                    )
                    for workflow_id, state in sorted(
                        self._mn_workflow_persistence_states.items()
                    )
                    if state.risk_kind is not None
                )
                if risks and not force:
                    return False, risks

                self._mn_accepting_events = False
                self._mn_shutting_down = True

                return True, risks

    async def _mn_close_event_acceptance(self) -> None:
        """Close event acceptance."""
        async with self._mn_event_acceptance_lock:
            self._mn_accepting_events = False

    async def _mn_open_event_acceptance(self) -> None:
        """Open event acceptance unless an irreversible stop has begun."""
        async with self._mn_event_acceptance_lock:
            if not self._mn_shutting_down:
                self._mn_accepting_events = True

    # Task Idleness

    async def _mn_wait_until_workflows_idle(
        self, timeout: float | None = None
    ) -> None:
        """Wait until this Minion has no live workflow tasks."""
        await self._mn_wait_until_tasks_idle(
            timeout=timeout,
            task_subset=self._mn_workflow_tasks,
            timeout_msg=(
                f"{type(self).__name__} workflows did not become idle before timeout"
            ),
        )

    async def _mn_wait_until_workflows_drained(
        self, timeout: float | None = None
    ) -> None:
        await self._mn_wait_until_tasks_idle(
            timeout=timeout,
            task_subset=self._mn_workflow_tasks,
        )
