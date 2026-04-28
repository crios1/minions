import asyncio
import ast
import contextvars
import inspect
import random
import sys
import textwrap
import time
import traceback
import uuid
from pathlib import Path
from types import TracebackType
from typing import (
    Any, Awaitable, Callable, ClassVar,
    Generic, Literal, Type, cast,
    get_args, get_origin, get_type_hints
)

from .types import T_Event, T_Ctx
from .minion_workflow_context import MinionWorkflowContext
from .exceptions import AbortWorkflow
from .resource import Resource
from .._framework.async_service import AsyncService
from .._framework.logger import Logger, DEBUG, INFO, WARNING, ERROR, CRITICAL
from .._framework.metrics import Metrics
from .._framework.metrics_constants import (
    MINION_WORKFLOW_STARTED_TOTAL, MINION_WORKFLOW_INFLIGHT_GAUGE,
    MINION_WORKFLOW_ABORTED_TOTAL, MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_STEP_STARTED_TOTAL, MINION_WORKFLOW_STEP_INFLIGHT_GAUGE,
    MINION_WORKFLOW_STEP_ABORTED_TOTAL, MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    LABEL_MINION_COMPOSITE_KEY, LABEL_MINION_WORKFLOW_STEP,
    LABEL_ERROR_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_STATE_STORE,
    LABEL_STATUS,
)
from .._framework.state_store import PersistenceOperationResult, StateStore
from .._utils.format_exception_traceback import format_exception_traceback
from .._utils.get_class import get_class
from .._utils.serialization import is_type_serializable
from .._utils.get_original_bases import get_original_bases

ExecutionStatus = Literal["undefined", "interrupted", "aborted", "failed", "succeeded"]
WorkflowPersistenceFailurePolicy = Literal[
    "continue-on-failure",
    "idle-until-persisted",
]
_ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES: tuple[WorkflowPersistenceFailurePolicy, ...] = (
    "continue-on-failure",
    "idle-until-persisted",
)


class WorkflowPersistenceNonRetryableError(RuntimeError):
    pass


class Minion(AsyncService, Generic[T_Event, T_Ctx]):
    _mn_user_facing = True
    _mn_shutdown_grace_seconds: ClassVar[float] = 1.0

    _mn_event_var: contextvars.ContextVar[T_Event] = contextvars.ContextVar("minion_pipeline_event")
    _mn_context_var: contextvars.ContextVar[T_Ctx] = contextvars.ContextVar("minion_workflow_context")
    _mn_event_cls: Type[T_Event]
    _mn_workflow_ctx_cls: Type[T_Ctx]

    _mn_workflow_spec: ClassVar[tuple[str, ...] | None] = None # tuple of ordered workflow step names
    _mn_defer_minion_setup: ClassVar[bool] = False

    def __init_subclass__(cls, *, defer_minion_setup=False, **kwargs):
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
            "When subclassing Minion, declare exactly one Minion[...] base with concrete Event and WorkflowCtx types."
        )

        nearest_minion = next((b for b in cls.__mro__[1:] if issubclass(b, Minion)), None)
        if nearest_minion is None:
            raise TypeError(f"{cls.__name__} must subclass Minion.")
        if nearest_minion is not Minion and not getattr(nearest_minion, "_mn_defer_minion_setup", False):
            raise TypeError(
                f"{cls.__name__} must subclass Minion directly. "
                "Subclasses of Minion subclasses are not supported."
            )

        bases = get_original_bases(cls)
        minionish = [
            b for b in bases
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

        if not is_type_serializable(cls._mn_event_cls):
            raise TypeError(
                f"{cls.__name__}: event type is not JSON-serializable. "
                "Only JSON-safe types are supported (str, int, float, bool, None, list, tuple, dict[str, V], dataclass, msgspec.Struct, TypedDict)."
            )
        
        if cls._mn_event_cls in (str, int, float, bool, type(None)):
            raise TypeError(f"{cls.__name__}: event type must be a structured type, not a primitive")

        if not is_type_serializable(cls._mn_workflow_ctx_cls):
            raise TypeError(
                f"{cls.__name__}: workflow context is not JSON-serializable. "
                "Only JSON-safe types are supported (str, int, float, bool, None, list, tuple, dict[str, V], dataclass, msgspec.Struct, TypedDict)."
            )

        if cls._mn_workflow_ctx_cls in (str, int, float, bool, type(None)):
            raise TypeError(f"{cls.__name__}: workflow context type must be a structured type, not a primitive")

        res_map: dict[str, list[str]] = {}
        for attr, hint in get_type_hints(cls).items():
            candidate = get_class(hint)
            if isinstance(candidate, type) and issubclass(candidate, Resource):
                resource_id = f"{candidate.__module__}.{candidate.__name__}"
                res_map.setdefault(resource_id, []).append(attr)

        duplicates = {rid: names for rid, names in res_map.items() if len(names) > 1}
        if duplicates:
            details = "; ".join(f"{rid} -> {names!r}" for rid, names in duplicates.items())
            raise TypeError(
                f"{cls.__name__} declares multiple class attributes with the same Resource type: {details}. "
                "Define only one class-level Resource per Resource type."
            )

        steps: list[tuple[int, str]] = []
        sources: dict[type, list[str]] = {}

        for c in reversed(cls.__mro__):
            if not issubclass(c, Minion):
                continue
            for name, obj in c.__dict__.items():
                kind = "instance"
                raw = obj
                if isinstance(obj, staticmethod):
                    kind = "staticmethod"
                    raw = obj.__func__
                elif isinstance(obj, classmethod):
                    kind = "classmethod"
                    raw = obj.__func__

                raw = inspect.unwrap(raw)

                if getattr(raw, "__minion_step__", False):
                    if kind != "instance":
                        raise TypeError(
                            f"{cls.__name__}.{name}: @minion_step must decorate an **instance** method, "
                            f"not a {kind}."
                        )
                    lineno = inspect.getsourcelines(raw)[1]
                    steps.append((lineno, name))
                    sources.setdefault(c, []).append(name)

        if len(sources) > 1:
            details = ", ".join(f"{c.__name__}: ({', '.join(names)})" for c, names in sources.items())
            raise TypeError(
                f"Invalid Minion composition: @minion_step methods found in multiple classes ({details}). "
                "Exactly one subclass may declare steps."
            )

        steps.sort()

        cls._mn_workflow_spec = tuple(name for _, name in steps)

        modpath = cls.__module__
        step_names = set(cls._mn_workflow_spec)
        for name in cls._mn_workflow_spec:
            fn = cls.__dict__[name]
            cls._mn_validate_no_step_to_step_calls(
                step_name=name,
                step_fn=fn,
                step_names=step_names,
            )
            cls._mn_validate_user_code(fn, modpath)

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

    def __init__(
        self,
        minion_instance_id: str,
        minion_composite_key: str,
        minion_modpath: str,
        config_path: str | None,
        state_store: StateStore,
        metrics: Metrics,
        logger: Logger,
        workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = "continue-on-failure",
        workflow_persistence_retry_delay_seconds: float = 1.0,
        workflow_persistence_retry_max_delay_seconds: float = 60.0,
        workflow_persistence_retry_backoff_multiplier: float = 2.0,
        workflow_persistence_retry_jitter_ratio: float = 0.1,
        workflow_persistence_retry_warning_interval_seconds: float = 30.0,
        workflow_persistence_retry_error_after_seconds: float | None = 60.0,
    ):
        super().__init__(logger)

        name = getattr(type(self), "name", None)
        if name is not None and not isinstance(name, str):
            raise TypeError(f"{type(self).__name__}.name must be a string, got {type(name).__name__}")
        self._mn_name = name

        self._mn_minion_instance_id = minion_instance_id
        self._mn_minion_composite_key = minion_composite_key
        self._mn_minion_modpath = minion_modpath
        self._mn_config_path = config_path
        self._mn_config = None
        self._mn_config_lock = asyncio.Lock()
        self._mn_state_store = state_store
        self._mn_metrics = metrics
        self._mn_workflow_persistence_blocked_counts: dict[tuple[tuple[str, str], ...], int] = {}
        self._mn_workflow_persistence_blocked_counts_lock = asyncio.Lock()
        self._mn_workflow_persistence_failure_policy = self._mn_validate_workflow_persistence_failure_policy(
            workflow_persistence_failure_policy,
        )
        self._mn_workflow_persistence_retry_delay_seconds = self._mn_validate_positive_seconds(
            "workflow_persistence_retry_delay_seconds",
            workflow_persistence_retry_delay_seconds,
        )
        self._mn_workflow_persistence_retry_max_delay_seconds = self._mn_validate_positive_seconds(
            "workflow_persistence_retry_max_delay_seconds",
            workflow_persistence_retry_max_delay_seconds,
        )
        if self._mn_workflow_persistence_retry_max_delay_seconds < self._mn_workflow_persistence_retry_delay_seconds:
            raise ValueError(
                "workflow_persistence_retry_max_delay_seconds must be greater than or equal to "
                "workflow_persistence_retry_delay_seconds"
            )
        self._mn_workflow_persistence_retry_backoff_multiplier = self._mn_validate_backoff_multiplier(
            "workflow_persistence_retry_backoff_multiplier",
            workflow_persistence_retry_backoff_multiplier,
        )
        self._mn_workflow_persistence_retry_jitter_ratio = self._mn_validate_jitter_ratio(
            "workflow_persistence_retry_jitter_ratio",
            workflow_persistence_retry_jitter_ratio,
        )
        self._mn_workflow_persistence_retry_warning_interval_seconds = self._mn_validate_positive_seconds(
            "workflow_persistence_retry_warning_interval_seconds",
            workflow_persistence_retry_warning_interval_seconds,
        )
        self._mn_workflow_persistence_retry_error_after_seconds = self._mn_validate_optional_nonnegative_seconds(
            "workflow_persistence_retry_error_after_seconds",
            workflow_persistence_retry_error_after_seconds,
        )
        self._mn_workflow_tasks: set[asyncio.Task] = set()
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

    @staticmethod
    def _mn_validate_workflow_persistence_failure_policy(
        policy: str,
    ) -> WorkflowPersistenceFailurePolicy:
        if policy not in _ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES:
            policies = " or ".join(f"'{value}'" for value in _ALLOWED_WORKFLOW_PERSISTENCE_FAILURE_POLICIES)
            raise ValueError(
                f"workflow_persistence_failure_policy must be {policies}"
            )
        return cast(WorkflowPersistenceFailurePolicy, policy)

    @staticmethod
    def _mn_validate_positive_seconds(name: str, value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
            raise ValueError(f"{name} must be a positive number of seconds")
        return float(value)

    @staticmethod
    def _mn_validate_optional_nonnegative_seconds(name: str, value: float | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
            raise ValueError(f"{name} must be None or a non-negative number of seconds")
        return float(value)

    @staticmethod
    def _mn_validate_backoff_multiplier(name: str, value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float) or value < 1:
            raise ValueError(f"{name} must be a number greater than or equal to 1")
        return float(value)

    @staticmethod
    def _mn_validate_jitter_ratio(name: str, value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float) or value < 0 or value > 1:
            raise ValueError(f"{name} must be a number between 0 and 1")
        return float(value)

    def _mn_apply_workflow_persistence_retry_jitter(self, delay_seconds: float) -> float:
        jitter_ratio = self._mn_workflow_persistence_retry_jitter_ratio
        if jitter_ratio == 0:
            return delay_seconds
        jitter_seconds = delay_seconds * jitter_ratio
        return max(0.0, delay_seconds + random.uniform(-jitter_seconds, jitter_seconds))

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

    def _mn_make_workflow(self) -> tuple[Callable]:
        "workflow is defined as the subclass's methods tagged as minion steps, in declaration order"
        steps: list[tuple[int, str]] = []
        sources: dict[type, list[str]] = {}

        for cls in reversed(type(self).__mro__):
            if not issubclass(cls, Minion):
                continue
            for name, method in cls.__dict__.items():
                if getattr(method, "__minion_step__", False):
                    lineno = inspect.getsourcelines(method)[1]
                    steps.append((lineno, name))
                    sources.setdefault(cls, []).append(name)

        if not sources:
            raise TypeError(
                f"No @minion_step methods found in {type(self).__name__}. "
                f"At least one step must be defined to form a workflow."
            )

        if len(sources) > 1:
            details = ", ".join(f"{c.__name__}: {', '.join(names)}" for c, names in sources.items())
            raise TypeError(
                f"Invalid Minion composition: @minion_step methods found in multiple classes ({details}). "
                f"Exactly one subclass may declare steps."
            )

        steps.sort()
        workflow = tuple(getattr(self, name) for _, name in steps)

        for step in workflow:
            self._mn_validate_user_code(step, self._mn_minion_modpath)

        return workflow

    async def _mn_startup(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ) -> None:
        async def _pre():
            self._mn_validate_user_code(self._mn_load_config, type(self).__module__)
            if self._mn_config_path:
                self._mn_config = await self._mn_load_config(self._mn_config_path)
        
        async def _post():
            contexts = await self._mn_state_store._mn_get_decoded_contexts_for_orchestration(
                self._mn_minion_composite_key,
                event_cls=type(self)._mn_event_cls,
                context_cls=type(self)._mn_workflow_ctx_cls,
            )
            if contexts:
                await asyncio.gather( 
                    *(self._mn_run_workflow(ctx) for ctx in contexts),
                    return_exceptions=True
                )

        return await super()._mn_startup(
            log_kwargs={'minion_instance_id': self._mn_minion_instance_id},
            pre=_pre,
            post=_post
        )

    async def _mn_load_config(self, config_path: str) -> dict:
        async with self._mn_config_lock:
            return await self.load_config(config_path)

    async def load_config(self, config_path: str) -> dict:
        """
        Default config loader that supports TOML, JSON, and YAML files.

        Override this method to define how your Minion loads its configuration.

        Returns:
            dict: Parsed configuration contents. This must always be a `dict`,
                regardless of the config file format or structure.

        Raises:
            FileNotFoundError: If the config file does not exist.
            ValueError: If the config format is unsupported or parsing fails.
        """
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Minion config file not found: {path}") # pragma: no cover

        suffix = path.suffix.lower()

        try:
            contents = await asyncio.to_thread(path.read_text)

            if suffix in (".yaml", ".yml"):
                try:
                    import yaml
                except ImportError:
                    raise RuntimeError(
                        "YAML support requires 'PyYAML'. Install it or override load_config()."
                    )
                return yaml.safe_load(contents)

            elif suffix == ".toml":
                try:
                    import tomllib  # Python 3.11+
                except ImportError:
                    try:
                        import tomli as tomllib  # fallback for <3.11  # pyright: ignore[reportMissingImports]
                    except ImportError:
                        raise RuntimeError(
                            "TOML support requires Python 3.11+ or installing 'tomli'. "
                            "Install tomli or override load_config()."
                        )
                return tomllib.loads(contents)

            elif suffix == ".json":
                import json
                return json.loads(contents)

            else:
                raise ValueError(
                    f"Unsupported config file format: '{suffix}'. "
                    f"Supported formats: .toml, .json, .yaml. "
                    f"If you want to support '{suffix}', override your Minion's load_config() method."
                )

        except Exception as e:
            raise ValueError(f"Failed to parse config file '{path}': {e}")

    async def _mn_run_workflow_persistence_checkpoint(
        self,
        ctx: MinionWorkflowContext[T_Event, T_Ctx],
        *,
        checkpoint: str,
        operation: Literal["save", "delete"] = "save",
        block_on_retryable_failure: bool | None = None,
    ) -> bool:
        if block_on_retryable_failure is None:
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
            while True:
                attempts += 1
                attempt_started_at = time.perf_counter()
                if operation == "save":
                    result = await asyncio.shield(
                        self._mn_state_store._mn_serialize_and_save_context(ctx)
                    )
                else:
                    result = await asyncio.shield(
                        self._mn_state_store._mn_delete_context(ctx.workflow_id)
                    )
                attempt_duration_seconds = time.perf_counter() - attempt_started_at
                await self._mn_record_workflow_persistence_attempt_metrics(
                    checkpoint=checkpoint,
                    operation=operation,
                    result=result,
                    duration_seconds=attempt_duration_seconds,
                )
                if result.persisted:
                    if blocked_labels is not None:
                        await self._mn_decrement_workflow_persistence_blocked_count(blocked_labels)
                        blocked_labels = None
                    if attempts > 1:
                        await self._mn_logger._log(
                            INFO,
                            "Workflow persistence resumed"
                            if operation == "save" else
                            "Workflow checkpoint delete resumed",
                            workflow_id=ctx.workflow_id,
                            checkpoint=checkpoint,
                            persistence_operation=operation,
                            persistence_failure_policy=self._mn_workflow_persistence_failure_policy,
                            persistence_retry_attempts=attempts,
                            persistence_retry_elapsed_seconds=(
                                0.0 if first_failure_at is None else time.monotonic() - first_failure_at
                            ),
                            persistence_retryable=True,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath,
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
                        checkpoint=checkpoint,
                        operation=operation,
                        result=result,
                        attempts=attempts,
                        elapsed_seconds=elapsed_seconds,
                        retry_delay_seconds=None,
                        level=ERROR,
                    )
                    raise WorkflowPersistenceNonRetryableError(
                        f"Workflow persistence failed during {result.failure_stage or 'unknown'} at {checkpoint}"
                    ) from result.error

                if not block_on_retryable_failure:
                    await self._mn_log_workflow_persistence_failure(
                        "Workflow continuing after persistence failure",
                        ctx=ctx,
                        checkpoint=checkpoint,
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
                        checkpoint=checkpoint,
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
                    or now - last_warning_at >= self._mn_workflow_persistence_retry_warning_interval_seconds
                    or (level == ERROR and last_logged_level != ERROR)
                )
                if should_log:
                    sleep_delay_seconds = self._mn_apply_workflow_persistence_retry_jitter(retry_delay_seconds)
                    await self._mn_log_workflow_persistence_failure(
                        "Workflow idled waiting for persistence"
                        if operation == "save" else
                        "Workflow idled waiting for checkpoint delete",
                        ctx=ctx,
                        checkpoint=checkpoint,
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
                    sleep_delay_seconds = self._mn_apply_workflow_persistence_retry_jitter(retry_delay_seconds)

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
        checkpoint: str,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
        attempts: int,
        elapsed_seconds: float,
        retry_delay_seconds: float | None,
        level: int,
    ) -> None:
        error = result.error
        suggestion_by_stage = {
            "serialize": "Ensure workflow event and context values are supported by the Minions persistence codec.",
            "save": "Ensure the configured StateStore is available and can persist workflow context blobs.",
            "delete": "Ensure the configured StateStore is available so completed workflow contexts can be removed.",
        }
        log_kwargs = {
            "workflow_id": ctx.workflow_id,
            "checkpoint": checkpoint,
            "persistence_operation": operation,
            "persistence_failure_policy": self._mn_workflow_persistence_failure_policy,
            "persistence_retry_attempts": attempts,
            "persistence_retry_delay_seconds": retry_delay_seconds,
            "persistence_retry_elapsed_seconds": elapsed_seconds,
            "persistence_failure_stage": result.failure_stage,
            "persistence_retryable": result.retryable,
            "suggestion": suggestion_by_stage.get(
                result.failure_stage,
                "Inspect the persistence failure details and runtime configuration.",
            ),
            "state_store": type(self._mn_state_store).__name__,
            "event_type": type(ctx.event).__name__,
            "context_type": getattr(ctx.context_cls, "__name__", type(ctx.context).__name__),
            "minion_name": self._mn_name,
            "minion_instance_id": self._mn_minion_instance_id,
            "minion_composite_key": self._mn_minion_composite_key,
            "minion_modpath": self._mn_minion_modpath,
        }
        if error is not None:
            log_kwargs.update(
                error_type=type(error).__name__,
                error_message=str(error),
                traceback=format_exception_traceback(error),
            )
        await self._mn_logger._log(level, message, **log_kwargs)

    def _mn_workflow_persistence_base_metric_labels(
        self,
        *,
        checkpoint: str,
        operation: Literal["save", "delete"],
    ) -> dict[str, str]:
        checkpoint_type, _ = self._mn_workflow_persistence_checkpoint_metric_parts(checkpoint)
        return {
            LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: checkpoint_type,
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: operation,
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: self._mn_workflow_persistence_failure_policy,
            LABEL_STATE_STORE: type(self._mn_state_store).__name__,
        }

    def _mn_workflow_persistence_failure_metric_labels(
        self,
        *,
        checkpoint: str,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
    ) -> dict[str, str]:
        return {
            **self._mn_workflow_persistence_base_metric_labels(checkpoint=checkpoint, operation=operation),
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: result.failure_stage or "none",
            LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: str(result.retryable).lower(),
        }

    def _mn_workflow_persistence_blocked_metric_labels(
        self,
        *,
        checkpoint: str,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
    ) -> dict[str, str]:
        checkpoint_type, _ = self._mn_workflow_persistence_checkpoint_metric_parts(checkpoint)
        return {
            LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: checkpoint_type,
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: operation,
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: result.failure_stage or "none",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: self._mn_workflow_persistence_failure_policy,
            LABEL_STATE_STORE: type(self._mn_state_store).__name__,
        }

    @staticmethod
    def _mn_workflow_persistence_checkpoint_metric_parts(checkpoint: str) -> tuple[str, str]:
        if checkpoint == "workflow_start":
            return "workflow_start", ""
        if checkpoint.startswith("before_step:"):
            return "before_step", checkpoint.removeprefix("before_step:")
        return checkpoint, ""

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
        await self._mn_metrics._set(
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
        await self._mn_metrics._set(
            metric_name=MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
            value=value,
            labels=labels,
        )

    async def _mn_record_workflow_persistence_attempt_metrics(
        self,
        *,
        checkpoint: str,
        operation: Literal["save", "delete"],
        result: PersistenceOperationResult,
        duration_seconds: float,
    ) -> None:
        base_labels = self._mn_workflow_persistence_base_metric_labels(
            checkpoint=checkpoint,
            operation=operation,
        )
        result_metric_name = (
            MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL
            if result.persisted else
            MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL
        )
        result_labels = (
            base_labels
            if result.persisted else
            self._mn_workflow_persistence_failure_metric_labels(
                checkpoint=checkpoint,
                operation=operation,
                result=result,
            )
        )
        await asyncio.gather(
            self._mn_metrics._inc(
                metric_name=MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
                labels=base_labels,
            ),
            self._mn_metrics._observe(
                metric_name=MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
                value=duration_seconds,
                labels=base_labels,
            ),
            self._mn_metrics._inc(
                metric_name=result_metric_name,
                labels=result_labels,
            ),
        )

    async def _mn_run_workflow(self, ctx: MinionWorkflowContext[T_Event, T_Ctx]):
        if self._mn_shutting_down:
            return
        async def _shielded_gather(*aws):
            return await asyncio.shield(asyncio.gather(*aws))

        async def run():
            event_token = self._mn_event_var.set(ctx.event)
            context_token = self._mn_context_var.set(ctx.context)
            workflow_status: ExecutionStatus = "undefined"
            delete_persisted_context_on_exit = True
            terminal_workflow_log_level: int | None = None
            terminal_workflow_log_message: str | None = None
            terminal_workflow_error: Exception | None = None
            try: # run workflow (step by step)
                if ctx.next_step_index == 0:
                    await _shielded_gather(*[
                        self._mn_logger._log(
                            INFO,
                            "Workflow started",
                            workflow_id=ctx.workflow_id,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath
                        ),
                        self._mn_metrics._inc(
                            metric_name=MINION_WORKFLOW_STARTED_TOTAL,
                            labels={LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key},
                        )
                    ])
                else:
                    await self._mn_logger._log(
                        INFO,
                        "Workflow resumed",
                        workflow_id=ctx.workflow_id,
                        minion_name=self._mn_name,
                        minion_instance_id=self._mn_minion_instance_id,
                        minion_composite_key=self._mn_minion_composite_key,
                        minion_modpath=self._mn_minion_modpath
                    )

                workflow = self._mn_workflow
                
                for i in range(ctx.next_step_index, len(workflow)):
                    if i == 0:
                        ctx.started_at = time.time()
                    ctx.next_step_index = i

                    step = workflow[i]
                    step_name = step.__name__
                    step_start = ctx.started_at if i == 0 else time.time()
                    step_status: ExecutionStatus = "undefined"

                    await _shielded_gather(*[
                        self._mn_logger._log(
                            DEBUG,
                            "Workflow Step started",
                            workflow_id=ctx.workflow_id,
                            step_name=step_name,
                            step_index=i,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath
                        ),
                        self._mn_metrics._inc(
                            metric_name=MINION_WORKFLOW_STEP_STARTED_TOTAL,
                            labels={
                                LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                LABEL_MINION_WORKFLOW_STEP: step_name,
                            },
                        )
                    ])

                    try: # run step / store context
                        await self._mn_run_workflow_persistence_checkpoint(
                            ctx,
                            checkpoint=f"before_step:{step_name}",
                        )
                        await step()
                    except asyncio.CancelledError:
                        step_status: ExecutionStatus = "interrupted"
                        raise
                    except AbortWorkflow: # log / measure step aborted
                        step_status: ExecutionStatus = "aborted"
                        await _shielded_gather(*[
                            self._mn_logger._log(
                                INFO,
                                "Workflow Step aborted",
                                workflow_id=ctx.workflow_id,
                                step_name=step_name,
                                step_index=i,
                                minion_name=self._mn_name,
                                minion_instance_id=self._mn_minion_instance_id,
                                minion_composite_key=self._mn_minion_composite_key,
                                minion_modpath=self._mn_minion_modpath
                            ),
                            self._mn_metrics._inc(
                                metric_name=MINION_WORKFLOW_STEP_ABORTED_TOTAL,
                                labels={
                                    LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                    LABEL_MINION_WORKFLOW_STEP: step_name,
                                },
                            )
                        ])
                        raise
                    except Exception as e: # log / measure step failure
                        step_status: ExecutionStatus = "failed"
                        log_kwargs = {
                            "workflow_id": ctx.workflow_id,
                            "step_name": step_name,
                            "step_index": i,
                            "error_type": type(e).__name__,
                            "error_message": str(e),
                            "minion_name": self._mn_name,
                            "minion_instance_id": self._mn_minion_instance_id,
                            "minion_composite_key": self._mn_minion_composite_key,
                            "minion_modpath": self._mn_minion_modpath
                        }
                        tb = sys.exc_info()[2]
                        err_loc = get_user_error_location(tb)
                        if err_loc:
                            log_kwargs.update({
                                "filepath": err_loc["filepath"],
                                "lineno": err_loc["lineno"],
                                "line": err_loc["line"]
                            })
                        else:
                            log_kwargs.update({
                                "traceback": format_exception_traceback(e),
                            })
                        await _shielded_gather(
                            self._mn_logger._log(
                                ERROR,
                                "Workflow Step failed",
                                **log_kwargs
                            ),
                            self._mn_metrics._inc(
                                metric_name=MINION_WORKFLOW_STEP_FAILED_TOTAL,
                                labels={
                                    LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                    LABEL_MINION_WORKFLOW_STEP: step_name,
                                    LABEL_ERROR_TYPE: type(e).__name__,
                                },
                            )
                        )
                        raise
                    else: # log / measure step success & update
                        step_status: ExecutionStatus = "succeeded"
                        await _shielded_gather(*[
                            self._mn_logger._log(
                                DEBUG,
                                "Workflow Step succeeded",
                                workflow_id=ctx.workflow_id,
                                step_name=step_name,
                                step_index=i,
                                minion_name=self._mn_name,
                                minion_instance_id=self._mn_minion_instance_id,
                                minion_composite_key=self._mn_minion_composite_key,
                                minion_modpath=self._mn_minion_modpath
                            ),
                            self._mn_metrics._inc(
                                metric_name=MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
                                labels={
                                    LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                    LABEL_MINION_WORKFLOW_STEP: step_name,
                                },
                            )
                        ])
                    finally: # measure step duration & update inflight gauge (if aborted or failed)
                        if step_start:
                            duration = time.time() - step_start
                        else:
                            # StateStore contexts are persisted with thier started_at time
                            # but it's possible that a context is manipulated then loaded.
                            # Using duration=-1.0 to identify when a loaded context
                            # doesn't have its started_at time when it should.
                            duration = -1.0
                        await self._mn_metrics._observe(
                            metric_name=MINION_WORKFLOW_STEP_DURATION_SECONDS,
                            value=duration,
                            labels={
                                LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                LABEL_MINION_WORKFLOW_STEP: step_name,
                                LABEL_STATUS: step_status,
                            },
                        )
            except asyncio.CancelledError:
                workflow_status: ExecutionStatus = "interrupted"
                raise
            except AbortWorkflow: # log / measure workflow aborted
                workflow_status: ExecutionStatus = "aborted"
                terminal_workflow_log_level = INFO
                terminal_workflow_log_message = "Workflow aborted"
            except Exception as e: # log / measure workflow failure
                workflow_status: ExecutionStatus = "failed"
                if isinstance(e, WorkflowPersistenceNonRetryableError):
                    delete_persisted_context_on_exit = False
                terminal_workflow_log_level = ERROR
                terminal_workflow_log_message = "Workflow failed"
                terminal_workflow_error = e
            else: # log / measure workflow success
                workflow_status: ExecutionStatus = "succeeded"
                terminal_workflow_log_level = INFO
                terminal_workflow_log_message = "Workflow succeeded"
            finally: # measure workflow duration, update inflight gauge, remove context from statestore
                if workflow_status in ("succeeded", "failed", "aborted") and delete_persisted_context_on_exit:
                    try:
                        await self._mn_run_workflow_persistence_checkpoint(
                            ctx,
                            checkpoint="workflow_resolve",
                            operation="delete",
                            block_on_retryable_failure=True,
                        )
                    except asyncio.CancelledError:
                        workflow_status = "interrupted"
                        terminal_workflow_log_level = None
                        terminal_workflow_log_message = None
                        terminal_workflow_error = None
                        raise

                if workflow_status == "aborted" and terminal_workflow_log_message is not None:
                    await _shielded_gather(*[
                        self._mn_logger._log(
                            terminal_workflow_log_level or INFO,
                            terminal_workflow_log_message,
                            workflow_id=ctx.workflow_id,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath
                        ),
                        self._mn_metrics._inc(
                            metric_name=MINION_WORKFLOW_ABORTED_TOTAL,
                            labels={LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key},
                        )
                    ])
                elif workflow_status == "failed" and terminal_workflow_log_message is not None:
                    failure_error = terminal_workflow_error
                    if failure_error is None:
                        failure_error = RuntimeError("workflow failed")
                    await _shielded_gather(*[
                        self._mn_logger._log(
                            terminal_workflow_log_level or ERROR,
                            terminal_workflow_log_message,
                            workflow_id=ctx.workflow_id,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath,
                            error_type=type(failure_error).__name__,
                            error_message=str(failure_error),
                            traceback=format_exception_traceback(failure_error)
                        ),
                        self._mn_metrics._inc(
                            metric_name=MINION_WORKFLOW_FAILED_TOTAL,
                            labels={
                                LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                                LABEL_ERROR_TYPE: type(failure_error).__name__,
                            },
                        )
                    ])
                elif workflow_status == "succeeded" and terminal_workflow_log_message is not None:
                    await _shielded_gather(*[
                        self._mn_logger._log(
                            terminal_workflow_log_level or INFO,
                            terminal_workflow_log_message,
                            workflow_id=ctx.workflow_id,
                            minion_name=self._mn_name,
                            minion_instance_id=self._mn_minion_instance_id,
                            minion_composite_key=self._mn_minion_composite_key,
                            minion_modpath=self._mn_minion_modpath
                        ),
                        self._mn_metrics._inc(
                            metric_name=MINION_WORKFLOW_SUCCEEDED_TOTAL,
                            labels={LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key},
                        )
                    ])

                started_at = ctx.started_at
                if started_at is not None:
                    duration = time.time() - started_at
                else:
                    # Cancellation can happen before step 0 sets started_at.
                    duration = -1.0

                metric_obs = self._mn_metrics._observe(
                    metric_name=MINION_WORKFLOW_DURATION_SECONDS,
                    value=duration,
                    labels={
                        LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key,
                        LABEL_STATUS: workflow_status,
                    },
                )
                await metric_obs
                task = asyncio.current_task()
                if task is not None:
                    async with self._mn_tasks_gate:
                        self._mn_workflow_tasks.discard(task)
                        inflight = len(self._mn_workflow_tasks)
                    if not self._mn_shutting_down:
                        await self._mn_metrics._set(
                            metric_name=MINION_WORKFLOW_INFLIGHT_GAUGE,
                            value=inflight,
                            labels={LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key},
                        )
                self._mn_event_var.reset(event_token)
                self._mn_context_var.reset(context_token)

        def get_user_error_location(tb: TracebackType | None) -> dict | None:
            if not tb:
                return None
            cwd = Path.cwd()
            for frame in reversed(traceback.extract_tb(tb)):
                try:
                    rel_path = Path(frame.filename).resolve().relative_to(cwd)
                except ValueError:
                    continue  # skip frames not under cwd
                if str(rel_path).startswith(str(self._mn_minion_modpath)):
                    return {
                        "filepath": str(rel_path),
                        "lineno": frame.lineno,
                        "line": frame.line,
                    }
            return None

        task = self.safe_create_task(run())
        async with self._mn_tasks_gate:
            self._mn_workflow_tasks.add(task)

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ) -> None:
        self._mn_shutting_down = True
        async def _post():
            async with self._mn_tasks_gate:
                tasks = list(self._mn_workflow_tasks)
            if tasks:
                done, pending = await asyncio.wait(
                    tasks,
                    timeout=self._mn_shutdown_grace_seconds,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            async with self._mn_tasks_gate:
                self._mn_workflow_tasks.clear()
        return await super()._mn_shutdown(
            log_kwargs={"minion_instance_id": self._mn_minion_instance_id},
            post=_post
        )

    async def _mn_handle_event(self, t_event: T_Event):
        # Live events must wait for startup replay to finish; otherwise an event
        # can be persisted before startup completes and then replayed immediately.
        await self._mn_wait_until_started()

        workflow_id = uuid.uuid4().hex

        ctx: MinionWorkflowContext[T_Event, T_Ctx] = MinionWorkflowContext(
            minion_composite_key=self._mn_minion_composite_key,
            minion_modpath=self._mn_minion_modpath,
            workflow_id=workflow_id,
            event=t_event,
            context=type(self)._mn_workflow_ctx_cls(),
            context_cls=type(self)._mn_workflow_ctx_cls
        )

        await self._mn_run_workflow_persistence_checkpoint(
            ctx,
            checkpoint="workflow_start",
        )
        await self._mn_run_workflow(ctx)

        async with self._mn_tasks_gate:
            inflight = len(self._mn_workflow_tasks)
        await self._mn_metrics._set(
            metric_name=MINION_WORKFLOW_INFLIGHT_GAUGE,
            value=inflight,
            labels={LABEL_MINION_COMPOSITE_KEY: self._mn_minion_composite_key},
        )

    async def _mn_wait_until_tasks_idle(
        self,
        timeout: float,
        *,
        include_aux_tasks: bool = False,
        timeout_msg: str,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + timeout

        while True:
            async with self._mn_tasks_gate:
                tasks = tuple(
                    self._mn_workflow_tasks | self._mn_service_tasks
                    if include_aux_tasks else
                    self._mn_workflow_tasks
                )
            if not tasks:
                return

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(timeout_msg)

            done, pending = await asyncio.wait(tasks, timeout=remaining)
            if pending and not done:
                raise TimeoutError(timeout_msg)

    async def _mn_wait_until_workflows_idle(self, timeout: float = 2.0) -> None:
        await self._mn_wait_until_tasks_idle(
            timeout,
            timeout_msg=f"{type(self).__name__} workflows did not become idle within {timeout:.2f}s",
        )

    async def _mn_wait_until_all_tasks_idle(self, timeout: float = 2.0) -> None:
        await self._mn_wait_until_tasks_idle(
            timeout,
            include_aux_tasks=True,
            timeout_msg=f"{type(self).__name__} tasks did not become idle within {timeout:.2f}s",
        )
