from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import psutil
import uuid

from contextlib import asynccontextmanager
from collections import defaultdict, deque
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, TypeGuard, cast, get_type_hints, overload

from .minion import Minion, WorkflowPersistenceFailurePolicy
from .pipeline import Pipeline
from .resource import Resource
from .types import T_Event, T_Ctx
from .component_identity import get_component_id
from .config_identity import get_config_id
from .gru_result_types import (
    StartResult,
    StopResult,
    ShutdownResult,
    ShutdownError,
)

from .._framework.logger import Logger, DEBUG, INFO, WARNING, ERROR, CRITICAL
from .._framework.logger_noop import NoOpLogger
from .._framework.logger_file import FileLogger

from .._framework.metrics import Metrics
from .._framework.metrics_noop import NoOpMetrics
from .._framework.metrics_prometheus import PrometheusMetrics
from .._framework.metrics_constants import (
    SYSTEM_MEMORY_USED_PERCENT, SYSTEM_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT, PROCESS_CPU_USED_PERCENT
)

from .._framework.async_component import AsyncComponent
from .._framework.minion_workflow_context_codec import WorkflowContextTypeMismatchError
from .._framework.state_store import StateStore
from .._framework.state_store_noop import NoOpStateStore
from .._framework.state_store_sqlite import SQLiteStateStore

from .._utils.safe_cancel_task import safe_cancel_task
from .._utils.get_type_from_hint import get_type_from_hint
from .._utils.safe_create_task import safe_create_task
from .._utils.serialization import (
    require_type_model,
    require_type_serializable,
    serialize,
)
from .._utils.base62_encode import base62_encode

class _UnsetType: ...

_UNSET = _UnsetType()

ORCHESTRATION_ID_VERSION = 1

_gru_instance: Gru | None = None

class Gru:
    """Runtime orchestrator.

    Advanced users can use Gru directly to embed Minions into
    custom async applications. Most users should use `run_shell()`
    or higher-level helpers.

    Concurrency contract:
    - Lifecycle operations on different orchestrations may run concurrently.
    - Lifecycle operations for the same orchestration id are serialized.
    - `shutdown()` is terminal: it waits for in-flight lifecycle work to drain
      while rejecting new start/stop work.
    """
    _allow_direct_init = False

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        state_store: StateStore | None | _UnsetType = _UNSET,
        logger: Logger | None | _UnsetType = _UNSET,
        metrics: Metrics | None | _UnsetType = _UNSET,
        metrics_port: int = 8081,
        workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = "continue-on-failure",
        workflow_persistence_retry_delay_seconds: float = 1.0,
        workflow_persistence_retry_max_delay_seconds: float = 60.0,
        workflow_persistence_retry_backoff_multiplier: float = 2.0,
        workflow_persistence_retry_jitter_ratio: float = 0.1,
        workflow_persistence_retry_warning_interval_seconds: float = 30.0,
        workflow_persistence_retry_error_after_seconds: float | None = 60.0,
    ):
        """
        Args:
            loop: The asyncio event loop used to run all services.
            state_store: Optional StateStore instance. If omitted (left as default), a default SQLiteStateStore is created.
                        Pass None to disable state persistence entirely.
            logger: Optional Logger instance. If omitted, a default logger is created. Pass None to disable logging.
            metrics: Optional Metrics backend. If omitted, PrometheusMetrics is used on the given port.
                    Pass None to disable metrics collection.
            metrics_port: The port to expose Prometheus metrics on (only used if default metrics backend is enabled).
            workflow_persistence_failure_policy: Behavior to use when a workflow checkpoint
                    cannot be persisted. `"continue-on-failure"` keeps the workflow running
                    and retries at the next checkpoint. `"idle-until-persisted"` pauses
                    workflow advancement until persistence succeeds.
            workflow_persistence_retry_delay_seconds: Delay between retry attempts when
                    `workflow_persistence_failure_policy="idle-until-persisted"`.
            workflow_persistence_retry_max_delay_seconds: Maximum delay between retry attempts.
            workflow_persistence_retry_backoff_multiplier: Multiplier applied to retry delay
                    after each retryable persistence failure.
            workflow_persistence_retry_jitter_ratio: Fractional jitter applied to retry
                    delays to avoid synchronized retries.
            workflow_persistence_retry_warning_interval_seconds: Minimum interval for
                    repeated idle persistence warnings.
            workflow_persistence_retry_error_after_seconds: Elapsed idle retry time after
                    which repeated warnings escalate to error logs. Pass None to disable.

        Note:
            This constructor uses a unique internal sentinel (`_UNSET`) to distinguish between omitted arguments and those explicitly set to None.
            This avoids Python's common pitfall with mutable default values and allows safe optional dependency injection.
            # _UNSET lets us distinguish between:
            #  - omitted: use default
            #  - None: explicitly disable
            #  - instance: use as-is

        """

        if not Gru._allow_direct_init:
            raise RuntimeError("Use 'await Gru.create(...)' instead of direct instantiation.")

        self._is_started = False
        self._is_shutdown = False
        self._is_shutting_down = False

        self._workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = (
            Minion._mn_validate_workflow_persistence_failure_policy(
                workflow_persistence_failure_policy,
            )
        )
        self._workflow_persistence_retry_delay_seconds = Minion._mn_validate_positive_seconds(
            "workflow_persistence_retry_delay_seconds",
            workflow_persistence_retry_delay_seconds,
        )
        self._workflow_persistence_retry_max_delay_seconds = Minion._mn_validate_positive_seconds(
            "workflow_persistence_retry_max_delay_seconds",
            workflow_persistence_retry_max_delay_seconds,
        )
        if self._workflow_persistence_retry_max_delay_seconds < self._workflow_persistence_retry_delay_seconds:
            raise ValueError(
                "workflow_persistence_retry_max_delay_seconds must be greater than or equal to "
                "workflow_persistence_retry_delay_seconds"
            )
        self._workflow_persistence_retry_backoff_multiplier = Minion._mn_validate_backoff_multiplier(
            "workflow_persistence_retry_backoff_multiplier",
            workflow_persistence_retry_backoff_multiplier,
        )
        self._workflow_persistence_retry_jitter_ratio = Minion._mn_validate_jitter_ratio(
            "workflow_persistence_retry_jitter_ratio",
            workflow_persistence_retry_jitter_ratio,
        )
        self._workflow_persistence_retry_warning_interval_seconds = Minion._mn_validate_positive_seconds(
            "workflow_persistence_retry_warning_interval_seconds",
            workflow_persistence_retry_warning_interval_seconds,
        )
        self._workflow_persistence_retry_error_after_seconds = Minion._mn_validate_optional_nonnegative_seconds(
            "workflow_persistence_retry_error_after_seconds",
            workflow_persistence_retry_error_after_seconds,
        )

        if logger is _UNSET:
            self._logger = FileLogger()
        elif logger is None:
            self._logger = NoOpLogger()
        elif isinstance(logger, Logger):
            self._logger = logger
        else:
            raise TypeError(f"Invalid logger: {type(logger).__name__}")

        if state_store is _UNSET:
            self._state_store = SQLiteStateStore(db_path="minions.db", logger=self._logger)
        elif state_store is None:
            self._state_store = NoOpStateStore()
        elif isinstance(state_store, StateStore):
            self._state_store = state_store
        else:
            raise TypeError(f"Invalid state_store: {type(state_store).__name__}")
        
        if metrics is _UNSET:
            self._metrics = PrometheusMetrics(logger=self._logger, port=metrics_port)
        elif metrics is None:
            self._metrics = NoOpMetrics()
        elif isinstance(metrics, Metrics):
            self._metrics = metrics
        else:
            raise TypeError(f"Invalid metrics: {type(metrics).__name__}")
        
        global _gru_instance
        if _gru_instance is not None:
            raise RuntimeError("Only one Gru instance is allowed per process.")
        _gru_instance = self

        self._loop = loop

        # Serializes access to Gru's mutable runtime lifecycle state.
        # Use it for writes and for reads that need a coherent snapshot across
        # registries, task maps, relationship maps, refcounts, and cleanup bookkeeping.
        self._runtime_state_lock = asyncio.Lock()

        # Coordinates shutdown with in-flight lifecycle operations.
        self._lifecycle_ops_state_lock = asyncio.Lock()
        self._lifecycle_ops_drained = asyncio.Condition(self._lifecycle_ops_state_lock)
        self._lifecycle_ops_active = 0

        # Lifecycle admission locks.
        # Orchestration locks serialize start/stop for a single orchestration_id
        # Pipeline and resource locks serialize startup for their shared singleton runtime instances
        self._orchestration_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._pipeline_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._resource_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # registries
        self._minions_by_instance_id: dict[str, Minion[Any, Any]] = {}                        # minion_instance_id -> Minion
        self._minions_by_name: dict[str, list[Minion[Any, Any]]] = defaultdict(list) # minion_name -> Minion (minions could have the same name)
        self._minions_by_orchestration_id: dict[str, Minion[Any, Any]] = {}             # orchestration_id -> Minion
        self._minion_tasks: dict[str, asyncio.Task[None]] = {}             # minion_instance_id -> asyncio.Task

        self._pipelines: dict[str, Pipeline[Any]] = {}     # pipeline_id -> Pipeline
        self._pipeline_tasks: dict[str, asyncio.Task[None]] = {} # pipeline_id -> asyncio.Task

        self._resources: dict[str, Resource] = {}          # resource_id -> Resource
        self._resource_tasks: dict[str, asyncio.Task[None]] = {} # resource_id -> asyncio.Task

        # dependency maps used to manage domain object lifecycles
        self._dependency_maps_lock = asyncio.Lock()
        self._minion_pipeline_map: dict[str, str] = {}        # minion_instance_id -> pipeline_id
        self._minion_resource_map: dict[str, set[str]] = {}   # minion_instance_id -> set of resource_ids
        self._pipeline_resource_map: dict[str, set[str]] = {} # pipeline_id -> set of resource_ids
        self._resource_dependencies: dict[str, set[str]] = defaultdict(set) # resource_id -> set(dep_id)
        self._resource_dependents: dict[str, set[str]] = defaultdict(set)   # resource_id -> set(parent_id)
        self._resource_refcounts: dict[str, int] = defaultdict(int)         # total refs (owners + edges)
        
        self._resource_monitor_task = safe_create_task(
            self._monitor_process_resources(),
            self._logger,
            on_failure=self._make_task_failure_hook("resource_monitor", "process"),
        )

    @classmethod
    async def create(
        cls,
        state_store: StateStore | None | _UnsetType = _UNSET,
        logger: Logger | None | _UnsetType = _UNSET,
        metrics: Metrics | None | _UnsetType = _UNSET,
        metrics_port: int = 8081,
        workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = "continue-on-failure",
        workflow_persistence_retry_delay_seconds: float = 1.0,
        workflow_persistence_retry_max_delay_seconds: float = 60.0,
        workflow_persistence_retry_backoff_multiplier: float = 2.0,
        workflow_persistence_retry_jitter_ratio: float = 0.1,
        workflow_persistence_retry_warning_interval_seconds: float = 30.0,
        workflow_persistence_retry_error_after_seconds: float | None = 60.0,
    ) -> "Gru":
        cls._allow_direct_init = True
        try:
            inst = cls(
                loop=asyncio.get_running_loop(),
                state_store=state_store,
                logger=logger,
                metrics=metrics,
                metrics_port=metrics_port,
                workflow_persistence_failure_policy=workflow_persistence_failure_policy,
                workflow_persistence_retry_delay_seconds=workflow_persistence_retry_delay_seconds,
                workflow_persistence_retry_max_delay_seconds=workflow_persistence_retry_max_delay_seconds,
                workflow_persistence_retry_backoff_multiplier=workflow_persistence_retry_backoff_multiplier,
                workflow_persistence_retry_jitter_ratio=workflow_persistence_retry_jitter_ratio,
                workflow_persistence_retry_warning_interval_seconds=workflow_persistence_retry_warning_interval_seconds,
                workflow_persistence_retry_error_after_seconds=workflow_persistence_retry_error_after_seconds,
            )
        finally:
            cls._allow_direct_init = False
        try:
            await inst._startup()
        except Exception:
            global _gru_instance
            if _gru_instance is inst:
                _gru_instance = None
            raise
        return inst

    async def _startup(self):
        if hasattr(self._logger, "_startup"):
            await self._logger._mn_startup()
        await asyncio.gather(
            self._startup_async_component(self._state_store),
            self._startup_async_component(self._metrics)
        )
        self._is_started = True
    
    async def _startup_async_component(self, comp: AsyncComponent, log_kwargs: dict[str, object] | None = None):
        if not hasattr(comp, "_mn_startup"):
            return # pragma: no cover
        log_kwargs = {
            "component": type(comp).__name__,
            **(log_kwargs or {}),
        }
        await self._logger._mn_log(DEBUG, "async component starting", **log_kwargs)
        await comp._mn_startup()
        await self._logger._mn_log(DEBUG, "async component started", **log_kwargs)

    # todo: extract shutdown logic from self.shutdown?
    # or put _startup logic in create?
    async def _shutdown(self):
        ...

    async def _shutdown_async_component(self, comp: AsyncComponent, log_kwargs: dict[str, object] | None = None):
        if not hasattr(comp, "_mn_shutdown"):
            return # pragma: no cover
        log_kwargs = {
            "component": type(comp).__name__,
            **(log_kwargs or {}),
        }
        await self._logger._mn_log(DEBUG, "async component shutting down", **log_kwargs)
        await comp._mn_shutdown()
        await self._logger._mn_log(DEBUG, "async component shutdown complete", **log_kwargs)


    # Minion Methods

    def _make_minion_instance_id(self) -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _make_inline_config_identity(minion_config: object) -> str:
        config_type = type(minion_config)
        require_type_serializable(
            config_type,
            owner="Gru.start_orchestration",
            type_label="minion_config type",
        )
        require_type_model(
            config_type,
            owner="Gru.start_orchestration",
            type_label="minion_config type",
        )

        type_id = f"{config_type.__module__}.{config_type.__qualname__}".encode("utf-8")
        payload = serialize(minion_config, exp_msg_prefix="Gru.start_orchestration minion_config: ")
        digest = hashlib.sha256(type_id + b"\0" + payload).hexdigest()[:16]
        return f"<inline:{digest}>"

    @staticmethod
    def _make_orchestration_id(
        minion_id: str,
        minion_config_id: str,
        pipeline_id: str,
    ) -> str:
        payload = {
            # Version the payload in case ther is a need to migrate IDs in the future.
            "version": ORCHESTRATION_ID_VERSION,
            "minion_id": minion_id,
            "minion_config_id": minion_config_id,
            "pipeline_id": pipeline_id,
        }
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        # Base62 makes the SHA-256-backed ID prettier: alphanumeric and shorter than hex.
        # Left-padding keeps the ID consistently 44 characters long.
        digest = hashlib.sha256(serialized).digest()
        return base62_encode(digest).rjust(44, "0")

    @staticmethod
    def _get_config_identity(minion_config_path: str | None) -> str:
        if not minion_config_path:
            return ""
        p = Path(minion_config_path)
        if minion_config_path.startswith("<inline:"):
            return minion_config_path
        config_id = get_config_id(minion_config_path)
        if config_id is not None:
            return config_id
        try:
            return p.resolve().relative_to(Path.cwd().resolve()).as_posix()
        except ValueError:
            return p.resolve().as_posix()

    @staticmethod
    def _get_component_identity(typ: type[Any], fallback: str) -> str:
        return get_component_id(typ) or fallback

    def _get_minion_class(self, minion_modpath: str) -> type[Minion[Any, Any]]:
        mod = importlib.import_module(minion_modpath)

        def is_minion_class(obj: object) -> TypeGuard[type[Minion[Any, Any]]]:
            return isinstance(obj, type) and issubclass(obj, Minion)

        minion_attr = getattr(mod, "minion", None)
        minion_cls: type[Minion[Any, Any]]

        if minion_attr is None:
            
            minion_classes: list[type[Minion[Any, Any]]] = [
                obj for obj in vars(mod).values()
                if is_minion_class(obj) and obj is not Minion
            ]

            if len(minion_classes) == 1:
                minion_cls = minion_classes[0]
            elif len(minion_classes) == 0:
                raise ImportError(
                    f"Module '{minion_modpath}' must define a `minion` variable or contain at least one subclass of `Minion`."
                )
            else:
                raise ImportError(
                    f"Module '{minion_modpath}' contains multiple Minion subclasses but no explicit `minion` variable to resolve the entrypoint."
                )

        elif is_minion_class(minion_attr):
            minion_cls = minion_attr
        else:
            raise TypeError(f"`minion` attribute in module '{minion_modpath}' is not a subclass of Minion")
        
        return minion_cls

    def _get_minion(
        self,
        minion_instance_id: str,
        orchestration_id: str,
        minion_id: str,
        minion_config_id: str,
        pipeline_id: str,
        minion_modpath: str,
        minion_config_path: str | None,
        inline_minion_config: object | None = None,
        minion_cls: type[Minion[Any, Any]] | None = None,
    ) -> Minion[Any, Any]:
        if minion_cls is None:
            minion_cls = self._get_minion_class(minion_modpath)

        return minion_cls(
            minion_instance_id=minion_instance_id,
            orchestration_id=orchestration_id,
            minion_id=minion_id,
            minion_config_id=minion_config_id,
            pipeline_id=pipeline_id,
            minion_modpath=minion_modpath,
            config_path=minion_config_path,
            inline_config=inline_minion_config,
            state_store=self._state_store,
            metrics=self._metrics,
            logger=self._logger,
            workflow_persistence_failure_policy=self._workflow_persistence_failure_policy,
            workflow_persistence_retry_delay_seconds=self._workflow_persistence_retry_delay_seconds,
            workflow_persistence_retry_max_delay_seconds=self._workflow_persistence_retry_max_delay_seconds,
            workflow_persistence_retry_backoff_multiplier=self._workflow_persistence_retry_backoff_multiplier,
            workflow_persistence_retry_jitter_ratio=self._workflow_persistence_retry_jitter_ratio,
            workflow_persistence_retry_warning_interval_seconds=self._workflow_persistence_retry_warning_interval_seconds,
            workflow_persistence_retry_error_after_seconds=self._workflow_persistence_retry_error_after_seconds,
        )

    async def _start_orchestration_minion(self, minion: Minion[Any, Any]):
        instance_id = minion._mn_minion_instance_id
        name = minion._mn_name

        async with self._runtime_state_lock:
            self._minions_by_instance_id[instance_id] = minion
            if name:
                self._minions_by_name[name].append(minion)

            self._minion_tasks[instance_id] = safe_create_task(
                minion._mn_start(),
                self._logger,
                name=f"minion:{instance_id}",
                on_failure=self._make_task_failure_hook("minion", instance_id),
            )

        await minion._mn_wait_until_started()

    async def _stop_orchestration_minion(self, minion: Minion[Any, Any]):
        instance_id = minion._mn_minion_instance_id
        async with self._runtime_state_lock:
            self._minions_by_instance_id.pop(instance_id, None)

            name = minion._mn_name
            if name:
                minions = self._minions_by_name.get(name, [])
                minions = [m for m in minions if m._mn_minion_instance_id != instance_id]
                if minions:
                    self._minions_by_name[name] = minions
                else:
                    self._minions_by_name.pop(name, None)

            task = self._minion_tasks.pop(instance_id, None)
        if task:
            await safe_cancel_task(task=task, logger=self._logger)
        if not minion._mn_shutting_down:
            await minion._mn_shutdown()

    # TODO: add helper methods for coherent runtime state inspection. Tests
    # should use them to verify minion, pipeline, resource, task, dependency,
    # and refcount state after lifecycle operations instead of reaching into
    # registries directly. A running-minion lookup helper should acquire
    # _runtime_state_lock, check instance ID before name, and account for
    # duplicate names instead of collapsing "not found" and "ambiguous name"
    # into the same None result.


    # Resource Methods

    def _make_resource_id(self, resource_cls: type[Resource]) -> str:
        fallback = f"{resource_cls.__module__}.{resource_cls.__name__}"
        return self._get_component_identity(resource_cls, fallback)

    def _get_direct_resource_dependencies(self, cls: type[Minion[Any, Any] | Pipeline[Any] | Resource]) -> list[type[Resource]]:
        classes: list[type[Resource]] = []
        for _attr, hint in get_type_hints(cls).items():
            r_cls = get_type_from_hint(hint)
            if r_cls is not None and issubclass(r_cls, Resource):
                classes.append(r_cls)
        return classes

    def _get_all_resource_dependencies(self, cls: type[Minion[Any, Any] | Pipeline[Any] | Resource]) -> set[type[Resource]]:
        "get all resource dependencies (direct and indirect)"
        seen: set[type[Resource]] = set()
        stack = list(self._get_direct_resource_dependencies(cls))
        while stack:
            c = stack.pop()
            if c in seen:
                continue # need to prevent cycles from expanding stack forever
            seen.add(c)
            stack.extend(self._get_direct_resource_dependencies(c))
        return seen

    async def _ensure_resource_tree_started(self, resource_cls: type[Resource]) -> Resource:
        seen: set[type[Resource]] = set()
        onpath: set[str] = set()
        start_order: list[type[Resource]] = [] # dependencies before dependents

        stack: list[tuple[type[Resource], bool]] = [(resource_cls, False)]
        while stack:
            cls, expanded = stack.pop()
            rid = self._make_resource_id(cls)

            if expanded:
                if cls in seen:
                    continue
                seen.add(cls)
                onpath.discard(rid)
                start_order.append(cls)
                continue

            if cls in seen:
                continue
            if rid in onpath:
                raise RuntimeError("Cycle detected in Resource dependencies")
            
            onpath.add(rid)
            stack.append((cls, True))

            deps = self._get_direct_resource_dependencies(cls)
            for d in reversed(deps):  # reversed to preserve intuitive L->R order
                stack.append((d, False))

        for cls in start_order:
            rid = self._make_resource_id(cls)
            await self._start_resource(rid, cls)
            
            async with self._runtime_state_lock:
                for dep_cls in self._get_direct_resource_dependencies(cls):
                    dep_id = self._make_resource_id(dep_cls)
                    if dep_id in self._resource_dependencies[rid]:
                        continue
                    self._resource_dependencies[rid].add(dep_id)
                    self._resource_dependents[dep_id].add(rid)
                    self._resource_refcounts[dep_id] += 1

        async with self._runtime_state_lock:
            return self._resources[self._make_resource_id(resource_cls)]

    async def _cleanup_resources(self, candidates: Iterable[str]):
        """
        Attempt to stop resources that become unreferenced, cascading through dependencies.
        A resource is stoppable when its total refcount is 0.
        """
        queue = deque(candidates)
        visited: set[str] = set()
        while queue:
            rid = queue.popleft()
            if rid in visited:
                continue
            visited.add(rid)

            async with self._runtime_state_lock:
                # only attempt stop if running and unreferenced
                if rid not in self._resources:
                    continue
                if self._resource_refcounts.get(rid, 0) > 0:
                    continue

                deps = list(self._resource_dependencies.get(rid, ()))
            await self._stop_resource(rid)

            async with self._runtime_state_lock:
                # Remove edges and decrement dependency refcounts; enqueue deps that hit zero
                for dep_id in deps:
                    self._resource_dependencies[rid].discard(dep_id)
                    self._resource_dependents[dep_id].discard(rid)
                    self._resource_refcounts[dep_id] -= 1
                    if self._resource_refcounts[dep_id] == 0:
                        queue.append(dep_id)
                self._resource_dependencies.pop(rid, None)
                self._resource_dependents.pop(rid, None)
                self._resource_refcounts.pop(rid, None)

    # TODO: in start and stop minion,
    # i need to start resources of resources
    # and i need to determine a depth of resource dependency
    # i can write the logic to have not depth and
    # then realistically, users won't have depth deeper
    # than 2 or 3.
    # Might have solved this already
    # in the sense that cycles are handled gracefully and there is not resource dep depth limit ...
    
    def _get_resource(self):
        ...

    async def _start_resource(self, resource_id: str, resource_cls: type[Resource]) -> Resource:
        async with self._resource_locks[resource_id]:
            created = False
            async with self._runtime_state_lock:
                existing = self._resources.get(resource_id)
                if existing is not None:
                    resource = existing
                else:
                    created = True
                    resource = resource_cls(
                        logger=self._logger,
                        metrics=self._metrics,
                        resource_modpath=f"{resource_cls.__module__}.{resource_cls.__name__}"
                    )
                    self._resources[resource_id] = resource
                    self._resource_tasks[resource_id] = safe_create_task(
                        resource._mn_start(),
                        self._logger,
                        name=f"resource:{resource_id}",
                        on_failure=self._make_task_failure_hook("resource", resource_id),
                    )
            if created:
                await self._logger._mn_log(DEBUG, "Resource starting", resource_id=resource_id)
            await resource._mn_wait_until_started()
            await self._logger._mn_log(DEBUG, "Resource started", resource_id=resource_id)
            return resource

    async def _stop_resource(self, resource_id: str):
        async with self._resource_locks[resource_id]:
            await self._logger._mn_log(DEBUG, "Resource stopping", resource_id=resource_id)
            async with self._runtime_state_lock:
                self._resources.pop(resource_id, None)
                task = self._resource_tasks.pop(resource_id, None)
            if task is not None:
                await safe_cancel_task(task=task, logger=self._logger)
            await self._logger._mn_log(DEBUG, "Resource stopped", resource_id=resource_id)

    def _is_resource_in_use(self, resource_id: str) -> bool:
        # A resource is considered in use if its total reference count is > 0
        return self._resource_refcounts.get(resource_id, 0) > 0


    # Pipeline Methods

    def _make_pipeline_id(self, pipeline_modpath: str) -> str:
        """idempotently make pipeline id"""
        return pipeline_modpath

    def _get_pipeline_identity(self, pipeline_cls: type[Pipeline[Any]], pipeline_modpath: str) -> str:
        return self._get_component_identity(pipeline_cls, pipeline_modpath)

    def _get_pipeline_class(self, pipeline_modpath: str) -> type[Pipeline[Any]]:
        mod = importlib.import_module(pipeline_modpath)

        def is_pipeline_class(obj: object) -> TypeGuard[type[Pipeline[Any]]]:
            return isinstance(obj, type) and issubclass(obj, Pipeline)

        pipeline_attr = getattr(mod, "pipeline", None)
        pipeline_cls: type[Pipeline[Any]]

        if pipeline_attr is None:

            pipeline_classes: list[type[Pipeline[Any]]] = [
                obj for obj in vars(mod).values()
                if is_pipeline_class(obj) and obj is not Pipeline
            ]

            if len(pipeline_classes) == 1:
                pipeline_cls = pipeline_classes[0]
            elif len(pipeline_classes) == 0:
                raise ImportError(
                    f"Module '{pipeline_modpath}' must define a `pipeline` variable or contain at least one subclass of `Pipeline`."
                )
            else:
                raise ImportError(
                    f"Module '{pipeline_modpath}' contains multiple Pipeline subclasses but no explicit `pipeline` variable to resolve the entrypoint."
                )

        elif is_pipeline_class(pipeline_attr):
            pipeline_cls = pipeline_attr
        else:
            raise TypeError(f"`pipeline` attribute in module '{pipeline_modpath}' is not a subclass of Pipeline")
        
        return pipeline_cls

    def _get_pipeline(
        self,
        pipeline_id: str,
        pipeline_modpath: str,
        pipeline_cls: type[Pipeline[Any]] | None = None,
    ) -> Pipeline[Any]:
        if pipeline_id in self._pipelines:
            return self._pipelines[pipeline_id]
        
        if pipeline_cls is None:
            pipeline_cls = self._get_pipeline_class(pipeline_modpath)

        return pipeline_cls(
            pipeline_id=pipeline_id,
            pipeline_modpath=pipeline_modpath,
            metrics=self._metrics,
            logger=self._logger
        )

    async def _start_pipeline(self, pipeline_id: str, pipeline: Pipeline[Any]):
        async with self._pipeline_locks[pipeline_id]:
            created = False
            async with self._runtime_state_lock:
                existing = self._pipelines.get(pipeline_id)
                if existing is not None:
                    pipeline = existing
                else:
                    created = True
                    self._pipelines[pipeline_id] = pipeline
                    self._pipeline_tasks[pipeline_id] = safe_create_task(
                        pipeline._mn_start(),
                        self._logger,
                        name=f"pipeline:{pipeline_id}",
                        on_failure=self._make_task_failure_hook("pipeline", pipeline_id),
                    )
            if created:
                await self._logger._mn_log(DEBUG, "Pipeline starting", pipeline_id=pipeline_id)
            await pipeline._mn_wait_until_started()
            await self._logger._mn_log(DEBUG, "Pipeline started", pipeline_id=pipeline_id)

    async def _stop_pipeline(self, pipeline_id: str):
        released_resource_ids: set[str] = set()
        resource_owner_refs_released = False
        resources_cleaned = False
        try:
            resource_ids: Iterable[str] | None = None
            async with self._pipeline_locks[pipeline_id]:
                await self._logger._mn_log(DEBUG, "Pipeline stopping", pipeline_id=pipeline_id)
                # remove pipeline from active map and cancel its task
                async with self._runtime_state_lock:
                    self._pipelines.pop(pipeline_id, None)
                    task = self._pipeline_tasks.pop(pipeline_id, None)
                    resource_ids = self._pipeline_resource_map.pop(pipeline_id, None)
                if task:
                    await safe_cancel_task(task=task, logger=self._logger)

                # manage resource lifecycle for resources owned by this pipeline
                if resource_ids:
                    released_resource_ids = set(resource_ids)
                    # Decrement owner refs and cleanup
                    async with self._runtime_state_lock:
                        for r_id in resource_ids:
                            self._resource_refcounts[r_id] -= 1
                    resource_owner_refs_released = True
                    await self._cleanup_resources(resource_ids)
                    resources_cleaned = True

                await self._logger._mn_log(DEBUG, "Pipeline stopped", pipeline_id=pipeline_id)
        except Exception:
            await self._finalize_failed_stop_pipeline(
                pipeline_id=pipeline_id,
                resource_owner_refs_released=resource_owner_refs_released,
                resources_cleaned=resources_cleaned,
                released_resource_ids=released_resource_ids,
            )
            raise

    def _is_pipeline_in_use(self, pipeline_id: str) -> bool:
        return pipeline_id in self._minion_pipeline_map.values()


    # Orchestration Lifecycle Helpers

    async def _cleanup_failed_start_orchestration(
        self,
        *,
        minion: Minion[Any, Any] | None,
        pipeline: Pipeline[Any] | None,
        pipeline_id: str | None,
        orchestration_id: str,
        preexisting_pipeline_ids: set[str],
        preexisting_resource_ids: set[str],
    ) -> None:
        async with self._runtime_state_lock:
            self._minions_by_orchestration_id.pop(orchestration_id, None)

        if minion is not None:
            instance_id = minion._mn_minion_instance_id
            async with self._runtime_state_lock:
                self._minion_pipeline_map.pop(instance_id, None)
            if pipeline is not None:
                try:
                    await pipeline._mn_unsubscribe(minion)
                except Exception as e:
                    await self._logger._mn_log_exception(
                        ERROR,
                        "Failed-start cleanup could not unsubscribe minion",
                        e,
                        minion_name=minion._mn_name,
                        minion_instance_id=instance_id,
                        pipeline_id=pipeline_id,
                    )

            async with self._runtime_state_lock:
                resource_ids = self._minion_resource_map.pop(instance_id, None)
                if resource_ids:
                    for resource_id in resource_ids:
                        self._resource_refcounts[resource_id] -= 1
            if resource_ids:
                await self._cleanup_resources_best_effort(resource_ids)

            await self._stop_orchestration_best_effort(minion)

        if pipeline_id is not None and pipeline_id not in preexisting_pipeline_ids:
            await self._stop_pipeline_best_effort(pipeline_id)

        async with self._runtime_state_lock:
            new_resource_ids = set(self._resources) - preexisting_resource_ids
            for resource_id in list(new_resource_ids):
                self._resource_refcounts[resource_id] = 0
        await self._cleanup_resources_best_effort(new_resource_ids)

        self._prune_resource_maps()

    async def _stop_orchestration_best_effort(self, minion: Minion[Any, Any]) -> None:
        try:
            await self._stop_orchestration_minion(minion)
        except Exception as e:
            await self._logger._mn_log_exception(
                ERROR,
                "Failed-start cleanup could not stop minion",
                e,
                minion_name=minion._mn_name,
                minion_instance_id=minion._mn_minion_instance_id,
            )
            await self._discard_orchestration_runtime_state(minion)

    async def _discard_orchestration_runtime_state(self, minion: Minion[Any, Any]) -> None:
        instance_id = minion._mn_minion_instance_id
        async with self._runtime_state_lock:
            self._minions_by_instance_id.pop(instance_id, None)
            if minion._mn_name:
                remaining = [
                    item for item in self._minions_by_name.get(minion._mn_name, [])
                    if item._mn_minion_instance_id != instance_id
                ]
                if remaining:
                    self._minions_by_name[minion._mn_name] = remaining
                else:
                    self._minions_by_name.pop(minion._mn_name, None)
            task = self._minion_tasks.pop(instance_id, None)
        if task is not None:
            try:
                await safe_cancel_task(task=task, logger=self._logger)
            except Exception as e:
                await self._logger._mn_log_exception(
                    ERROR,
                    "Minion task discard cleanup failed",
                    e,
                    minion_name=minion._mn_name,
                    minion_instance_id=instance_id,
                )

    async def _finalize_failed_stop_orchestration(
        self,
        *,
        minion: Minion[Any, Any],
        resource_owner_refs_released: bool,
        resources_cleaned: bool,
        released_resource_ids: Iterable[str] = (),
    ) -> None:
        instance_id = minion._mn_minion_instance_id
        try:
            pipeline_id = self._minion_pipeline_map.pop(instance_id, None)
            if pipeline_id is not None:
                pipeline = self._pipelines.get(pipeline_id)
                if pipeline is not None:
                    try:
                        async with pipeline._mn_subs_lock:
                            pipeline._mn_subs.discard(minion)
                    except Exception as e:
                        await self._logger._mn_log_exception(
                            ERROR,
                            "Stop cleanup could not discard pipeline subscription",
                            e,
                            minion_name=minion._mn_name,
                            minion_instance_id=instance_id,
                            pipeline_id=pipeline_id,
                        )
                if not self._is_pipeline_in_use(pipeline_id):
                    await self._stop_pipeline_best_effort(pipeline_id)

            resource_ids = list(released_resource_ids)
            if not resource_ids:
                resource_ids = list(self._minion_resource_map.pop(instance_id, ()) or ())

            if resource_ids and not resource_owner_refs_released:
                for resource_id in resource_ids:
                    self._resource_refcounts[resource_id] -= 1
                resource_owner_refs_released = True

            if resource_ids and resource_owner_refs_released and not resources_cleaned:
                await self._cleanup_resources_best_effort(resource_ids)

            await self._stop_orchestration_best_effort(minion)
        except Exception as e:
            await self._logger._mn_log_exception(
                ERROR,
                "Stop cleanup failed",
                e,
                minion_name=minion._mn_name,
                minion_instance_id=minion._mn_minion_instance_id,
            )
        finally:
            await self._discard_orchestration_runtime_state(minion)
            self._minions_by_orchestration_id.pop(minion._mn_orchestration_id, None)
            self._prune_resource_maps()


    # Resource Lifecycle Methods

    async def _discard_resource_runtime_state(self, resource_id: str) -> None:
        async with self._runtime_state_lock:
            self._resources.pop(resource_id, None)
            task = self._resource_tasks.pop(resource_id, None)
        if task is not None:
            try:
                await safe_cancel_task(task=task, logger=self._logger)
            except Exception as e:
                await self._logger._mn_log_exception(
                    ERROR,
                    "Resource task discard cleanup failed",
                    e,
                    resource_id=resource_id,
                )

        cascade_discard_ids: list[str] = []
        async with self._runtime_state_lock:
            deps = set(self._resource_dependencies.pop(resource_id, ()))
            for dep_id in deps:
                self._resource_dependents[dep_id].discard(resource_id)
                if dep_id in self._resource_refcounts:
                    self._resource_refcounts[dep_id] -= 1
                    if self._resource_refcounts[dep_id] <= 0 and dep_id in self._resources:
                        cascade_discard_ids.append(dep_id)

            self._resource_dependents.pop(resource_id, None)
            self._resource_refcounts.pop(resource_id, None)
            for dependencies in self._resource_dependencies.values():
                dependencies.discard(resource_id)
            for dependents in self._resource_dependents.values():
                dependents.discard(resource_id)
            for resource_ids in self._minion_resource_map.values():
                resource_ids.discard(resource_id)
            for resource_ids in self._pipeline_resource_map.values():
                resource_ids.discard(resource_id)

        for dep_id in cascade_discard_ids:
            await self._discard_resource_runtime_state(dep_id)

    async def _cleanup_resources_best_effort(self, resource_ids: Iterable[str]) -> None:
        resource_id_list = list(resource_ids)
        try:
            await self._cleanup_resources(resource_id_list)
        except Exception as e:
            try:
                await self._logger._mn_log_exception(
                    ERROR,
                    "Resource cleanup could not stop resources",
                    e,
                    resource_ids=resource_id_list,
                )
            except Exception:
                pass
            for resource_id in resource_id_list:
                if self._resource_refcounts.get(resource_id, 0) <= 0:
                    await self._discard_resource_runtime_state(resource_id)
            self._prune_resource_maps()

    def _prune_resource_maps(self) -> None:
        for mapping in (
            self._resource_dependencies,
            self._resource_dependents,
            self._resource_refcounts,
        ):
            for resource_id in list(mapping):
                if resource_id not in self._resources:
                    mapping.pop(resource_id, None)


    # Pipeline Lifecycle Methods

    async def _stop_pipeline_best_effort(self, pipeline_id: str) -> None:
        if pipeline_id not in self._pipelines and pipeline_id not in self._pipeline_tasks:
            return
        try:
            await self._stop_pipeline(pipeline_id)
        except Exception as e:
            await self._logger._mn_log_exception(
                ERROR,
                "Failed-start cleanup could not stop pipeline",
                e,
                pipeline_id=pipeline_id,
            )
            await self._discard_pipeline_runtime_state(pipeline_id)

    async def _discard_pipeline_runtime_state(self, pipeline_id: str) -> None:
        async with self._runtime_state_lock:
            self._pipelines.pop(pipeline_id, None)
            task = self._pipeline_tasks.pop(pipeline_id, None)
        if task is not None:
            try:
                await safe_cancel_task(task=task, logger=self._logger)
            except Exception as e:
                await self._logger._mn_log_exception(
                    ERROR,
                    "Pipeline task discard cleanup failed",
                    e,
                    pipeline_id=pipeline_id,
                )
        async with self._runtime_state_lock:
            self._pipeline_resource_map.pop(pipeline_id, None)

    async def _finalize_failed_stop_pipeline(
        self,
        *,
        pipeline_id: str,
        resource_owner_refs_released: bool,
        resources_cleaned: bool,
        released_resource_ids: Iterable[str] = (),
    ) -> None:
        async with self._runtime_state_lock:
            resource_ids = list(released_resource_ids)
            if not resource_ids:
                resource_ids = list(self._pipeline_resource_map.pop(pipeline_id, ()) or ())

        if resource_ids and not resource_owner_refs_released:
            async with self._runtime_state_lock:
                for resource_id in resource_ids:
                    self._resource_refcounts[resource_id] -= 1
            resource_owner_refs_released = True

        if resource_ids and resource_owner_refs_released and not resources_cleaned:
            await self._cleanup_resources_best_effort(resource_ids)

        await self._discard_pipeline_runtime_state(pipeline_id)
        self._prune_resource_maps()


    # Runtime State Methods

    def _clear_runtime_state(self) -> None:
        for val in vars(self).values():
            if isinstance(val, (dict, set)):
                val.clear()

    def _runtime_state_snapshot(self) -> dict[str, object]:
        runtime_state = {
            "minions_by_instance_id": self._minions_by_instance_id,
            "minions_by_name": dict(self._minions_by_name),
            "minions_by_orchestration_id": self._minions_by_orchestration_id,
            "minion_tasks": self._minion_tasks,
            "pipelines": self._pipelines,
            "pipeline_tasks": self._pipeline_tasks,
            "resources": self._resources,
            "resource_tasks": self._resource_tasks,
            "minion_pipeline_map": self._minion_pipeline_map,
            "minion_resource_map": self._minion_resource_map,
            "pipeline_resource_map": self._pipeline_resource_map,
            "resource_dependencies": dict(self._resource_dependencies),
            "resource_dependents": dict(self._resource_dependents),
            "resource_refcounts": dict(self._resource_refcounts),
        }
        return {name: value for name, value in runtime_state.items() if value}

    def _runtime_tasks_snapshot(self) -> list[asyncio.Task[None]]:
        return [
            task
            for task in (
                *self._minion_tasks.values(),
                *self._pipeline_tasks.values(),
                *self._resource_tasks.values(),
                getattr(self, "_resource_monitor_task", None),
            )
            if task is not None
        ]

    async def _cancel_runtime_tasks_best_effort(self, tasks: Iterable[asyncio.Task[None]]) -> None:
        for task in tasks:
            try:
                await safe_cancel_task(task=task, logger=self._logger)
            except Exception:
                pass


    # Misc Helper Methods

    def _make_task_failure_hook(self, component: str, identifier: str | None = None):
        async def _hook(exception: BaseException, task_name: str | None) -> None:
            await self._logger._mn_log_exception(
                ERROR,
                "Gru runtime task failure observed",
                exception,
                component=component,
                identifier=identifier,
                task_name=task_name,
            )
        return _hook

    def _ensure_started(self):
        if self._is_shutdown:
            raise RuntimeError(
                "Gru has been shut down. Create a new instance with `await Gru.create(...)`."
            )
        if not self._is_started:
            raise RuntimeError(
                "Gru is not started. Either use `await Gru.create(...)` to construct and start it in one step, "
                "or call `await gru._startup()` manually after instantiating it with `Gru(...)`."
            ) # pragma: no cover

    @asynccontextmanager
    async def _reserve_lifecycle_op(self):
        """Reserve a lifecycle operation slot while it runs.

        Use as:
            async with self._reserve_lifecycle_op() as reserved:
                if not reserved:
                    ...
        """
        async with self._lifecycle_ops_state_lock:
            if self._is_shutdown or self._is_shutting_down:
                yield False
                return
            self._lifecycle_ops_active += 1
        try:
            yield True
        finally:
            async with self._lifecycle_ops_state_lock:
                self._lifecycle_ops_active -= 1
                if self._lifecycle_ops_active == 0:
                    self._lifecycle_ops_drained.notify_all()


    # Public API

    @overload
    async def start_orchestration(
        self,
        pipeline: type[Pipeline[T_Event]],
        minion: type[Minion[T_Event, T_Ctx]],
        *,
        minion_config: object | None = None,
    ) -> StartResult: ...

    @overload
    async def start_orchestration(
        self,
        pipeline: str,
        minion: str,
        *,
        minion_config_path: str | None = None,
    ) -> StartResult: ...

    async def start_orchestration(
        self,
        pipeline: object,
        minion: object,
        *,
        minion_config: object | None = None,
        minion_config_path: str | None = None,
    ) -> StartResult:
        self._ensure_started()
        async with self._reserve_lifecycle_op() as reserved:
            if not reserved:
                return StartResult(
                    success=False,
                    reason="Gru is shutting down.",
                )

            # TODO: consider that i need to log and return StartMinionErrors
            # so raise in try-except or log and return line by line?

            # string based start
            if isinstance(minion, str):
                if not isinstance(pipeline, str):
                    return StartResult(
                        success=False,
                        reason="pipeline must be str when minion is str"
                    )
                if minion_config is not None:
                    return StartResult(
                        success=False,
                        reason="minion_config is only allowed when using Minion and Pipeline subclasses",
                        suggestion="use minion_config_path instead"
                    )
                minion_modpath = minion.strip()
                pipeline_modpath = pipeline.strip()
                minion_config_path = \
                    None if not minion_config_path \
                    else str(Path(minion_config_path.strip()).resolve())
                try:
                    minion_cls: type[Minion[Any, Any]] | None = self._get_minion_class(minion_modpath)
                except Exception:
                    minion_cls = None
                try:
                    pipeline_cls: type[Pipeline[Any]] | None = self._get_pipeline_class(pipeline_modpath)
                except Exception:
                    pipeline_cls = None

            # class based start
            elif isinstance(minion, type) and issubclass(minion, Minion):
                if not (isinstance(pipeline, type) and issubclass(pipeline, Pipeline)):
                    return StartResult(
                        success=False,
                        reason="pipeline must be a Pipeline subclass when minion is a Minion subclass"
                    )
                if minion_config_path is not None:
                    return StartResult(
                        success=False,
                        reason="minion_config_path is only allowed when using module path strings for minion and pipeline",
                        suggestion="use minion_config instead"
                )
                minion_cls = cast(type[Minion[Any, Any]], minion)
                pipeline_cls = cast(type[Pipeline[Any]], pipeline)
                minion_modpath = minion_cls.__module__
                pipeline_modpath = pipeline_cls.__module__
                try:
                    minion_config_path = (
                        self._make_inline_config_identity(minion_config)
                        if minion_config is not None
                        else None
                    )
                except TypeError as e:
                    return StartResult(
                        success=False,
                        reason=str(e),
                    )

            else:
                return StartResult(
                    success=False,
                    reason="minion must be either a Minion subclass or a module path string",
                )

            # TODO: if ram_usage >= self._max_ram_usage: log and return MinionStartResult;

            minion_instance_id = self._make_minion_instance_id()
            minion_identity = (
                self._get_component_identity(minion_cls, minion_modpath)
                if minion_cls is not None
                else minion_modpath
            )
            pipeline_identity = (
                self._get_pipeline_identity(pipeline_cls, pipeline_modpath)
                if pipeline_cls is not None
                else pipeline_modpath
            )
            minion_config_identity = self._get_config_identity(minion_config_path)
            orchestration_id = self._make_orchestration_id(
                minion_id=minion_identity,
                minion_config_id=minion_config_identity,
                pipeline_id=pipeline_identity,
            )
            orchestration_log_kwargs = {
                "orchestration_id": orchestration_id,
                "minion_id": minion_identity,
                "minion_config_id": minion_config_identity,
                "pipeline_id": pipeline_identity,
            }
            minion_inst: Minion[Any, Any] | None = None
            pipeline_inst: Pipeline[Any] | None = None
            pipeline_id: str | None = None

            async with self._orchestration_locks[orchestration_id]:
                async with self._runtime_state_lock:
                    preexisting_pipeline_ids = set(self._pipelines)
                    preexisting_resource_ids = set(self._resources)

                try:
                    # ensure minion is not running
                    async with self._runtime_state_lock:
                        minion_inst = self._minions_by_orchestration_id.get(orchestration_id)
                    if minion_inst:
                        reason = "Orchestration already running - start request was rejected."
                        suggestion = "Use a different config file if you want to launch another instance."
                        minion_instance_id = minion_inst._mn_minion_instance_id
                        minion_name = minion_inst._mn_name
                        await self._logger._mn_log(
                            INFO,
                            "Failed to start orchestration",
                            reason=reason,
                            suggestion=suggestion,
                            minion_name=minion_name,
                            minion_instance_id=minion_instance_id,
                            **orchestration_log_kwargs,
                            minion_modpath=minion_modpath,
                            minion_config_path=minion_config_path,
                            pipeline_modpath=pipeline_modpath,
                        )
                        return StartResult(
                            success=False,
                            reason=reason,
                            suggestion=suggestion,
                            name=minion_name,
                            orchestration_id=orchestration_id,
                        )

                    # ensure minion and pipeline event compatibility
                    minion_inst = self._get_minion(
                        minion_instance_id=minion_instance_id,
                        orchestration_id=orchestration_id,
                        minion_id=minion_identity,
                        minion_config_id=minion_config_identity,
                        pipeline_id=pipeline_identity,
                        minion_modpath=minion_modpath,
                        minion_config_path=minion_config_path,
                        inline_minion_config=minion_config,
                        minion_cls=minion_cls,
                    )
                    pipeline_id = pipeline_identity
                    pipeline_inst = self._get_pipeline(pipeline_id, pipeline_modpath, pipeline_cls=pipeline_cls)

                    if minion_inst._mn_event_cls != pipeline_inst._mn_event_cls:
                        reason = (
                            "Incompatible minion and pipeline event types: "
                            f"pipeline_emits={pipeline_inst._mn_event_cls.__name__}; "
                            f"minion_expects={minion_inst._mn_event_cls.__name__}"
                        )
                        suggestion = "Update the minion or pipeline so they use the same event type."
                        minion_instance_id = minion_inst._mn_minion_instance_id
                        minion_name = minion_inst._mn_name
                        await self._logger._mn_log(
                            INFO,
                            "Failed to start orchestration",
                            reason=reason,
                            suggestion=suggestion,
                            minion_name=minion_name,
                            minion_instance_id=minion_instance_id,
                            **orchestration_log_kwargs,
                            minion_modpath=minion_modpath,
                            minion_config_path=minion_config_path,
                            pipeline_modpath=pipeline_modpath,
                        )
                        return StartResult(
                            success=False,
                            reason=reason,
                            suggestion=suggestion,
                            name=minion_name,
                            orchestration_id=orchestration_id,
                        )

                    await self._logger._mn_log(
                        DEBUG,
                        "Starting orchestration...",
                        **orchestration_log_kwargs,
                        minion_modpath=minion_modpath,
                        minion_config_path=minion_config_path,
                        pipeline_modpath=pipeline_modpath,
                    )

                    async with self._runtime_state_lock:
                        self._minions_by_orchestration_id[orchestration_id] = minion_inst

                    resources_running: list[tuple[str, str, type[Resource]]] = []
                    resources_not_running: list[tuple[str, str, type[Resource]]] = []
                    async with self._runtime_state_lock:
                        for attr, hint in get_type_hints(type(minion_inst)).items():
                            cls = get_type_from_hint(hint)
                            if cls is not None and issubclass(cls, Resource):
                                resource_id = self._make_resource_id(cls)
                                item = (resource_id, attr, cls)
                                if resource_id in self._resources:
                                    resources_running.append(item)
                                else:
                                    resources_not_running.append(item)

                    await asyncio.gather(*[
                        self._ensure_resource_tree_started(cls)
                        for _resource_id, _name, cls in resources_not_running
                    ])

                    for resource_id, name, cls in resources_running + resources_not_running:
                        resource = self._resources.get(resource_id)
                        if resource is None:
                            resource = await self._ensure_resource_tree_started(cls)
                        async with self._runtime_state_lock:
                            resource = self._resources.get(resource_id, resource)
                            setattr(minion_inst, name, resource)
                            self._minion_resource_map.setdefault(minion_instance_id, set()).add(resource_id)
                            self._resource_refcounts[resource_id] += 1

                    async with self._runtime_state_lock:
                        pipeline_running = pipeline_id in self._pipelines

                    if not pipeline_running:
                        pipeline_resources_running: list[tuple[str, str, type[Resource]]] = []
                        pipeline_resources_not_running: list[tuple[str, str, type[Resource]]] = []
                        async with self._runtime_state_lock:
                            for attr, hint in get_type_hints(pipeline_inst.__class__).items():
                                cls = get_type_from_hint(hint)
                                if cls is not None and issubclass(cls, Resource):
                                    resource_id = self._make_resource_id(cls)
                                    item = (resource_id, attr, cls)
                                    if resource_id in self._resources:
                                        pipeline_resources_running.append(item)
                                    else:
                                        pipeline_resources_not_running.append(item)

                        await asyncio.gather(*[
                            self._ensure_resource_tree_started(cls)
                            for _resource_id, _name, cls in pipeline_resources_not_running
                        ])

                        for resource_id, name, cls in pipeline_resources_running + pipeline_resources_not_running:
                            resource = self._resources.get(resource_id)
                            if resource is None:
                                resource = await self._ensure_resource_tree_started(cls)
                            async with self._runtime_state_lock:
                                resource = self._resources.get(resource_id, resource)
                                setattr(pipeline_inst, name, resource)
                                pipeline_resource_ids = self._pipeline_resource_map.setdefault(
                                    pipeline_id,
                                    set(),
                                )
                                if resource_id not in pipeline_resource_ids:
                                    pipeline_resource_ids.add(resource_id)
                                    self._resource_refcounts[resource_id] += 1

                        await self._start_pipeline(pipeline_id, pipeline_inst)
                    else:
                        async with self._runtime_state_lock:
                            pipeline_inst = self._pipelines[pipeline_id]

                    await pipeline_inst._mn_subscribe(minion_inst)

                    async with self._runtime_state_lock:
                        self._minion_pipeline_map[minion_instance_id] = pipeline_id

                    await self._start_orchestration_minion(minion_inst)

                    await self._logger._mn_log(
                        INFO,
                        "Orchestration started",
                        minion_name=minion_inst._mn_name,
                        minion_instance_id=minion_inst._mn_minion_instance_id,
                        **orchestration_log_kwargs,
                        minion_modpath=minion_modpath,
                        minion_config_path=minion_config_path,
                        pipeline_modpath=pipeline_modpath,
                    )

                    return StartResult(
                        success=True,
                        name=minion_inst._mn_name,
                        orchestration_id=orchestration_id,
                    )

                except Exception as e:
                    try:
                        await self._cleanup_failed_start_orchestration(
                            minion=minion_inst,
                            pipeline=pipeline_inst,
                            pipeline_id=pipeline_id,
                            orchestration_id=orchestration_id,
                            preexisting_pipeline_ids=preexisting_pipeline_ids,
                            preexisting_resource_ids=preexisting_resource_ids,
                        )
                    except Exception as cleanup_err:
                        await self._logger._mn_log_exception(
                            ERROR,
                            "Failed-start cleanup raised",
                            cleanup_err,
                            **orchestration_log_kwargs,
                        )

                    await self._logger._mn_log_exception(
                        ERROR,
                        "Failed to start orchestration",
                        e,
                        **orchestration_log_kwargs,
                    )
                    context_type_mismatch_error: WorkflowContextTypeMismatchError | None = None
                    current_error: BaseException | None = e
                    seen_error_ids: set[int] = set()
                    while current_error is not None and id(current_error) not in seen_error_ids:
                        seen_error_ids.add(id(current_error))
                        if isinstance(current_error, WorkflowContextTypeMismatchError):
                            context_type_mismatch_error = current_error
                            break
                        current_error = current_error.__cause__ or current_error.__context__
                    if context_type_mismatch_error is not None:
                        return StartResult(
                            success=False,
                            reason=str(context_type_mismatch_error),
                            suggestion=(
                                "Run the previous compatible code and drain the orchestration "
                                "before starting it with the new types. If these workflows are "
                                "no longer needed, delete the persisted workflow contexts for "
                                f"orchestration {orchestration_id!r} from your configured "
                                "StateStore."
                            ),
                        )
                    return StartResult(
                        success=False,
                        reason=str(e),
                    )

    @overload
    async def start(
        self,
        pipeline: type[Pipeline[T_Event]],
        minion: type[Minion[T_Event, T_Ctx]],
        *,
        minion_config: object | None = None,
    ) -> StartResult: ...

    @overload
    async def start(
        self,
        pipeline: str,
        minion: str,
        *,
        minion_config_path: str | None = None,
    ) -> StartResult: ...

    async def start(
        self,
        pipeline: object,
        minion: object,
        *,
        minion_config: object | None = None,
        minion_config_path: str | None = None,
    ) -> StartResult:
        if isinstance(pipeline, str) and isinstance(minion, str):
            return await self.start_orchestration(
                pipeline=pipeline,
                minion=minion,
                minion_config_path=minion_config_path,
            )

        if isinstance(pipeline, type) and isinstance(minion, type):
            return await self.start_orchestration(
                pipeline=pipeline,
                minion=minion,
                minion_config=minion_config,
            )

        return StartResult(
            success=False,
            reason="pipeline and minion must both be classes or both be strings",
        )

    async def stop_orchestration(self, orchestration_id: str) -> StopResult:
        self._ensure_started()
        async with self._reserve_lifecycle_op() as reserved:
            if not reserved:
                return StopResult(
                    success=False,
                    reason="Gru is shutting down.",
                )

            minion: Minion[Any, Any] | None = None
            stop_committed = False
            resource_owner_refs_released = False
            resources_cleaned = False
            released_resource_ids: set[str] = set()
            try:
                orchestration_id = orchestration_id.strip()

                async with self._runtime_state_lock:
                    minion = self._minions_by_orchestration_id.get(orchestration_id, None)
                    if minion is not None:
                        minion_or_result: Minion[Any, Any] | StopResult = minion
                    else:
                        minion_or_result = StopResult(
                            success=False,
                            reason="No orchestration found with the given ID."
                        )

                if isinstance(minion_or_result, StopResult):
                    result = minion_or_result
                    await self._logger._mn_log(
                        INFO,
                        "Failed to stop orchestration",
                        reason=result.reason,
                        **({"suggestion": result.suggestion} if result.suggestion else {}),
                        attempted_key=orchestration_id,
                    )
                    return result
            
                minion = minion_or_result

                async with self._orchestration_locks[orchestration_id]:
                    await self._logger._mn_log(
                        DEBUG,
                        "Stopping orchestration...",
                        minion_name=minion._mn_name,
                        minion_instance_id=minion._mn_minion_instance_id,
                        **minion._mn_orchestration_log_kwargs(),
                    )

                    # unsub minion from pipeline
                    async with self._runtime_state_lock:
                        pipeline_id = self._minion_pipeline_map.get(
                            minion._mn_minion_instance_id
                        )
                        pipeline = (
                            self._pipelines.get(pipeline_id)
                            if pipeline_id is not None
                            else None
                        )
                    if pipeline_id is None or pipeline is None:
                        reason = "Minion is no longer running."
                        await self._logger._mn_log(
                            INFO,
                            "Failed to stop orchestration",
                            reason=reason,
                            minion_name=minion._mn_name,
                            minion_instance_id=minion._mn_minion_instance_id,
                            **minion._mn_orchestration_log_kwargs(),
                        )
                        return StopResult(
                            success=False,
                            reason=reason,
                        )
                    stop_committed = True
                    await pipeline._mn_unsubscribe(minion)
                    async with self._runtime_state_lock:
                        self._minion_pipeline_map.pop(minion._mn_minion_instance_id, None)

                    # manage pipeline lifecycle
                    if not self._is_pipeline_in_use(pipeline_id):
                        await self._stop_pipeline(pipeline_id)

                    # manage resource lifecycle(s)
                    async with self._runtime_state_lock:
                        resource_ids = self._minion_resource_map.pop(minion._mn_minion_instance_id, None)
                        if resource_ids:
                            released_resource_ids = set(resource_ids)
                            for r_id in resource_ids:
                                self._resource_refcounts[r_id] -= 1 # remove owner ref from minion
                    if resource_ids:
                        resource_owner_refs_released = True
                        await self._cleanup_resources(resource_ids)
                        resources_cleaned = True

                    # stop minion
                    await self._stop_orchestration_minion(minion)
                    async with self._runtime_state_lock:
                        self._minions_by_orchestration_id.pop(orchestration_id, None)

                    await self._logger._mn_log(
                        INFO,
                        "Orchestration stopped",
                        minion_name=minion._mn_name,
                        minion_instance_id=minion._mn_minion_instance_id,
                        **minion._mn_orchestration_log_kwargs(),
                    )

                    return StopResult(success=True)

            except Exception as e:
                if stop_committed and minion is not None:
                    try:
                        await self._finalize_failed_stop_orchestration(
                            minion=minion,
                            resource_owner_refs_released=resource_owner_refs_released,
                            resources_cleaned=resources_cleaned,
                            released_resource_ids=released_resource_ids,
                        )
                    except Exception as cleanup_err:
                        await self._logger._mn_log_exception(
                            ERROR,
                            "Failed-stop cleanup raised",
                            cleanup_err,
                            minion_name=minion._mn_name,
                            minion_instance_id=minion._mn_minion_instance_id,
                            **minion._mn_orchestration_log_kwargs(),
                        )
                stop_log_kwargs: dict[str, object] = {}
                if minion is not None:
                    stop_log_kwargs = {
                        "minion_name": minion._mn_name,
                        "minion_instance_id": minion._mn_minion_instance_id,
                        **minion._mn_orchestration_log_kwargs(),
                    }
                await self._logger._mn_log_exception(
                    ERROR,
                    "Failed to stop orchestration",
                    e,
                    **stop_log_kwargs,
                )
                return StopResult(
                    success=False,
                    reason=str(e),
                )

    async def stop(self, orchestration_id: str) -> StopResult:
        return await self.stop_orchestration(orchestration_id)

    async def shutdown(self) -> ShutdownResult:
        global _gru_instance
        if self._is_shutdown:
            return ShutdownResult(success=True)
        if not self._is_started:
            self._is_shutdown = True
            if _gru_instance is self:
                _gru_instance = None
            return ShutdownResult(success=True)

        # Wait until all in-flight lifecycle operations drain.
        # Condition.wait_for() releases _lifecycle_ops_state_lock while waiting,
        # then re-acquires it before returning.
        if self._is_shutting_down:
            async with self._lifecycle_ops_state_lock:
                await self._lifecycle_ops_drained.wait_for(lambda: self._is_shutdown)
            return ShutdownResult(success=True)
        
        self._is_shutting_down = True
        async with self._lifecycle_ops_state_lock:
            await self._lifecycle_ops_drained.wait_for(lambda: self._lifecycle_ops_active == 0)

        all_tasks = self._runtime_tasks_snapshot()
        try:
            await self._logger._mn_log(INFO, "Gru shutting down...")
            shutdown_errors: list[ShutdownError] = []
            
            async def _collect_phase_errors(phase: str, targets: list[tuple[str, Any]]) -> list[ShutdownError]:
                results = await asyncio.gather(
                    *[op for _, op in targets],
                    return_exceptions=True,
                )
                return [
                    ShutdownError(
                        phase=phase,
                        component=component,
                        error_type=type(result).__name__,
                        error_message=str(result),
                    )
                    for (component, _), result in zip(targets, results)
                    if isinstance(result, Exception)
                ]

            cancel_targets = [
                (
                    t.get_name() if hasattr(t, "get_name") else "task",
                    safe_cancel_task(task=t, logger=self._logger),
                )
                for t in all_tasks
                if t
            ]
            shutdown_errors.extend(await _collect_phase_errors("cancel_task", cancel_targets))

            component_targets = [
                ("state_store", self._shutdown_async_component(self._state_store)),
                ("metrics", self._shutdown_async_component(self._metrics)),
            ]
            shutdown_errors.extend(await _collect_phase_errors("shutdown_component", component_targets))
            
            if shutdown_errors:
                await self._logger._mn_log(
                    ERROR,
                    "Gru shutdown completed with internal errors",
                    error_count=len(shutdown_errors),
                    errors=[asdict(e) for e in shutdown_errors],
                )
            else:
                await self._logger._mn_log(INFO, "Gru shutdown complete")

            await self._logger._mn_shutdown()
            if shutdown_errors:
                return ShutdownResult(
                    success=False,
                    reason=f"Gru shutdown completed with {len(shutdown_errors)} internal error(s).",
                    errors=shutdown_errors,
                )
            return ShutdownResult(success=True)
        except Exception as e:
            await self._logger._mn_log_exception(ERROR, "Gru.shutdown failed", e)
            return ShutdownResult(
                success=False,
                reason=str(e),
                errors=[
                    ShutdownError(
                        phase="shutdown",
                        component="gru",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                ],
            )
        finally:
            await self._cancel_runtime_tasks_best_effort(all_tasks)
            self._clear_runtime_state()
            async with self._lifecycle_ops_state_lock:
                self._is_started = False
                self._is_shutdown = True
                self._is_shutting_down = False
                self._lifecycle_ops_drained.notify_all()
                if _gru_instance is self:
                    _gru_instance = None


    # Background Tasks

    async def _monitor_process_resources(self, interval: int = 5):
        process = psutil.Process()
        process.cpu_percent(interval=None)

        cpu_count = psutil.cpu_count(logical=True)

        warned_ram_high = False
        warned_monitoring_failed = False

        if not cpu_count: # pragma: no cover
            cpu_count = 1
            await self._logger._mn_log(
                WARNING,
                "Unable to determine CPU count. Defaulting to single-core normalization for monitoring CPU usage."
            )

        while True:
            try:
                sys_mem = psutil.virtual_memory()

                sys_mem_used_pct = int(sys_mem.percent)
                sys_cpu_used_pct = int(psutil.cpu_percent(interval=None))

                await self._metrics._mn_set(SYSTEM_MEMORY_USED_PERCENT, sys_mem_used_pct)
                await self._metrics._mn_set(SYSTEM_CPU_USED_PERCENT, sys_cpu_used_pct)

                if sys_mem_used_pct >= 90:
                    if not warned_ram_high:
                        await self._logger._mn_log(
                            WARNING,
                            "System memory usage is very high. This may impact Gru performance or stability.",
                            system_memory_used_percent=sys_mem_used_pct
                        )
                        warned_ram_high = True
                else:
                    warned_ram_high = False

                proc_mem_used_pct = int((process.memory_info().rss / sys_mem.total) * 100)
                proc_cpu_used_pct = int(process.cpu_percent(interval=None) / cpu_count)

                await self._metrics._mn_set(PROCESS_MEMORY_USED_PERCENT, proc_mem_used_pct)
                await self._metrics._mn_set(PROCESS_CPU_USED_PERCENT, proc_cpu_used_pct)

                if warned_monitoring_failed:
                    await self._logger._mn_log(INFO, "Resource monitoring recovered")
                    warned_monitoring_failed = False

            except Exception as e:
                if not warned_monitoring_failed:
                    await self._logger._mn_log_exception(
                        CRITICAL,
                        "Resource monitoring failed (continuing without it)",
                        e,
                    )
                    warned_monitoring_failed = True

            await asyncio.sleep(interval)
