from __future__ import annotations

import asyncio
import contextlib
import gc
import importlib
import psutil
import traceback
import uuid

from collections import defaultdict, deque
from pathlib import Path
from typing import Iterable, Mapping, Any, get_type_hints, overload

from .minion import Minion
from .pipeline import Pipeline
from .resource import Resource
from .types import T_Event, T_Ctx
from .gru_result_types import StartMinionResult, StopMinionResult, ConflictingMinion

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
from .._framework.state_store import StateStore
from .._framework.state_store_noop import NoOpStateStore
from .._framework.state_store_sqlite import SQLiteStateStore

from .._utils.safe_cancel_task import safe_cancel_task
from .._utils.get_class import get_class
from .._utils.safe_create_task import safe_create_task

class _UnsetType: ...

_UNSET = _UnsetType()

_GRU_INSTANCE: Gru | None = None


class Gru:
    """Runtime orchestrator.

    Advanced users can use Gru directly to embed Minions into
    custom async applications. Most users should use `run_shell()`
    or higher-level helpers.
    """
    _allow_direct_init = False

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        state_store: StateStore | None | _UnsetType = _UNSET,
        logger: Logger | None | _UnsetType = _UNSET,
        metrics: Metrics | None | _UnsetType = _UNSET,
        metrics_port: int = 8081,
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

        self._started = False
        self._shutdown = False

        global _GRU_INSTANCE
        if _GRU_INSTANCE is not None:
            raise RuntimeError("Only one Gru instance is allowed per process.")
        _GRU_INSTANCE = self

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
        
        self._loop = loop

        # TODO: use the locks with my datastructures to make helpers and use them in my public method implementations

        # per-entity ID locks
        self._minion_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._pipeline_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._resource_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # registries
        self._minions_by_id: dict[str, Minion] = {}                        # minion_instance_id -> Minion
        self._minions_by_name: dict[str, list[Minion]] = defaultdict(list) # minion_name -> Minion (minions could have the same name)
        self._minions_by_composite_key: dict[str, Minion] = {}             # composite_key -> Minion
        self._minion_tasks: dict[str, asyncio.Task] = {}                   # minion_instance_id -> asyncio.Task

        self._pipelines: dict[str, Pipeline] = {}          # pipeline_id -> Pipeline
        self._pipeline_tasks: dict[str, asyncio.Task] = {} # pipeline_id -> asyncio.Task

        self._resources: dict[str, Resource] = {}          # resource_id -> Resource
        self._resource_tasks: dict[str, asyncio.Task] = {} # resource_id -> asyncio.Task

        # dependency maps used to manage domain object lifecycles
        self._dependency_maps_lock = asyncio.Lock()
        self._minion_pipeline_map: dict[str, str] = {}        # minion_instance_id -> pipeline_id
        self._minion_resource_map: dict[str, set[str]] = {}   # minion_instance_id -> set of resource_ids
        self._pipeline_resource_map: dict[str, set[str]] = {} # pipeline_id -> set of resource_ids
        self._resource_dependencies: dict[str, set[str]] = defaultdict(set) # resource_id -> set(dep_id)
        self._resource_dependents: dict[str, set[str]] = defaultdict(set)   # resource_id -> set(parent_id)
        self._resource_refcounts: dict[str, int] = defaultdict(int)         # total refs (owners + edges)
        
        self._resource_monitor_task = safe_create_task(self._monitor_process_resources(), self._logger)

    @classmethod
    async def create(
        cls,
        state_store: StateStore | None | _UnsetType = _UNSET,
        logger: Logger | None | _UnsetType = _UNSET,
        metrics: Metrics | None | _UnsetType = _UNSET,
        metrics_port: int = 8081,
    ) -> "Gru":
        cls._allow_direct_init = True
        try:
            inst = cls(
                loop=asyncio.get_running_loop(),
                state_store=state_store,
                logger=logger,
                metrics=metrics,
                metrics_port=metrics_port,
            )
        finally:
            cls._allow_direct_init = False
        await inst._startup()
        if hasattr(gc, "freeze"):
            gc.freeze()
        return inst

    async def _startup(self):
        if hasattr(self._logger, "_startup"):
            await self._logger._mn_startup()

        startups = [
            self._startup_async_component(
                self._state_store,
                "StateStore",
                {'state_store': self._state_store.__class__.__name__}
            ),
            self._startup_async_component(
                self._metrics,
                "Metrics",
                {'metrics': self._metrics.__class__.__name__}
            )
        ]
        await asyncio.gather(*startups)

        self._started = True
    
    async def _startup_async_component(self, comp: AsyncComponent, comp_kind: str, log_kwargs: dict | None = None):
        if not hasattr(comp, "_startup"):
            return # pragma: no cover
        log_kwargs = log_kwargs or {}
        await self._logger._log(DEBUG, f"{comp_kind} starting", **log_kwargs)
        await comp._mn_startup()
        await self._logger._log(DEBUG, f"{comp_kind} started", **log_kwargs)

    async def _shutdown_async_component(self, comp: AsyncComponent, comp_kind: str, log_kwargs: dict | None = None):
        if not hasattr(comp, "_shutdown"):
            return # pragma: no cover
        log_kwargs = log_kwargs or {}
        await self._logger._log(DEBUG, f"{comp_kind} shutting down", **log_kwargs)
        await comp._mn_shutdown()
        await self._logger._log(DEBUG, f"{comp_kind} shutdown complete", **log_kwargs)

    # Minion Methods

    def _make_minion_instance_id(self) -> str:
        return uuid.uuid4().hex

    def _make_minion_composite_key(self, minion_modpath: str, minion_config_path: str | None, pipeline_modpath: str) -> str:
        "conceptually composite_key is project-root-relative identity (with absolute fallbacks)"
        if minion_config_path == "<inline>":
            cfg = "<inline>"
        elif minion_config_path:
            p = Path(minion_config_path)
            try:
                cfg = p.resolve().relative_to(Path.cwd().resolve()).as_posix()
            except ValueError:
                cfg = p.resolve().as_posix()
        else:
            cfg = ""
        return f"{minion_modpath}|{cfg}|{pipeline_modpath}"


    def _get_minion_class(self, minion_modpath: str) -> type[Minion]:
        mod = importlib.import_module(minion_modpath)

        minion_cls = getattr(mod, "minion", None)

        if minion_cls is None:
            
            minion_classes = [
                obj for obj in vars(mod).values()
                if isinstance(obj, type) and issubclass(obj, Minion) and obj is not Minion
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

        elif not isinstance(minion_cls, type) or not issubclass(minion_cls, Minion):
            raise TypeError(f"`minion` attribute in module '{minion_modpath}' is not a subclass of Minion")
        
        return minion_cls

    def _get_minion(
        self,
        minion_instance_id: str,
        minion_composite_key: str,
        minion_modpath: str,
        minion_config_path: str
    ) -> Minion:
        minion_cls = self._get_minion_class(minion_modpath)

        return minion_cls(
            minion_instance_id=minion_instance_id,
            minion_composite_key=minion_composite_key,
            minion_modpath=minion_modpath,
            config_path=minion_config_path,
            state_store=self._state_store,
            metrics=self._metrics,
            logger=self._logger
        )

    async def _start_minion(self, minion: Minion):
        id = minion._mn_minion_instance_id
        name = minion._mn_name

        self._minions_by_id[id] = minion
        if name:
            self._minions_by_name[name].append(minion)

        self._minion_tasks[id] = safe_create_task(minion._mn_start())

        await minion._mn_wait_until_started()

    async def _stop_minion(self, minion: Minion):
        instance_id = minion._mn_minion_instance_id
        self._minions_by_id.pop(instance_id, None)

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

    # Resource Methods

    def _make_resource_id(self, resource_cls: type[Resource]) -> str:
        return f"{resource_cls.__module__}.{resource_cls.__name__}"

    def _get_direct_resource_dependencies(self, cls: type[Minion | Pipeline | Resource]) -> list[type[Resource]]:
        classes = []
        for attr, hint in get_type_hints(cls).items():
            r_cls = get_class(hint)
            if isinstance(r_cls, type) and issubclass(r_cls, Resource):
                classes.append(r_cls)
        return classes

    def _get_all_resource_dependencies(self, cls: type[Minion | Pipeline | Resource]) -> set[type[Resource]]:
        "get all resource dependencies (direct and indirect)"
        seen = set()
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
            if rid in self._resources:
                continue

            await self._start_resource(rid, cls)
            
            for dep_cls in self._get_direct_resource_dependencies(cls):
                dep_id = self._make_resource_id(dep_cls)
                if dep_id in self._resource_dependencies[rid]:
                    continue
                self._resource_dependencies[rid].add(dep_id)
                self._resource_dependents[dep_id].add(rid)
                self._resource_refcounts[dep_id] += 1

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

            # only attempt stop if running and unreferenced
            if rid not in self._resources:
                continue
            if self._resource_refcounts.get(rid, 0) > 0:
                continue

            deps = list(self._resource_dependencies.get(rid, ()))
            await self._stop_resource(rid)

            # Remove edges and decrement dependency refcounts; enqueue deps that hit zero
            for dep_id in deps:
                self._resource_dependencies[rid].discard(dep_id)
                self._resource_dependents[dep_id].discard(rid)
                self._resource_refcounts[dep_id] -= 1
                if self._resource_refcounts[dep_id] == 0:
                    queue.append(dep_id)

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
        await self._logger._log(DEBUG, "Resource starting", resource_id=resource_id)
        resource = resource_cls(
            logger=self._logger,
            metrics=self._metrics,
            resource_modpath=f"{resource_cls.__module__}.{resource_cls.__name__}"
        )
        self._resources[resource_id] = resource
        self._resource_tasks[resource_id] = safe_create_task(resource._mn_start())
        await resource._mn_wait_until_started()
        await self._logger._log(DEBUG, "Resource started", resource_id=resource_id)
        return resource

    async def _stop_resource(self, resource_id: str):
        await self._logger._log(DEBUG, "Resource stopping", resource_id=resource_id)
        self._resources.pop(resource_id)
        task = self._resource_tasks.pop(resource_id)
        await safe_cancel_task(task=task, logger=self._logger)
        await self._logger._log(DEBUG, "Resource stopped", resource_id=resource_id)

    def _is_resource_in_use(self, resource_id: str) -> bool:
        # A resource is considered in use if its total reference count is > 0
        return self._resource_refcounts.get(resource_id, 0) > 0

    # Pipeline Methods

    def _make_pipeline_id(self, pipeline_modpath: str) -> str:
        return pipeline_modpath

    def _get_pipeline_class(self, pipeline_modpath: str) -> type[Pipeline]:
        mod = importlib.import_module(pipeline_modpath)

        pipeline_cls = getattr(mod, "pipeline", None)

        if pipeline_cls is None:

            pipeline_classes = [
                obj for obj in vars(mod).values()
                if isinstance(obj, type) and issubclass(obj, Pipeline) and obj is not Pipeline
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

        elif not isinstance(pipeline_cls, type) or not issubclass(pipeline_cls, Pipeline):
            raise TypeError(f"`pipeline` attribute in module '{pipeline_modpath}' is not a subclass of Pipeline")
        
        return pipeline_cls

    def _get_pipeline(self, pipeline_id: str, pipeline_modpath: str) -> Pipeline:
        if pipeline_id in self._pipelines:
            return self._pipelines[pipeline_id]
        
        pipeline_cls = self._get_pipeline_class(pipeline_modpath)

        return pipeline_cls(
            pipeline_id=pipeline_id,
            pipeline_modpath=pipeline_modpath,
            metrics=self._metrics,
            logger=self._logger
        )

    async def _start_pipeline(self, pipeline_id: str, pipeline: Pipeline):
        await self._logger._log(DEBUG, "Pipeline starting", pipeline_id=pipeline_id)
        self._pipelines[pipeline_id] = pipeline
        self._pipeline_tasks[pipeline_id] = safe_create_task(pipeline._mn_start())
        await pipeline._mn_wait_until_started()
        await self._logger._log(DEBUG, "Pipeline started", pipeline_id=pipeline_id)

    async def _stop_pipeline(self, pipeline_id: str):
        await self._logger._log(DEBUG, "Pipeline stopping", pipeline_id=pipeline_id)
        # remove pipeline from active map and cancel its task
        self._pipelines.pop(pipeline_id, None)
        task = self._pipeline_tasks.pop(pipeline_id, None)
        if task:
            await safe_cancel_task(task=task, logger=self._logger)

        # manage resource lifecycle for resources owned by this pipeline
        if (resource_ids := self._pipeline_resource_map.pop(pipeline_id, None)):
            # Decrement owner refs and cleanup
            for r_id in resource_ids:
                self._resource_refcounts[r_id] -= 1
            await self._cleanup_resources(resource_ids)

        await self._logger._log(DEBUG, "Pipeline stopped", pipeline_id=pipeline_id)

    def _is_pipeline_in_use(self, pipeline_id: str) -> bool:
        return pipeline_id in self._minion_pipeline_map.values()

    # Helper Methods

    async def _log_exception_with_context(self, exc: BaseException, msg: str):
        trace = getattr(exc, "__cause__", None) or exc
        context = getattr(exc, "context", {})

        await self._logger._log(
            ERROR,
            msg,
            error_type=type(trace).__name__,
            error_message=str(trace),
            traceback="".join(traceback.format_exception(type(trace), trace, trace.__traceback__)),
            **context
        )

    def _ensure_started(self):
        if self._shutdown:
            raise RuntimeError(
                "Gru has been shut down. Create a new instance with `await Gru.create(...)`."
            )
        if not self._started:
            raise RuntimeError(
                "Gru is not started. Either use `await Gru.create(...)` to construct and start it in one step, "
                "or call `await gru._startup()` manually after instantiating it with `Gru(...)`."
            ) # pragma: no cover

    # Public API

    # TODO: might need a per minion lock or some thing for the public endpoints

    # TODO: finish implementing start_minion string and class based starts

    @overload
    async def start_minion(
        self,
        minion: type[Minion[T_Event, T_Ctx]],
        pipeline: type[Pipeline[T_Event]],
        *,
        minion_config: Mapping[str, Any] | None = None,
    ) -> StartMinionResult: ...
    
    @overload
    async def start_minion(
        self,
        minion: str,
        pipeline: str,
        *,
        minion_config_path: str | None = None,
    ) -> StartMinionResult: ...

    async def start_minion(
        self,
        minion: type[Minion[Any, Any]] | str,
        pipeline: type[Pipeline[Any]] | str,
        *,
        minion_config: Mapping[str, Any] | None = None,
        minion_config_path: str | None = None,
    ) -> StartMinionResult:
        self._ensure_started()

        # string based start
        if isinstance(minion, str):
            if not isinstance(pipeline, str):
                return StartMinionResult(
                    success=False,
                    reason="pipeline must be str when minion is str"
                )
            if minion_config is not None:
                return StartMinionResult(
                    success=False,
                    reason="minion_config is only allowed when using Minion and Pipeline subclasses",
                    suggestion="use minion_config_path instead"
                )
            minion_modpath = minion.strip()
            pipeline_modpath = pipeline.strip()
            minion_config_path = \
                None if not minion_config_path \
                else str(Path(minion_config_path.strip()).resolve())

        # class based start
        elif isinstance(minion, type) and issubclass(minion, Minion):
            if not (isinstance(pipeline, type) and issubclass(pipeline, Pipeline)):
                return StartMinionResult(
                    success=False,
                    reason="pipeline must be a Pipeline subclass when minion is a Minion subclass"
                )
            if minion_config_path is not None:
                return StartMinionResult(
                    success=False,
                    reason="minion_config_path is only allowed when using module path strings for minion and pipeline",
                    suggestion="use minion_config instead"
                )
            minion_modpath = minion.__module__
            pipeline_modpath = pipeline.__module__
            minion_config_path = "<inline>" if minion_config else None

        else:
            return StartMinionResult(
                success=False,
                reason="minion must be either a Minion subclass or a module path string",
            )

        # TODO: if ram_usage >= self._max_ram_usage: log and return MinionStartResult;

        minion_instance_id = self._make_minion_instance_id()
        minion_composite_key = self._make_minion_composite_key(minion_modpath, minion_config_path, pipeline_modpath)

        # ...

        # prev sig: 
        # async def start_minion(self, minion_modpath: str, minion_config_path: str, pipeline_modpath: str) -> StartMinionResult:
        try:
            # ensure minion is not running

            # todo: will prob rename this and refs to _minion
            minion = self._minions_by_composite_key.get(minion_composite_key)
            if minion:
                reason = "Minion already running — start request was skipped."
                suggestion = "Use a different config file if you want to launch another instance."
                minion_instance_id = minion._mn_minion_instance_id
                minion_name = minion._mn_name
                await self._logger._log(
                    INFO,
                    "Failed to start minion",
                    reason=reason,
                    suggestion=suggestion,
                    minion_name=minion_name,
                    minion_instance_id=minion_instance_id,
                    minion_composite_key=minion_composite_key,
                    minion_modpath=minion_modpath,
                    minion_config_path=minion_config_path,
                    pipeline_modpath=pipeline_modpath
                )
                return StartMinionResult(
                    success=False,
                    reason=reason,
                    suggestion=suggestion,
                    name=minion_name,
                    instance_id=minion_instance_id
                )

            # ensure minion and pipeline event compatibility

            minion = self._get_minion(
                minion_instance_id=minion_instance_id,
                minion_composite_key=minion_composite_key,
                minion_modpath=minion_modpath,
                minion_config_path=minion_config_path
            )
            
            pipeline_id = self._make_pipeline_id(pipeline_modpath)
            pipeline = self._get_pipeline(pipeline_id, pipeline_modpath)
            
            if minion._mn_event_cls != pipeline._mn_event_cls:

                reason = (
                    f"Incompatible minion and pipeline event types:\n"
                    f"  Pipeline emits: {pipeline._mn_event_cls.__name__}\n"
                    f"  Minion expects: {minion._mn_event_cls.__name__}"
                )
                suggestion = "Update the minion or pipeline so they use the same event type."
                minion_instance_id = minion._mn_minion_instance_id
                minion_name = minion._mn_name
                await self._logger._log(
                    INFO,
                    "Failed to start minion",
                    reason=reason,
                    suggestion=suggestion,
                    minion_name=minion_name,
                    minion_instance_id=minion_instance_id,
                    minion_composite_key=minion_composite_key,
                    minion_modpath=minion_modpath,
                    minion_config_path=minion_config_path,
                    pipeline_modpath=pipeline_modpath
                )
                return StartMinionResult(
                    success=False,
                    reason=reason,
                    suggestion=suggestion,
                    name=minion_name,
                    instance_id=minion_instance_id
                )

            await self._logger._log(
                DEBUG,
                "Starting minion...",
                minion_composite_key=minion_composite_key,
                minion_modpath=minion_modpath,
                minion_config_path=minion_config_path,
                pipeline_modpath=pipeline_modpath
            )

            self._minions_by_composite_key[minion_composite_key] = minion

            # ensure required resources are started and minion resource relationships are tracked
            resources_running: list[tuple[str, str, type[Resource]]] = []     # (id, name, class)
            resources_not_running: list[tuple[str, str, type[Resource]]] = [] # (id, name, class)
            for attr, hint in get_type_hints(type(minion)).items():
                cls = get_class(hint)
                if isinstance(cls, type) and issubclass(cls, Resource):
                    id = self._make_resource_id(cls)
                    item = (id, attr, cls)
                    if id in self._resources:
                        resources_running.append(item)
                    else:
                        resources_not_running.append(item)
                
            # Start any missing resource trees (includes transitive dependencies)
            await asyncio.gather(*[
                self._ensure_resource_tree_started(cls)
                for id, name, cls in resources_not_running
            ])

            for id, name, cls in resources_running + resources_not_running:
                setattr(minion, name, self._resources[id])
                self._minion_resource_map.setdefault(minion_instance_id, set()).add(id)
                self._resource_refcounts[id] += 1  # owner ref from minion

            # ensure pipeline resources are started and pipeline-resource relationships are tracked
            pipeline_resources_running: list[tuple[str, str, type[Resource]]] = []     # (id, name, class)
            pipeline_resources_not_running: list[tuple[str, str, type[Resource]]] = [] # (id, name, class)
            for attr, hint in get_type_hints(pipeline.__class__).items():
                cls = get_class(hint)
                if isinstance(cls, type) and issubclass(cls, Resource):
                    id = self._make_resource_id(cls)
                    item = (id, attr, cls)
                    if id in self._resources:
                        pipeline_resources_running.append(item)
                    else:
                        pipeline_resources_not_running.append(item)

            await asyncio.gather(*[
                self._ensure_resource_tree_started(cls)
                for id, name, cls in pipeline_resources_not_running
            ])

            for id, name, cls in pipeline_resources_running + pipeline_resources_not_running:
                setattr(pipeline, name, self._resources[id])
                self._pipeline_resource_map.setdefault(pipeline_id, set()).add(id)
                self._resource_refcounts[id] += 1  # owner ref from pipeline

            # ensure pipeline is started, minion is subscribed, and minion pipeline relationship is tracked
            if pipeline_id not in self._pipelines:
                await self._start_pipeline(pipeline_id, pipeline)
            else:
                pipeline = self._pipelines[pipeline_id]

            await pipeline._mn_subscribe(minion)

            self._minion_pipeline_map[minion_instance_id] = pipeline_id

            # start minion
            await self._start_minion(minion)

            await self._logger._log(
                INFO,
                "Minion started",
                minion_name=minion._mn_name,
                minion_instance_id=minion._mn_minion_instance_id,
                minion_composite_key=minion._mn_minion_composite_key,
                minion_modpath=minion_modpath,
                minion_config_path=minion_config_path,
                pipeline_modpath=pipeline_modpath
            )

            return StartMinionResult(
                success=True,
                name=minion._mn_name,
                instance_id=minion._mn_minion_instance_id,
            )
        
        except Exception as e:
            # TODO: will need to clear entries in like self._minions_by_composite_key
            # to cover the case where there is a memory leak due to an unexpected exception happening
            # do it for every public method
            self._minions_by_composite_key.pop(minion_composite_key, None)
            self._minions_by_id
            self._minions_by_id
            self._minion_resource_map

            await self._log_exception_with_context(e, "Failed to start minion")
            return StartMinionResult(
                success=False,
                reason=str(e)
            )

    async def stop_minion(self, name_or_instance_id: str) -> StopMinionResult:
        self._ensure_started()

        name_or_instance_id = name_or_instance_id.strip()

        try:
            # ensure minion is running

            def _resolve_minion(name_or_id: str) -> Minion | StopMinionResult:
                minion = self._minions_by_id.get(name_or_id, None)
                if minion:
                    return minion

                minions = self._minions_by_name.get(name_or_id, [])
                if not minions:
                    return StopMinionResult(
                        success=False,
                        reason="No minion found with the given name or instance ID."
                    )

                if len(minions) == 1:
                    return minions[0]
                else:
                    return StopMinionResult(
                        success=False,
                        reason="Multiple minions found with the same name.",
                        suggestion="Use the full instance ID to stop the intended minion.",
                        conflicts=[
                            ConflictingMinion(
                                instance_id=m._mn_minion_instance_id,
                                modpath=m._mn_minion_modpath,
                                config_modpath=m._mn_config_path,
                                pipeline_modpath=self._pipelines[self._minion_pipeline_map[m._mn_minion_instance_id]]._mn_pipeline_modpath
                            ) for m in minions
                        ]
                    )

            minion_or_result = _resolve_minion(name_or_instance_id)

            if isinstance(minion_or_result, StopMinionResult):
                result = minion_or_result
                await self._logger._log(
                    INFO,
                    "Failed to stop minion",
                    reason=result.reason,
                    **({"suggestion": result.suggestion} if result.suggestion else {}),
                    attempted_key=name_or_instance_id
                )
                return result
        
            minion = minion_or_result
        
            await self._logger._log(
                DEBUG,
                "Stopping minion...",
                minion_name=minion._mn_name,
                minion_instance_id=minion._mn_minion_instance_id
            )

            # unsub minion from pipeline
            pipeline_id = self._minion_pipeline_map.pop(minion._mn_minion_instance_id)
            pipeline = self._pipelines[pipeline_id]
            await pipeline._mn_unsubscribe(minion)

            # manage pipeline lifecycle
            if not self._is_pipeline_in_use(pipeline_id):
                await self._stop_pipeline(pipeline_id)

            # manage resource lifecycle(s)
            if (resource_ids := self._minion_resource_map.pop(minion._mn_minion_instance_id, None)):
                for r_id in resource_ids:
                    self._resource_refcounts[r_id] -= 1 # remove owner ref from minion
                await self._cleanup_resources(resource_ids)

            # stop minion
            await self._stop_minion(minion)

            await self._logger._log(
                INFO,
                "Minion stopped",
                minion_name=minion._mn_name,
                minion_instance_id=minion._mn_minion_instance_id
            )

            return StopMinionResult(success=True)

        except Exception as e: # pragma: no cover
            await self._log_exception_with_context(e, "Failed to stop minion")
            return StopMinionResult(
                success=False,
                reason=str(e),
            )

    async def shutdown(self):
        self._ensure_started()

        try:
            await self._logger._log(INFO, "Gru shutting down...")
            all_tasks = [
                *self._minion_tasks.values(),
                *self._pipeline_tasks.values(),
                *self._resource_tasks.values(),
                getattr(self, "_resource_monitor_task", None)
            ]

            with contextlib.suppress(Exception):
                await asyncio.gather(
                    *[safe_cancel_task(task=t, logger=self._logger) for t in all_tasks if t],
                    return_exceptions=True,
                )           
                shutdowns = [
                    self._shutdown_async_component(
                        self._state_store,
                        "StateStore",
                        {'state_store': self._state_store.__class__.__name__}
                    ),
                    self._shutdown_async_component(
                        self._metrics,
                        "Metrics",
                        {'metrics': self._metrics.__class__.__name__}
                    )
                ]
                await asyncio.gather(*shutdowns, return_exceptions=True)
            
            for key, val in vars(self).items():
                if isinstance(val, (dict, set)):
                    val.clear()

            await self._logger._log(INFO, "Gru shutdown complete")

            await self._logger._mn_shutdown()

        except Exception as e: # pragma: no cover
            await self._log_exception_with_context(e, "Gru.shutdown failed")
        finally:
            self._started = False
            self._shutdown = True
            global _GRU_INSTANCE
            if _GRU_INSTANCE is self:
                _GRU_INSTANCE = None

    # Background Tasks

    async def _monitor_process_resources(self, interval: int = 5):
        # https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/69067169-91cc-8331-8cea-542e6cb5d10e
        # https://chatgpt.com/g/g-p-6843ab69c6f081918162f6743a0722c4-minions-dev/c/69091dbb-94bc-832c-b5f8-b18c8c5fc012
        # TODO: on high memory usage:
        # - "turn off" pipelines (so new workflows don't spawn) & let the user know what's going on
        # TODO: when memory usage returns to reasonable levels,
        # - "turn on" pipelines
        # ...consider how you'd manage potential thrashing between turning pipelines on and off cuz ram usage
        # ...could increase significantly when turning pipelines back on
        # ...maybe turn them on gradually or wait til ram reaches a minimum threashold before turning pipelines back on?

        process = psutil.Process()
        process.cpu_percent(interval=None)

        cpu_count = psutil.cpu_count(logical=True)

        warned_ram_high = False
        warned_monitoring_failed = False

        if not cpu_count: # pragma: no cover
            cpu_count = 1
            await self._logger._log(
                WARNING,
                "Unable to determine CPU count. Defaulting to single-core normalization for monitoring CPU usage."
            )

        while True:
            try:
                sys_mem = psutil.virtual_memory()

                sys_mem_used_pct = int(sys_mem.percent)
                sys_cpu_used_pct = int(psutil.cpu_percent(interval=None))

                await self._metrics._set(SYSTEM_MEMORY_USED_PERCENT, sys_mem_used_pct)
                await self._metrics._set(SYSTEM_CPU_USED_PERCENT, sys_cpu_used_pct)

                if sys_mem_used_pct >= 90:
                    if not warned_ram_high:
                        await self._logger._log(
                            WARNING,
                            f"System memory usage is very high. This may impact Gru performance or stability.",
                            system_memory_used_percent=sys_mem_used_pct
                        )
                        warned_ram_high = True
                else:
                    warned_ram_high = False

                proc_mem_used_pct = int((process.memory_info().rss / sys_mem.total) * 100)
                proc_cpu_used_pct = int(process.cpu_percent(interval=None) / cpu_count)

                await self._metrics._set(PROCESS_MEMORY_USED_PERCENT, proc_mem_used_pct)
                await self._metrics._set(PROCESS_CPU_USED_PERCENT, proc_cpu_used_pct)

                if warned_monitoring_failed:
                    await self._logger._log(INFO, "Resource monitoring recovered")
                    warned_monitoring_failed = False

            except Exception as e:
                if not warned_monitoring_failed:
                    await self._logger._log(
                        CRITICAL,
                        "Resource monitoring failed (continuing without it)",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        traceback="".join(traceback.format_exception(type(e), e, e.__traceback__))
                    )
                    warned_monitoring_failed = True

            await asyncio.sleep(interval)
