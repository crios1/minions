import asyncio
import contextvars
import inspect
import sys
import time
import traceback
import uuid
from pathlib import Path
from types import TracebackType
from typing import (
    Any, Awaitable, Callable, ClassVar,
    Generic, Literal, Type,
    get_args, get_origin, get_type_hints
)

from minions._internal._framework.logger import DEBUG

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
    MINION_WORKFLOW_STEP_STARTED_TOTAL, MINION_WORKFLOW_STEP_INFLIGHT_GAUGE,
    MINION_WORKFLOW_STEP_ABORTED_TOTAL, MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    LABEL_MINION_INSTANCE_ID, LABEL_MINION_WORKFLOW_STEP,
    LABEL_ERROR_TYPE
)
from .._framework.state_store import StateStore
from .._utils.get_class import get_class
from .._utils.serialization import is_type_serializable

Statuses = Literal["undefined", "aborted", "failed", "succeeded"]

class Minion(AsyncService, Generic[T_Event, T_Ctx]):
    _mn_event_var: contextvars.ContextVar[T_Event] = contextvars.ContextVar("minion_pipeline_event")
    _mn_context_var: contextvars.ContextVar[T_Ctx] = contextvars.ContextVar("minion_workflow_context")
    _mn_event_cls: Type[T_Event]
    _mn_workflow_ctx_cls: Type[T_Ctx]

    _mn_workflow_spec: ClassVar[tuple[str, ...] | None] = None # tuple of ordered workflow step names
    _mn_defer_minion_setup: ClassVar[bool] = False

    def __init_subclass__(cls, *, defer_minion_setup=False, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._mn_ensure_attrspace()

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

        base = None
        bases = getattr(cls, '__orig_bases__', ())
        if minion_bases := [b for b in bases if get_origin(b) is Minion]:
            if len(minion_bases) > 1:
                raise multi_inheritance_err
            base = minion_bases[0]
        elif sub_minion_bases := [b for b in bases if (get_origin(b) is not None and issubclass(get_origin(b), Minion))]:
            if len(sub_minion_bases) > 1:
                raise multi_inheritance_err
            base = sub_minion_bases[0]
        else:
            raise no_event_or_ctx_types_err
        
        args = get_args(base)
        if len(args) < 2:
            raise no_event_or_ctx_types_err

        cls._mn_event_cls = args[0]
        cls._mn_workflow_ctx_cls = args[1]

        # TODO: write test where subclasses of Minion subclasses are created/rejected
        for base in cls.__mro__[1:]:
            if base is Minion:
                break
            if issubclass(base, Minion) and not getattr(base, "_mn_defer_minion_setup", False):
                raise TypeError(
                    f"{cls.__name__} must subclass Minion directly. "
                    f"Subclasses of Minion subclasses are not supported."
                )

        if not is_type_serializable(cls._mn_event_cls):
            raise TypeError(
                f"{cls.__name__}: event type is not JSON-serializable. "
                "Only JSON-safe types are supported (str, int, float, bool, None, list, tuple, dict[str, V], dataclass, TypedDict)."
            )
        
        if cls._mn_event_cls in (str, int, float, bool, type(None)):
            raise TypeError(f"{cls.__name__}: event type must be a structured type, not a primitive")

        if not is_type_serializable(cls._mn_workflow_ctx_cls):
            raise TypeError(
                f"{cls.__name__}: workflow context is not JSON-serializable. "
                "Only JSON-safe types are supported (str, int, float, bool, None, list, tuple, dict[str, V], dataclass, TypedDict)."
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

        # TODO: i need to decide if i'll let workflows be constructed with steps throughout the inheritance chain
        # or if workflows should be constructed strictly from the "tip" of the inheritance chain (i can see benefits to both approaches)
        # but currently i'm keeping it simple by requiring workflow steps only be declared on the inheritance chain "tip"
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
        for name in cls._mn_workflow_spec:
            fn = cls.__dict__[name]
            cls._mn_validate_user_code(fn, modpath)

    def __init__(
        self,
        minion_instance_id: str,
        minion_composite_key: str,
        minion_modpath: str,
        config_path: str,
        state_store: StateStore,
        metrics: Metrics,
        logger: Logger
    ):
        super().__init__(logger)

        name = getattr(type(self), "name", None)
        if name is not None and not isinstance(name, str):
            raise TypeError(f"{type(self).__name__}.name must be a string, got {type(name).__name__}")
        self._mn_name = name

        # TODO: i'm thinking of droping minion from each of these props
        # or add minion to _config_path prop
        self._mn_minion_instance_id = minion_instance_id
        self._mn_minion_composite_key = minion_composite_key
        self._mn_minion_modpath = minion_modpath
        self._mn_config_path = config_path
        self._mn_config = None
        self._mn_config_lock = asyncio.Lock()
        self._mn_state_store = state_store
        self._mn_metrics = metrics
        self._mn_tasks: set[asyncio.Task] = set()
        self._mn_tasks_lock = asyncio.Lock()

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
    ):
        async def _pre():
            self._mn_validate_user_code(self._mn_load_config, type(self).__module__)
            self._mn_config = await self._mn_load_config(self._mn_config_path)
        
        async def _post():
            contexts = await self._mn_state_store._load_all_contexts()
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
                        import tomli as tomllib  # fallback for <3.11
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

    async def _mn_run_workflow(self, ctx: MinionWorkflowContext[T_Event, T_Ctx]):
        async def run():
            event_token = self._mn_event_var.set(ctx.event)
            context_token = self._mn_context_var.set(ctx.context)
            workflow_status: Statuses = "undefined"
            try: # run workflow (step by step)
                if ctx.step_index == 0:
                    await asyncio.gather(*[
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
                            LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id
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
                
                for i in range(ctx.step_index, len(workflow)):
                    if i == 0:
                        ctx.started_at = time.time()
                    ctx.step_index = i

                    step = workflow[i]
                    step_name = step.__name__
                    step_start = ctx.started_at if i == 0 else time.time()
                    step_status: Statuses = "undefined"

                    await asyncio.gather(*[
                        self._mn_logger._log(
                            DEBUG,
                            f"Workflow Step started",
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
                            LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                            LABEL_MINION_WORKFLOW_STEP=step_name
                        )
                    ])

                    try: # run step / store context
                        await asyncio.shield(self._mn_state_store._save_context(ctx))
                        await step()
                    except AbortWorkflow: # log / measure step aborted
                        step_status: Statuses = "aborted"
                        await asyncio.gather(*[
                            self._mn_logger._log(
                                INFO,
                                f"Workflow Step aborted",
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
                                LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                                LABEL_MINION_WORKFLOW_STEP=step_name
                            )
                        ])
                        raise
                    except Exception as e: # log / measure step failure
                        step_status: Statuses = "failed"
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
                                "traceback": "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                            })
                        await asyncio.gather(
                            self._mn_logger._log(
                                ERROR,
                                f"Workflow Step failed",
                                **log_kwargs
                            ),
                            self._mn_metrics._inc(
                                metric_name=MINION_WORKFLOW_STEP_FAILED_TOTAL,
                                LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                                LABEL_MINION_WORKFLOW_STEP=step_name,
                                LABEL_ERROR_TYPE=type(e).__name__,
                            )
                        )
                        raise
                    else: # log / measure step success & update
                        step_status: Statuses = "succeeded"
                        await asyncio.gather(*[
                            self._mn_logger._log(
                                DEBUG,
                                f"Workflow Step succeeded",
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
                                LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                                LABEL_MINION_WORKFLOW_STEP=step_name
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
                            LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                            LABEL_MINION_WORKFLOW_STEP=step_name,
                            status=step_status # TODO: is this what I want to do or put in a LABEL_MINION_WORKFLOW_STEP_STATUS ?
                        )
            except AbortWorkflow: # log / measure workflow aborted
                workflow_status: Statuses = "aborted"
                await asyncio.gather(*[
                    self._mn_logger._log(
                        INFO,
                        "Workflow aborted",
                        workflow_id=ctx.workflow_id,
                        minion_name=self._mn_name,
                        minion_instance_id=self._mn_minion_instance_id,
                        minion_composite_key=self._mn_minion_composite_key,
                        minion_modpath=self._mn_minion_modpath
                    ),
                    self._mn_metrics._inc(
                        metric_name=MINION_WORKFLOW_ABORTED_TOTAL,
                        LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id
                    )
                ])
            except Exception as e: # log / measure workflow failure
                workflow_status: Statuses = "failed"
                await asyncio.gather(*[
                    self._mn_logger._log(
                        ERROR,
                        "Workflow failed",
                        workflow_id=ctx.workflow_id,
                        minion_name=self._mn_name,
                        minion_instance_id=self._mn_minion_instance_id,
                        minion_composite_key=self._mn_minion_composite_key,
                        minion_modpath=self._mn_minion_modpath,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        traceback="".join(traceback.format_exception(type(e), e, e.__traceback__))
                    ),
                    self._mn_metrics._inc(
                        metric_name=MINION_WORKFLOW_FAILED_TOTAL,
                        LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                        LABEL_ERROR_TYPE=type(e).__name__
                    )
                ])
            else: # log / measure workflow success
                workflow_status: Statuses = "succeeded"
                await asyncio.gather(*[
                    self._mn_logger._log(
                        INFO,
                        "Workflow succeeded",
                        workflow_id=ctx.workflow_id,
                        minion_name=self._mn_name,
                        minion_instance_id=self._mn_minion_instance_id,
                        minion_composite_key=self._mn_minion_composite_key,
                        minion_modpath=self._mn_minion_modpath
                    ),
                    self._mn_metrics._inc(
                        metric_name=MINION_WORKFLOW_SUCCEEDED_TOTAL,
                        LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id
                    )
                ])
            finally: # measure workflow duration, update inflight gauge, remove context from statestore
                duration = time.time() - ctx.started_at # type: ignore[reportOperatorIssue]
                await asyncio.gather(*[
                    self._mn_metrics._observe(
                        metric_name=MINION_WORKFLOW_DURATION_SECONDS,
                        value=duration,
                        LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id,
                        status=workflow_status # TODO: is this what I want to do or put in a LABEL_MINION_WORKFLOW_STEP_STATUS ?
                    ),
                    await asyncio.shield(self._mn_state_store._delete_context(ctx.workflow_id))
                ])
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

        def discard_task(task: asyncio.Task):
            self._mn_tasks.discard(task)
            self.safe_create_task(
                self._mn_metrics._set(
                    metric_name=MINION_WORKFLOW_INFLIGHT_GAUGE,
                    value=len(self._mn_tasks),
                    LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id
                )
            )

        task = self.safe_create_task(run())
        task.add_done_callback(lambda t: discard_task(t))
        async with self._mn_tasks_lock:
            self._mn_tasks.add(task)

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., Any | Awaitable[Any]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., Any | Awaitable[Any]] | None = None,
        post_args: list | None = None
    ):
        async def _post():
            async with self._mn_tasks_lock:
                for task in self._mn_tasks:
                    task.cancel()
                await asyncio.gather(*self._mn_tasks, return_exceptions=True)
                self._mn_tasks.clear()
        return await super()._mn_shutdown(
            log_kwargs={"minion_instance_id": self._mn_minion_instance_id},
            post=_post
        )

    async def _mn_handle_event(self, t_event: T_Event):
        workflow_id = uuid.uuid4().hex

        ctx: MinionWorkflowContext[T_Event, T_Ctx] = MinionWorkflowContext(
            minion_modpath=self._mn_minion_modpath,
            workflow_id=workflow_id,
            event=t_event,
            context=type(self)._mn_workflow_ctx_cls(),
            context_cls=type(self)._mn_workflow_ctx_cls
        )

        try:
            await asyncio.shield(self._mn_state_store.save_context(ctx))
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                "StateStore failed to save minion workflow context",
                suggestion="Ensure that your event and context types are supported by your state store.",
                state_store=self._mn_state_store.__name__,
                event_type=T_Event,
                context_type=T_Ctx,
                error_type=type(e).__name__,
                error_message=str(e),
                traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
                minion_name=self._mn_name,
                minion_instance_id=self._mn_minion_instance_id,
                minion_composite_key=self._mn_minion_composite_key,
                minion_modpath=self._mn_minion_modpath
            )
            return

        await self._mn_run_workflow(ctx)

        await self._mn_metrics._set(
            metric_name=MINION_WORKFLOW_INFLIGHT_GAUGE,
            value=len(self._mn_tasks),
            LABEL_MINION_INSTANCE_ID=self._mn_minion_instance_id
        )
