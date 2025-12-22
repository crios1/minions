import ast
import inspect
import textwrap
from abc import ABC
from typing import TypeVar, Callable, Awaitable

from .._domain.exceptions import UnsupportedUserCode, MinionsError
from .._utils.get_relative_module_path import get_relative_module_path

T = TypeVar("T")

class AsyncLifecycle(ABC):
    """
    Framework base class for async lifecycle components.

    Definition-time validation applies only to user-facing domain classes
    and is restricted to established user-facing inheritance chains.
    """
    _mn_user_facing = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__module__.startswith("minions._internal."):
            return
        if not cls._mn_user_facing:
            return
        if not any(getattr(base, "_mn_user_facing", False) for base in cls.__mro__[1:]):
            return
        cls._mn_ensure_attrspace()
        cls._mn_validate_class_user_code(cls.__module__)

    @classmethod
    def _mn_ensure_attrspace(cls):
        "Ensure no user-defined class attributes or annotations violate the reserved _mn_ attrspace."
        names = {**cls.__dict__, **getattr(cls, "__annotations__", {})}
        allowed = {"_mn_user_facing"} # allows subclasses of user facing classes to be user facing too
        bad = {
            n for n in names
            if isinstance(n, str)
            and n.startswith("_mn_")
            and n not in allowed
        }
        if bad:
            modpath = f"{cls.__module__}.{cls.__qualname__}"
            names = ", ".join(f"`{cls.__name__}.{n}`" for n in sorted(bad))
            raise UnsupportedUserCode(
                f"Invalid attribute assignment: {names} in `{modpath}`. "
                f"Attributes starting with `_mn_` are reserved for internal Minions runtime use."
            )

    @classmethod
    def _mn_validate_class_user_code(cls, modpath: str | None = None):
        modpath = modpath or cls.__module__
        isfunction = inspect.isfunction

        for name, attr in cls.__dict__.items():
            if not name or not name[0].isalpha():
                continue

            func = getattr(attr, "__func__", attr)  # unwrap staticmethod/classmethod if present
            if not isfunction(func):
                continue  # avoids builtins / descriptors that don't have user code

            cls._mn_validate_user_code(func, modpath)

    @classmethod
    def _mn_validate_user_code(cls, func: Callable, modpath: str):
        try:
            src = textwrap.dedent(inspect.getsource(func))
            tree = ast.parse(src)
        except (OSError, TypeError, IndentationError) as e:
            raise UnsupportedUserCode(
                f"Could not validate source of function `{func.__name__}` ({modpath}): {e}"
            )

        banned_task_fns = {"create_task", "ensure_future"}
        banned_exit_names = {"exit", "quit", "_exit", "SystemExit"}
        banned_exit_attrs = {
            ("sys", "exit"), ("os", "_exit"), ("builtins", "exit"),
            ("builtins", "quit"), ("builtins", "SystemExit")
        }

        for node in ast.walk(tree):

            if isinstance(node, ast.Call):
                f = node.func

                if isinstance(f, ast.Attribute):
                    owner = getattr(f.value, "id", None)
                    if owner in {"asyncio", "aio"} and f.attr in banned_task_fns:
                        raise UnsupportedUserCode(
                            f"Unsupported use of `{owner}.{f.attr}` in `{func.__name__}` ({modpath}). "
                            "Use `self.safe_create_task(...)` instead."
                        )
                    if (owner, f.attr) in banned_exit_attrs:
                        name = f"{owner}.{f.attr}"
                        raise UnsupportedUserCode(
                            f"Unsupported use of `{name}` in `{func.__name__}` ({modpath}). "
                            "If you want to abort an in-flight workflow, raise an AbortWorkflow exception. "
                            "If you want to stop a minion, run stop_minion."
                        )
                    if owner == "object" and f.attr == "__setattr__":
                        if len(node.args) >= 2 and isinstance(node.args[0], ast.Name) and node.args[0].id == "self":
                            a1 = node.args[1]
                            if isinstance(a1, ast.Constant) and isinstance(a1.value, str) and a1.value.startswith("_mn_"):
                                raise UnsupportedUserCode(
                                    f"Invalid attribute assignment: `self.{a1.value}` in `{func.__name__}` ({modpath}). "
                                    "Attributes starting with `_mn_` are reserved for framework use."
                                )

                if isinstance(f, ast.Name):
                    if f.id in banned_task_fns:
                        raise UnsupportedUserCode(
                            f"Unsupported use of `{f.id}` in `{func.__name__}` ({modpath}). "
                            "Use `self.safe_create_task(...)` instead."
                        )
                    if f.id in banned_exit_names:
                        raise UnsupportedUserCode(
                            f"Unsupported use of `{f.id}` in `{func.__name__}` ({modpath}). "
                            "If you want to abort an in-flight workflow, raise an AbortWorkflow exception. "
                            "If you want to stop a minion, run stop_minion."
                        )
                    if f.id == "setattr" and len(node.args) >= 2:
                        a0, a1 = node.args[0], node.args[1]
                        if isinstance(a0, ast.Name) and a0.id == "self":
                            if isinstance(a1, ast.Constant) and isinstance(a1.value, str) and a1.value.startswith("_mn_"):
                                raise UnsupportedUserCode(
                                    f"Invalid attribute assignment: `self.{a1.value}` in `{func.__name__}` ({modpath}). "
                                    "Attributes starting with `_mn_` are reserved for framework use."
                                )

            if isinstance(node, ast.Raise):
                exc = node.exc
                if isinstance(exc, ast.Name) and exc.id == "SystemExit":
                    raise UnsupportedUserCode(
                        f"Unsupported use of `raise SystemExit` in `{func.__name__}` ({modpath}). "
                        "If you want to abort an in-flight workflow, raise an AbortWorkflow exception. "
                        "If you want to stop a minion, run stop_minion."
                    )
                if isinstance(exc, ast.Call) and isinstance(exc.func, ast.Name) and exc.func.id == "SystemExit":
                    raise UnsupportedUserCode(
                        f"Unsupported use of `raise SystemExit(...)` in `{func.__name__}` ({modpath}). "
                        "If you want to abort an in-flight workflow, raise an AbortWorkflow exception. "
                        "If you want to stop a minion, run stop_minion."
                    )

            if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                continue

            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if isinstance(t, ast.Attribute):
                    owner = getattr(t.value, "id", None)
                    if owner != "self":
                        continue
                    if not (isinstance(t.attr, str) and t.attr.startswith("_mn_")):
                        continue
                    raise UnsupportedUserCode(
                        f"Invalid attribute assignment: `self.{t.attr}` in `{func.__name__}` ({modpath}). "
                        f"Attributes starting with `_mn_` are reserved for framework use."
                    )

                if not isinstance(t, ast.Subscript):
                    continue

                base = t.value
                if not (
                    isinstance(base, ast.Attribute)
                    and isinstance(base.value, ast.Name)
                    and base.value.id == "self"
                    and base.attr == "__dict__"
                ):
                    continue

                k = t.slice
                key = getattr(k, "value", None) if isinstance(k, ast.Constant) else None
                if not (isinstance(key, str) and key.startswith("_mn_")):
                    continue

                raise UnsupportedUserCode(
                    f"Invalid attribute assignment: `self.{key}` in `{func.__name__}` ({modpath}). "
                    f"Attributes starting with `_mn_` are reserved for framework use."
                )

    @staticmethod
    def _mn_raise_not_implemented(method_name: str, cls: type):
        raise NotImplementedError(
            f"{cls.__name__}.{method_name} must be implemented in a subclass and "
            f"should only be called via _{method_name}(), which ensures logging and lifecycle safety."
        )

    async def _mn_run_lifecycle_phase(
        self,
        *,
        name: str,
        lifecyle_method: Callable[[], Awaitable[T]],
        log_kwargs: dict | None = None,
        pre: Callable[..., T | Awaitable[T]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., T | Awaitable[T]] | None = None,
        post_args: list | None = None,
    ):
        try:
            if pre:
                pre_args = pre_args or []
                result = pre(*pre_args) if pre else None
                if inspect.isawaitable(result):
                    await result
            await lifecyle_method()
            if post:
                post_args = post_args or []
                result = post(*post_args)
                if inspect.isawaitable(result):
                    await result
        except Exception as e:
            log_kwargs = log_kwargs or {}
            rel_modpath = get_relative_module_path(type(self))
            raise MinionsError(
                f"{type(self).__name__}.{name} failed ({rel_modpath})",
                context=log_kwargs,
            ) from e

    async def _mn_startup(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., T | Awaitable[T]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., T | Awaitable[T]] | None = None,
        post_args: list | None = None
    ):
        pre_args = pre_args or []
        async def _pre():
            self._mn_validate_user_code(self.startup, type(self).__module__)
            self._mn_validate_user_code(self.shutdown, type(self).__module__)
            if pre:
                result = pre(*pre_args)
                if inspect.isawaitable(result):
                    await result
        
        await self._mn_run_lifecycle_phase(
            name="startup",
            lifecyle_method=self.startup,
            log_kwargs=log_kwargs,
            pre=_pre,
            post=post,
            post_args=post_args,
        )

    async def _mn_shutdown(
        self,
        *,
        log_kwargs: dict | None = None,
        pre: Callable[..., T | Awaitable[T]] | None = None,
        pre_args: list | None = None,
        post: Callable[..., T | Awaitable[T]] | None = None,
        post_args: list | None = None
    ):
        await self._mn_run_lifecycle_phase(
            name="shutdown",
            lifecyle_method=self.shutdown,
            log_kwargs=log_kwargs,
            pre=pre,
            pre_args=pre_args,
            post=post,
            post_args=post_args,
        )

    async def startup(self):
        "Prepare internal state or dependencies"
        # self._raise_not_implemented("startup", type(self))

    async def shutdown(self):
        "Clean up anything async-allocated in startup"
        # self._raise_not_implemented("shutdown", type(self))
