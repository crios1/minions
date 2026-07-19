import asyncio
import cmd
import concurrent.futures as cf
import os
import shlex
from collections.abc import Coroutine
from pprint import pprint
from typing import Any, Literal, Optional, TypeAlias, TypeVar, cast

from .gru import Gru, GruRuntimeStateSnapshot
from .gru_result_types import GruResult, StartResult, StopResult

T_Result = TypeVar("T_Result")
T_GruResult = TypeVar("T_GruResult", bound=GruResult)
_Future: TypeAlias = asyncio.Future[T_Result] | cf.Future[T_Result]
_OperationFuture: TypeAlias = _Future[StartResult] | _Future[StopResult]

State = Literal[
    "starting",
    "running",
    "stopping",
    "stopped",
    "failed",
    "aborted",
    "unknown"
]

# TODO: add docstrings for each command so the user has useful info when they use the 'help' command

# TODO: implement design
# DESIGN:
# start ...  → enqueue orchestration, print IDs, return immediately.
# stop ...   → enqueue orchestration, print IDs, return immediately.
# status ... → non-blocking snapshot of current state.
# wait IDs... [--timeout N]
#   → keeps the command pending until each ID leaves its transitional state ("starting"/"stopping").
#   → timeout only ends the wait, never affects the underlying job.
#   → Ctrl+C only ends the wait, never affects the underlying job.
# Rationale:
#   The shell remains responsive for submitting orchestrations,
#   while still giving users an explicit way to wait on their
#   submitted orchestration to resolve.

# TODO: add 'deps' command to GruShell (make a Gru.get_dependencies method)
# gru> deps (prints a dependency graph)
# gru> deps minion MID_OR_MNAME
# gru> deps pipeline PID
# gru> deps resource RID


class GruShell(cmd.Cmd):
    intro = "Welcome to GruShell. Type 'help' or '?' to list commands."
    prompt = "gru> "

    def __init__(self, gru: Gru):
        super().__init__()
        self._gru = gru
        self._loop = gru._loop
        self._shutdown_done: asyncio.Future[bool] = self._loop.create_future()
        self._start_ops: dict[str, _Future[StartResult]] = {}  # id_or_pending -> future
        self._stop_ops: dict[str, _Future[StopResult]] = {}  # id -> future
        self._last_targets: list[str] = []

    # -------- helpers --------

    def _to_argv(self, line: str) -> list[str]:
        return shlex.split(line)

    def _get_orchestration_ids(self) -> list[str]:
        return list(self._runtime_state_snapshot().orchestrations)

    def _runtime_state_snapshot(self) -> GruRuntimeStateSnapshot:
        return asyncio.run_coroutine_threadsafe(
            self._gru.runtime_state_snapshot(),
            self._loop,
        ).result()

    def _submit(self, coro: Coroutine[Any, Any, T_Result]) -> _Future[T_Result]:
        if self._loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self._loop)
        return self._loop.create_task(coro)

    def _future_exception(self, f: _Future[T_Result]) -> BaseException | None:
        try:
            return f.exception()
        except (asyncio.InvalidStateError, cf.InvalidStateError):
            return None

    def _future_result(self, f: _Future[T_Result], timeout: float | None = None) -> T_Result:
        if isinstance(f, cf.Future):
            return f.result(timeout=timeout)
        if timeout is not None and not f.done():
            raise TimeoutError()
        return f.result()

    def _operation_failed(self, f: _Future[T_GruResult]) -> bool:
        if not f.done():
            return False
        if self._future_exception(f):
            return True
        result = self._future_result(f)
        return isinstance(result, GruResult) and not result.success

    def _operation_result(
        self,
        f: _OperationFuture,
        timeout: float | None = None,
    ) -> GruResult:
        if isinstance(f, cf.Future):
            return f.result(timeout=timeout)
        if timeout is not None and not f.done():
            raise TimeoutError()
        return f.result()

    def _wait_on_future_if_any(self, target: str) -> _OperationFuture | None:
        state = self._compute_state(target)
        if target.startswith("pending:") or state == "starting":
            return self._start_ops.get(target)
        if state == "stopping":
            return self._stop_ops.get(target)
        return None

    def _parse_wait_args(self, argv: list[str]) -> tuple[float | None, list[str]]:
        timeout: float | None = None
        targets: list[str] = []
        i = 0
        while i < len(argv):
            arg = argv[i]
            if arg == "--timeout":
                if i + 1 >= len(argv):
                    raise ValueError("Usage: wait [--timeout N] [targets...]")
                timeout = float(argv[i + 1])
                i += 2
                continue
            targets.append(arg)
            i += 1
        return timeout, targets or self._last_targets

    def _compute_state(self, key: str) -> State:
        if key.startswith("pending:"):
            f = self._start_ops.get(key)
            return (
                "failed"
                if (f and self._operation_failed(f))
                else ("starting" if f and not f.done() else "unknown")
            )
        if key in self._stop_ops:
            f = self._stop_ops[key]
            if not f.done():
                return "stopping"
            try:
                self._future_result(f)
                return "stopped"
            except asyncio.CancelledError:
                return "aborted"
            except Exception:
                return "failed"
        if key in self._runtime_state_snapshot().orchestrations:
            return "running"
        return "unknown"

    def _print_summary(self):
        counts: dict[State, int] = {}
        keys = (
            set(self._start_ops)
            | set(self._stop_ops)
            | set(self._runtime_state_snapshot().orchestrations)
        )
        for k in keys:
            s = self._compute_state(k)
            counts[s] = counts.get(s, 0) + 1
        print(" ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "(none)")

    def _print_failed_start_result(self, result: StartResult) -> None:
        print("Cannot start orchestration.")
        if result.reason:
            print()
            print(result.reason)
        if result.suggestion:
            print()
            print("Recommended:")
            print(result.suggestion)
        print()
        print("No workflows were started.")

    # -------- start --------

    def do_start(self, line: str):
        argv = self._to_argv(line)
        if len(argv) != 3:
            print("Usage: start MINION_MODULE_PATH MINION_CONFIG_PATH PIPELINE_MODULE_PATH")
            return

        minion_module_path, minion_config_path, pipeline_module_path = argv
        fut = self._submit(
            self._gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
                minion_config_path=minion_config_path,
            )
        )
        pending_id = f"pending:{id(fut)}"
        self._start_ops[pending_id] = fut
        self._last_targets = [pending_id]
        print("start queued")

        def _cb(f: _Future[StartResult]) -> None:
            try:
                result = self._future_result(f)
            except Exception:
                self._start_ops[pending_id] = f  # keep for status to show 'failed'
                return
            if (
                not isinstance(result, StartResult)
                or not result.success
                or not result.orchestration_id
            ):
                self._start_ops[pending_id] = f  # keep for status to show 'failed'
                if isinstance(result, StartResult):
                    self._print_failed_start_result(result)
                return
            self._start_ops.pop(pending_id, None)
            self._start_ops[result.orchestration_id] = f
            self._last_targets = [result.orchestration_id]

        fut.add_done_callback(_cb)

    def complete_start(self, text: str, line: str, begidx: int, endidx: int):
        return [f for f in os.listdir(".") if f.endswith(".py") and f.startswith(text)]

    # -------- stop --------

    def do_stop(self, line: str):
        ids = self._to_argv(line)
        if not ids:
            print("Usage: stop ORCHESTRATION_ID ...")
            return
        for mid in ids:
            fut = self._submit(self._gru.stop_orchestration(mid))
            self._stop_ops[mid] = fut
        self._last_targets = ids
        print(f"stop queued for {len(ids)}")

    def complete_stop(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_orchestration_ids()

    # -------- status --------

    def do_status(self, line: str):
        argv = self._to_argv(line)
        await_mode = "--await" in argv
        timeout: Optional[float] = None
        if "--timeout" in argv:
            i = argv.index("--timeout")
            timeout = float(argv[i + 1]) if i + 1 < len(argv) else None
        targets = [a for a in argv if not a.startswith("--")] or self._last_targets
        if not targets:
            self._print_summary()
            return

        if not await_mode:
            for t in targets:
                print(f"{t} {self._compute_state(t)}")
            return

        def _wait_on(t: str):
            st = self._compute_state(t)
            if t.startswith("pending:"):
                f = self._start_ops.get(t)
                return f
            if st in ("stopping",):
                return self._stop_ops.get(t)
            if st in ("starting",):
                # started but rekeyed: find its real id if available, else pending future
                f = self._start_ops.get(t)
                return f
            return None  # running/unknown: nothing to wait on

        futs = [f for t in targets if (f := _wait_on(t)) is not None]
        if futs:
            try:
                if timeout is None:
                    for f in futs:
                        self._operation_result(f)
                else:
                    for f in futs:
                        self._operation_result(f, timeout=timeout)
            except Exception as e:
                print(f"status/await error: {e}")

        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_status(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_orchestration_ids() + [
            k for k in self._start_ops if k.startswith("pending:")
        ]

    # -------- wait --------

    def do_wait(self, line: str):
        argv = self._to_argv(line)
        try:
            timeout, targets = self._parse_wait_args(argv)
        except ValueError as e:
            print(e)
            return
        if not targets:
            print("No targets to wait on")
            return

        futs = [f for t in targets if (f := self._wait_on_future_if_any(t)) is not None]

        try:
            if futs:
                cf.wait(
                    [
                        cast(cf.Future[Any], future)
                        for future in futs
                        if isinstance(future, cf.Future)
                    ],
                    timeout=timeout,
                )
        except KeyboardInterrupt:
            print("wait interrupted by user")

        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_wait(self, text: str, line: str, begidx: int, endidx: int):
        return [k for k in self._start_ops if k.startswith("pending:")]

    # -------- metrics --------

    def do_metrics(self, line: str):
        coro = self._gru._metrics._mn_snapshot()
        try:
            if self._loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
                snap = fut.result(timeout=5)
            else:
                snap = self._loop.run_until_complete(coro)
        except Exception as e:
            print(f"metrics error: {e}")
            return
        pprint(snap)
        # TODO: consider simplifying the metrics printed instead of a full dump
        # inflights = sum(
        #     s["value"] for s in snap["gauges"].get("MINION_WORKFLOW_INFLIGHT_GAUGE", [])
        # )
        # succeeded_workflows = sum(
        #     s["value"]
        #     for s in snap["counters"].get("MINION_WORKFLOW_SUCCEEDED_TOTAL", [])
        # )

    # -------- shutdown --------

    def do_shutdown(self, line: str):
        if self._shutdown_done.done():
            print("Shutdown already in progress...")
            return
        print("Shutting down gru and minions...")

        async def _shutdown():
            await self._gru.shutdown()
            if not self._shutdown_done.done():
                self._shutdown_done.set_result(True)

        self._submit(_shutdown())
        return True

    # -------- clear --------

    def do_clear(self, line: str):
        os.system("cls" if os.name == "nt" else "clear")

    # -------- do i still need the following? maybe... --------

    # async def run_until_complete(self) -> bool:
    #     self.cmdloop()
    #     return await self._shutdown_done
