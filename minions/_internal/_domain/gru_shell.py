import asyncio
import cmd
import os
import shlex
import concurrent.futures as cf

from pprint import pprint
from typing import Literal, Optional

from .gru import Gru

State = Literal[
    "starting","running","stopping",
    "stopped","failed","aborted","unknown"
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

# TODO: add 'list' command to GruShell (make a Gru.list_orchestrations method)
# that lists all live orchestrations Gru is running

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
        self._shutdown_done = self._loop.create_future()
        self._start_ops: dict[str, cf.Future] = {}    # id_or_pending -> future
        self._stop_ops: dict[str, cf.Future] = {}     # id -> future
        self._last_targets: list[str] = []

    # -------- helpers --------

    def _to_argv(self, line: str) -> list[str]:
        return shlex.split(line)

    def _get_minion_ids_and_names(self) -> list[str]:
        return [i for i in self._gru._minions_by_id] \
        + [n for n in self._gru._minions_by_name]

    def _submit(self, coro) -> cf.Future:
        if self._loop.is_running():
            return asyncio.run_coroutine_threadsafe(coro, self._loop)
        return self._loop.create_task(coro)

    def _compute_state(self, key: str) -> State:
        if key.startswith("pending:"):
            f = self._start_ops.get(key)
            return "failed" if (f and f.done() and f.exception()) \
            else ("starting" if f and not f.done() else "unknown")
        if key in self._stop_ops:
            f = self._stop_ops[key]
            if not f.done(): return "stopping"
            try:
                f.result(); return "stopped"
            except asyncio.CancelledError: return "aborted"
            except Exception: return "failed"
        if key in self._gru._minions_by_id:  # running if present
            return "running"
        return "unknown"

    def _print_summary(self):
        counts: dict[State, int] = {}
        keys = set(self._start_ops) | set(self._stop_ops) | set(self._gru._minions_by_id)
        for k in keys:
            s = self._compute_state(k); counts[s] = counts.get(s, 0) + 1
        print(" ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "(none)")

    # -------- start --------

    def do_start(self, line: str):
        argv = self._to_argv(line)
        if len(argv) != 3:
            print("Usage: start MINION_MODULEPATH MINION_CONFIG_MODULEPATH PIPELINE_MODULEPATH")
            return

        fut = self._submit(self._gru.start_minion(*argv))  # must return instance_id
        pending_id = f"pending:{id(fut)}"
        self._start_ops[pending_id] = fut
        self._last_targets = [pending_id]
        print("start queued")

        def _cb(f: cf.Future):
            try:
                inst_id = f.result()
            except Exception:
                self._start_ops[pending_id] = f  # keep for status to show 'failed'
                return
            self._start_ops.pop(pending_id, None)
            self._start_ops[inst_id] = f
            self._last_targets = [inst_id]

        fut.add_done_callback(_cb)

    def complete_start(self, text: str, line: str, begidx: int, endidx: int):
        return [f for f in os.listdir(".") if f.endswith(".py") and f.startswith(text)]

    # -------- stop --------

    def do_stop(self, line: str):
        ids = self._to_argv(line)
        if not ids:
            print("Usage: stop NAME_OR_INSTANCE_ID ...")
            return
        for mid in ids:
            fut = self._submit(self._gru.stop_minion(mid))
            self._stop_ops[mid] = fut
        self._last_targets = ids
        print(f"stop queued for {len(ids)}")

    def complete_stop(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names()

    # -------- status --------

    def do_status(self, line: str):
        argv = self._to_argv(line)
        await_mode = "--await" in argv
        timeout: Optional[float] = None
        if "--timeout" in argv:
            i = argv.index("--timeout")
            timeout = float(argv[i+1]) if i + 1 < len(argv) else None
        targets = [a for a in argv if not a.startswith("--")] or self._last_targets
        if not targets:
            self._print_summary(); return

        if not await_mode:
            for t in targets:
                print(f"{t} {self._compute_state(t)}")
            return

        def _wait_on(t: str):
            st = self._compute_state(t)
            if t.startswith("pending:"):
                f = self._start_ops.get(t);  return f
            if st in ("stopping",):
                return self._stop_ops.get(t)
            if st in ("starting",):
                # started but rekeyed: find its real id if available, else pending future
                f = self._start_ops.get(t); return f
            return None  # running/unknown: nothing to wait on

        futs = [f for t in targets if (f := _wait_on(t)) is not None]
        if futs:
            try:
                if timeout is None:
                    for f in futs: f.result()
                else:
                    for f in futs: f.result(timeout=timeout)
            except Exception as e:
                print(f"status/await error: {e}")

        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_status(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names() \
        + [k for k in self._start_ops if k.startswith("pending:")]

    # -------- wait --------

    def do_wait(self, line: str):
        argv = self._to_argv(line)
        timeout = parse_timeout(argv)  # whatever you're already doing
        targets = parse_targets_or_last(argv, self._last_targets)
        if not targets:
            print("No targets to wait on"); return

        futs = [f for t in targets if (f := _wait_on_future_if_any(t)) is not None]

        try:
            if futs:
                cf.wait(futs, timeout=timeout)
            # optionally: poll for IDs without futures until they leave transitional states
        except KeyboardInterrupt:
            print("wait interrupted by user")

        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_wait(self, text: str, line: str, begidx: int, endidx: int):
        return [k for k in self._start_ops if k.startswith("pending:")]

    # -------- metrics --------

    def do_metrics(self, line: str):
        coro = self._gru._metrics._snapshot()
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
        # inflights = sum(s["value"] for s in snap["gauges"].get("MINION_WORKFLOW_INFLIGHT_GAUGE", []))
        # succeeded_workflows = sum(s["value"] for s in snap["counters"].get("MINION_WORKFLOW_SUCCEEDED_TOTAL", []))

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
        os.system('cls' if os.name == 'nt' else 'clear')

    # -------- do i still need the following? maybe... --------

    # async def run_until_complete(self) -> bool:
    #     self.cmdloop()
    #     return await self._shutdown_done