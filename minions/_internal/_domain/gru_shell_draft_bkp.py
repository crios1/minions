# TODO:
# The following is a draft implementation of the given design ...
# I need to review and finalize it so i can integrate it into gru_shell.py

# usage: demo_argparse [-h] [--verbose] [--mode {fast,safe,dry-run}] path {run,inspect} ...

# CORE DESIGN:
# start <m_modpath> <m_configpath> <p_modpath>
#   → enqueue orchestration, print IDs, return immediately.
# stop <mid_or_mname>
#   → enqueue orchestration, print IDs, return immediately.
# status <mid_or_mname>
#   → non-blocking snapshot of current state
#   → without an arg 'status' can just print a summary of 
#     all the minion statuses
# wait <id_name_vector> [--timeout N]
#   → keeps the command pending until each ID leaves its transitional state ("starting"/"stopping").
#   → timeout only ends the wait, never affects the underlying job.
#   → Ctrl+C only ends the wait, never affects the underlying job.
#   → is used to wait for 'start's and 'stop's
#     but would also be used for 'redeploy's
#     since those can happen concurrently too
#     todo: i just need to make sure that users don't
#     lose thier 'redeploy' ids
#     because they could if they clear it away
#     maybe there should be some way for me to surface it to them
#     event if it's complete, idk, we'll see
# Rationale:
#   The shell remains responsive for submitting orchestrations,
#   while still giving users an explicit way to wait on their
#   submitted orchestration to resolve.

# DEVOPS DESIGN:
# snapshot
#   → prints canonical sorted json to stdout
#   → contains code_hash / config_hash
# fingerprint
#   → basically a wrapper on 'snapshot' that returns its hash
# deps <id_or_name>
#   → <id_or_name> of minion, pipeline, or resource
#   → i guess users can only get ids from running 'deps' on minions first?
#     or no they probably use the snapshot first and then decide
#     to do scoped views of deps with the command right?
#   → usage:
#     gru> deps minion 1234-abcd
#     minion 1234-abcd:
#       pipeline: pipe-foo
#       resources:
#         - redis-main
#         - feature-flags
#       transitive-resources:
#         - redis-main
#         - feature-flags
#         - db-primary
#     gru> deps pipeline 1234-abcd
#     pipeline 1234-abcd:
#       resources:
#         - redis-main
#         - feature-flags
#       transitive-resources:
#         - redis-main
#         - feature-flags
#         - db-primary
#     gru> deps resource 1234-abcd
#     resource 1234-abcd:
#       resources:
#         - redis-main
#         - feature-flags
#       transitive-resources:
#         - redis-main
#         - feature-flags
#         - db-primary
# redeploy <path_or_modpath>
#   → redeploys of minions and pipelines is fine to do concurrently
#     minions have no dependents and pipelines only have soft dependents
#     if a pipeline is removed minions dependents just don't spawn new workflows
#   → redeploys of resources need to be serialized because
#     or at least checks need to happen to avoid conflicts
#   → if you redeploy a minion,
#     just restart each of that minion's instances by each config file
#     (minions have no dependents)
#   → if you redeploy a pipeline,
#     just restart the pipeline
#     (pipelines only have soft dependents [minions])
#   → if you redeploy a resource,
#     you need to look at dependencies and 
#     know when you can do them concurrently vs serially
#     (resources have hard dependencies)
# note: i will never add true statefulness to pipeline/resource
#       only adhoc supplemental statefulness
#       like an in memory cache but maybe i want to persist the cache
#       to disk w/ sqlite so that on restarts the cache
#       can be immediately usefule again assuming data isn't stale
#       that state will never touch statestore
#       statestore is only for workflow states
# Rationale:
# 'snapshot' and 'fingerprint' creates comparable orchestration state between SDLC enviornments (dev,qa,uat,prod)
# users can export with 'snapshot' and do diffs or just use 'fingerprint' and compare hashes

# TODO: implement 'print outs' for the following commands
# gru> start minion.py cfg.py pipe.py
# start queued: pending:12345

# gru> status
# pending:12345 starting

# gru> status --await --timeout 30
# pending:12345 running   # or failed, etc.

# gru> stop minion-1
# stop queued for 1

# gru> status --await minion-1 --timeout 10
# minion-1 stopped

# does it make sense to have 'list' and 'snapshot' commands?
# list would be more user friendly but not as robust as snapshot.
# 'list' would give you a list of what 'start' commands you ran that are still present
# but why would i let the user do that if they can get that info and more from 'snapshot'?
# but idk, i sense that it would be best for the user to only have one robust way of reading state system state
# like the user either sees the following json from 'snapshot' or the following text block from 'list'
# idk what would be better for them or if it makes sense to just offer one
# {
#   "minions": [
#     {
#       "name": "PriceWatcher",
#       "module": "app.minions.price_watcher",
#       "class": "PriceWatcher",
#       "config_module": "app.configs.price_watcher",
#       "pipeline": "price_pipeline",
#       "config_hash": "a3f1c0...",
#       "code_hash": "7be92d..."
#     },
#     ...
#   ]
# }
# start <m_modpath> <m_configpath> <p_modpath>
# start <m_modpath> <m_configpath> <p_modpath>
# ...


# TODO: add 'deps' command to GruShell (make a Gru.get_dependencies method)
# TODO: add 'reload' command to GruShell (make a Gru.reload method)

# TODO: document how you manage deployments
# so i think the best practice for managing code changes is just replicating prod in lower enviornments and make sure you get it right in the lower enviornments before you go to prod. in other words, the "sending a rocket to the moon" dev to prod process, you only have one shot so get it right.

# TODO: Update my docs:
# The best practice for managing code changes is just replicating prod in lower enviornments
# and making sure you get it right there before going to prod.
# Almost like a "sending a rocket to the moon" dev process 
# where you only have one shot to get it right but you do simulations beforehand.

# So my conclusion is reasonable:
# - prod-like lower envs.
# - prove changes there.
# - treat prod deploys as “rocket launch”: one clear shot, with a rollback playbook if needed.

# The whole deployment/rollback process could be like:
# - ensure enviornment parity between prod and lower enviornments
# - validate changes in lower enviornments
# - in prod:
#    - freeze and clone inflight workflows / accumulate pipeline events (if not changing pipelines)
#    - make validated changes (update files / use redeploy command)
#    - unfreeze cloned inflight workflows
#    - check logs / go thru your validation plan
#    - if all good, release accumulated pipeline events
#    - if not good, (maybe cleanup/delete the bad workflows and logs)
#      rollback changes, unfreeze inflight workflows and release accumulated pipeline events

# TODO: put the following documentation / mental model in the proper place(s)
# TopLevelModuleAPI: Minion, Pipeline, Resource, Gru, run_gru_shell
# because wiring Gru and GruShell is non trivial so helper does it for us
# manual usage of 'runtime domain objects' (Gru, GruShell)
# is for advanced use cases (like embedding Gru into a larger asyncio app)
# and thus generally not need by most users
# DefaultDeploymentScriptDraft:
# from minions import Minion, Pipeline, Resource, run_gru_shell, Gru
# import asyncio

# MINION_SPECS = [
#     ("mod_a", "cfg_a", "pipe_a"),
#     ("mod_b", "cfg_b", "pipe_b"),
#     ("mod_c", "cfg_c", "pipe_c"),
# ]

# async def init(gru: Gru):
#     coros = [gru.start_minion(*spec) for spec in MINION_SPECS]
#     results = await asyncio.gather(*coros, return_exceptions=True)
#     failures = [r for r in results if isinstance(r, Exception)]
#     if failures:
#         raise RuntimeError(f"{len(failures)} minions failed to start")

# if __name__ == "__main__":
#     run_gru_shell(init=init)

# and run_gru_shell is implemented launch_gru_and_shell or something
# so powerusers can do whatever they want i guess?
# gru, shell, loop, thread = launch_gru_and_shell(init=my_init)
# shell.cmdloop()
# custom shutdown, or embed shell into existing app

# import asyncio
# import threading
# import concurrent.futures as cf
# from collections.abc import Awaitable, Callable

# from .gru import Gru, _UNSET, StateStore, Logger, Metrics

# InitHook = Callable[[Gru], Awaitable[None]]

# def launch_gru_and_shell(
#     *,
#     init: InitHook | None = None,
#     state_store: StateStore | None | object = _UNSET,
#     logger: Logger | None | object = _UNSET,
#     metrics: Metrics | None | object = _UNSET,
#     metrics_port: int = 8081,
# ) -> tuple[Gru, "GruShell", asyncio.AbstractEventLoop, threading.Thread]:
#     loop = asyncio.new_event_loop()
#     ready: cf.Future[Gru] = cf.Future()

#     def runner():
#         asyncio.set_event_loop(loop)

#         async def bootstrap():
#             gru = await Gru.create(
#                 state_store=state_store,
#                 logger=logger,
#                 metrics=metrics,
#                 metrics_port=metrics_port,
#             )
#             if init:
#                 await init(gru)
#             if not ready.done(): ready.set_result(gru)

#         loop.create_task(bootstrap())
#         loop.run_forever()

#     thread = threading.Thread(target=runner, daemon=True)
#     thread.start()

#     gru = ready.result()
#     shell = GruShell(gru)
#     return gru, shell, loop, thread

# def run_gru_shell(
#     *,
#     init: InitHook | None = None,
#     state_store: StateStore | None | object = _UNSET,
#     logger: Logger | None | object = _UNSET,
#     metrics: Metrics | None | object = _UNSET,
#     metrics_port: int = 8081,
# ) -> None:
#     gru, shell, loop, thread = launch_gru_and_shell(
#         init=init,
#         state_store=state_store,
#         logger=logger,
#         metrics=metrics,
#         metrics_port=metrics_port,
#     )

#     try:
#         shell.cmdloop()
#     finally:
#         try:
#             asyncio.run_coroutine_threadsafe(gru.shutdown(), loop).result()
#         except Exception:
#             pass
#         loop.call_soon_threadsafe(loop.stop)
#         thread.join()

import asyncio
import cmd
import os
import shlex
import time
import concurrent.futures as cf

from pprint import pprint
from typing import Literal, Optional

from .gru import Gru

State = Literal[
    "starting","running","stopping",
    "stopped","failed","aborted","unknown"
]

class GruShell(cmd.Cmd):
    """Interactive shell bound to a running Gru instance.

    This is a runtime integration object. Prefer `minions.run_shell()`
    unless you need custom wiring.
    """
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
        if not self._loop.is_running():
            raise RuntimeError("Gru loop must be running before submitting coroutines")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)
        # if self._loop.is_running():
        #     return asyncio.run_coroutine_threadsafe(coro, self._loop)
        # return self._loop.create_task(coro)

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
        if key in self._gru._minions_by_id:
            return "running"
        return "unknown"

    def _print_summary(self):
        counts: dict[State, int] = {}
        keys = set(self._start_ops) | set(self._stop_ops) | set(self._gru._minions_by_id)
        for k in keys:
            s = self._compute_state(k); counts[s] = counts.get(s, 0) + 1
        print(" ".join(f"{k}={v}" for k, v in sorted(counts.items())) or "(none)")

    def _parse_timeout_and_targets(self, argv: list[str]) -> tuple[Optional[float], list[str]]:
        timeout = None
        if "--timeout" in argv:
            i = argv.index("--timeout")
            if i + 1 < len(argv):
                timeout = float(argv[i+1])
            argv = [a for j, a in enumerate(argv) if j not in (i, i + 1)]
        targets = [a for a in argv if not a.startswith("--")]
        return timeout, targets

    def _is_transitional(self, state: State) -> bool:
        return state in ("starting", "stopping")

    # -------- start --------

    def do_start(self, line: str):
        argv = self._to_argv(line)
        if len(argv) != 3:
            print("Usage: start MINION_MODULEPATH MINION_CONFIG_MODULEPATH PIPELINE_MODULEPATH")
            return

        fut = self._submit(self._gru.start_minion(*argv))
        pending_id = f"pending:{id(fut)}"
        self._start_ops[pending_id] = fut
        self._last_targets = [pending_id]
        print("start queued")

        def _cb(f: cf.Future):
            try:
                inst_id = f.result()
            except Exception:
                self._start_ops[pending_id] = f
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

    # -------- status (snapshot only) --------

    def do_status(self, line: str):
        "used to check the state of pending orchestrations"
        argv = self._to_argv(line)
        targets = [a for a in argv if not a.startswith("--")] or self._last_targets
        if not targets:
            self._print_summary(); return
        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_status(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names() \
        + [k for k in self._start_ops if k.startswith("pending:")]

    # -------- wait (blocking command, loop keeps running) --------

    def do_wait(self, line: str):
        argv = self._to_argv(line)
        timeout, targets = self._parse_timeout_and_targets(argv)
        targets = targets or self._last_targets
        if not targets:
            print("No targets to wait on"); return

        pending = set(targets)
        deadline = time.monotonic() + timeout if timeout is not None else None

        try:
            while pending:
                done = []
                for t in pending:
                    st = self._compute_state(t)
                    if not self._is_transitional(st):
                        done.append(t)
                for t in done:
                    pending.discard(t)
                if not pending:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("wait interrupted by user")

        for t in targets:
            print(f"{t} {self._compute_state(t)}")

    def complete_wait(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names() \
        + [k for k in self._start_ops if k.startswith("pending:")] \
        + [k for k in self._stop_ops]

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

    # async def run_until_complete(self) -> bool:
    #     self.cmdloop()
    #     return await self._shutdown_done
