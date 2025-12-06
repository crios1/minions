import asyncio
import cmd
import os
import readline
import shlex

from pprint import pprint

from .gru import Gru

readline.parse_and_bind("tab: complete")

# TODO: write thorough tests for Gru and realisitc but not too heavy of tests for GruShell.

# TODO: consider implementing the below or no just give a warning on the first ctrl-c and do on the second
# """
# You can “disable Ctrl-C,” but the better, safer UX is:
# First Ctrl-C = graceful shutdown, Second Ctrl-C = hard abort.
# Don't swallow it entirely—just wire it to your shutdown path.
# """

class GruShell(cmd.Cmd):
    """No exit method cuz you got to run GruShell in something like tmux."""
    intro = "Welcome to GruShell. Type 'help' or '?' to list commands."
    prompt = "gru> "

    def __init__(self, gru: Gru):
        super().__init__()
        self._gru = gru
        self._loop = gru._loop
        self._shutdown_done = self._loop.create_future()

    def _to_argv(self, line: str) -> list[str]:
        return shlex.split(line)

    def _get_minion_ids_and_names(self) -> list[str]:
        return [id for id in self._gru._minions_by_id] \
        + [name for name in self._gru._minions_by_name]


    def do_start(self, line: str):
        """Starts a minion."""
        argv = self._to_argv(line)
        if not argv or len(argv) != 3:
            print("Usage: start MINION_MODULEPATH MINION_CONFIG_MODULEPATH PIPELINE_MODULEPATH")
            return
        # self._loop.create_task(self._gru.start_minion(*argv))
        # print(f"Minion launched. Check `status {argv[0]}` to confirm launch.")
        # print(f"{'minion'} starting. Check `status {'name'}` to confirm launch.")
        print('<start minion>')
        ...

    def complete_start(self, text: str, line: str, begidx: int, endidx: int):
        return [f for f in os.listdir(".") if f.endswith(".py") and f.startswith(text)]


    def do_stop(self, line: str):
        """Gracefully stop a minion."""
        argv = self._to_argv(line)
        if not argv:
            print("Usage: stop NAME_OR_INSTANCE_ID")
            return
        for name_or_instance_id in argv:
            self._gru.stop_minion(name_or_instance_id)
        # TODO: determine shell design (sync or async await)
        # is it better for the user to execute a command and return quickly
        # or for them to wait until the the minion completely stops
        # before the command ends.

    def complete_stop(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names()


    def do_status(self, line: str):
        ...

    def complete_status(self, text: str, line: str, begidx: int, endidx: int):
        return self._get_minion_ids_and_names()


    def do_shutdown(self, line: str):
        "Shutdown GruShell and minions."
        if self._shutdown_done.done():
            print("Shutdown already in progress...")
            return

        print("Shutting down gru and minions...")

        async def _shutdown():
            await self._gru.shutdown()
            if not self._shutdown_done.done():
                self._shutdown_done.set_result(True)

        self._loop.create_task(_shutdown())
        return True

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

    def do_clear(self, line: str):
        "Clear the shell screen."
        os.system('cls' if os.name == 'nt' else 'clear')

    async def run_until_complete(self) -> bool:
        self.cmdloop()
        return await self._shutdown_done