import asyncio
from minions import Gru, GruShell

MINION_SPECS = [
    ("mod_a", "cfg_a", "pipe_a"),
    ("mod_b", "cfg_b", "pipe_b"),
    ("mod_c", "cfg_c", "pipe_c"),
]

async def _start_initial_minions(gru: Gru):
    coros = [gru.start_minion(*spec) for spec in MINION_SPECS]
    results = await asyncio.gather(*coros, return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        # up to you: log and keep going, or bail hard
        raise RuntimeError(f"{len(failures)} minions failed to start")

async def main():
    gru = await Gru.create()
    await _start_initial_minions(gru)

    shell = GruShell(gru)
    try:
        await shell.run()  # prompt_toolkit-based async shell loop
    finally:
        await gru.shutdown()  # make sure we wind down when shell exits

if __name__ == "__main__":
    asyncio.run(main())
