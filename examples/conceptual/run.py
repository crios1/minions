import asyncio

from minions import Gru, GruShell

async def main():
    gru = await Gru.create()
    shell = GruShell(gru)
    await gru.start_minion('', '', '')
    await shell.run_until_complete()

asyncio.run(main())
