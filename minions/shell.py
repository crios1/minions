from ._internal._domain.gru_shell import GruShell

__all__ = ["GruShell"]

if __name__ == '__main__':
    import asyncio

    from ._internal._domain.gru import Gru
    from ._internal._framework.logger_noop import NoOpLogger
    from ._internal._framework.metrics_noop import NoOpMetrics
    from ._internal._framework.state_store_noop import NoOpStateStore

    async def main():
        # TODO: the shell will be used for demo-ing / playing around ... maybe have --args that let the user decide what statestore, logger, and metrics they want
        gru = await Gru.create(NoOpStateStore(), NoOpLogger(), NoOpMetrics())
        shell = GruShell(gru)
        await shell.run_until_complete()

    asyncio.run(main())