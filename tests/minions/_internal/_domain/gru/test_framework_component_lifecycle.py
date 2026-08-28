import asyncio

import pytest

from minions import Gru
from minions._internal._domain.exceptions import MinionsError
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore


class TestStartup:
    @pytest.mark.asyncio
    async def test_create_starts_injected_logger_before_state_store_and_metrics(
        self,
    ):
        calls: list[str] = []

        class MyLogger(NoOpLogger):
            async def startup(self) -> None:
                calls.append("logger startup")

        class MyStateStore(NoOpStateStore):
            async def startup(self) -> None:
                calls.append("state store startup")

        class MyMetrics(NoOpMetrics):
            async def startup(self) -> None:
                calls.append("metrics startup")

        gru = await Gru.create(
            logger=MyLogger(),
            state_store=MyStateStore(),
            metrics=MyMetrics(),
        )
        try:
            assert calls[0] == "logger startup"
            assert set(calls[1:]) == {
                "state store startup",
                "metrics startup",
            }
        finally:
            await gru.shutdown()

    @pytest.mark.asyncio
    async def test_create_settles_in_progress_component_startup_before_framework_component_shutdown_when_component_startup_fails(  # noqa: E501
        self,
    ):
        calls: list[str] = []

        class MyLogger(NoOpLogger):
            async def startup(self) -> None:
                calls.append("logger startup")

            async def shutdown(self) -> None:
                calls.append("logger shutdown")

        class MyStateStore(NoOpStateStore):
            async def startup(self) -> None:
                calls.append("state store startup")
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    calls.append("state store startup cancelled")
                    raise

            async def shutdown(self) -> None:
                calls.append("state store shutdown")

        class MyMetrics(NoOpMetrics):
            async def startup(self) -> None:
                calls.append("metrics startup")
                raise RuntimeError("metrics startup failed")

            async def shutdown(self) -> None:
                calls.append("metrics shutdown")

        with pytest.raises(MinionsError, match="MyMetrics.startup failed"):
            await Gru.create(
                logger=MyLogger(),
                state_store=MyStateStore(),
                metrics=MyMetrics(),
            )

        assert calls[0] == "logger startup"
        assert set(calls[1:3]) == {
            "state store startup",
            "metrics startup",
        }
        assert calls[3] == "state store startup cancelled"
        assert set(calls[4:-1]) == {
            "state store shutdown",
            "metrics shutdown",
        }
        assert calls[-1] == "logger shutdown"

    @pytest.mark.asyncio
    async def test_create_preserves_startup_failure_when_framework_component_shutdown_also_fails(  # noqa: E501
        self,
    ):
        class MyStateStore(NoOpStateStore):
            async def shutdown(self) -> None:
                raise RuntimeError("state store shutdown failed")

        class MyMetrics(NoOpMetrics):
            async def startup(self) -> None:
                raise RuntimeError("metrics startup failed")

        with pytest.raises(MinionsError, match="MyMetrics.startup failed") as exc_info:
            await Gru.create(
                logger=NoOpLogger(),
                state_store=MyStateStore(),
                metrics=MyMetrics(),
            )

        assert any(
            "state store shutdown failed" in note
            for note in getattr(exc_info.value, "__notes__", ())
        )

    @pytest.mark.asyncio
    async def test_create_does_not_start_resource_monitor_when_framework_component_startup_fails(  # noqa: E501
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        monitor_started = asyncio.Event()

        async def record_monitor_start(_gru: Gru) -> None:
            monitor_started.set()

        class MyMetrics(NoOpMetrics):
            async def startup(self) -> None:
                raise RuntimeError("metrics startup failed")

        monkeypatch.setattr(Gru, "_monitor_process_resources", record_monitor_start)

        with pytest.raises(MinionsError, match="MyMetrics.startup failed"):
            await Gru.create(
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=MyMetrics(),
            )
        await asyncio.sleep(0)

        assert not monitor_started.is_set()

    @pytest.mark.asyncio
    async def test_create_continues_framework_component_shutdown_when_a_shutdown_fails(
        self,
    ):
        calls: list[str] = []

        class MyLogger(NoOpLogger):
            async def startup(self) -> None:
                calls.append("logger startup")

            async def shutdown(self) -> None:
                calls.append("logger shutdown")

        class MyStateStore(NoOpStateStore):
            async def shutdown(self) -> None:
                calls.append("state store shutdown")
                raise RuntimeError("state store shutdown failed")

        class MyMetrics(NoOpMetrics):
            async def startup(self) -> None:
                raise RuntimeError("metrics startup failed")

            async def shutdown(self) -> None:
                calls.append("metrics shutdown")

        with pytest.raises(MinionsError, match="MyMetrics.startup failed"):
            await Gru.create(
                logger=MyLogger(),
                state_store=MyStateStore(),
                metrics=MyMetrics(),
            )

        assert calls == [
            "logger startup",
            "metrics shutdown",
            "state store shutdown",
            "logger shutdown",
        ]

    @pytest.mark.asyncio
    async def test_cancelling_create_shuts_down_attempted_framework_components_and_releases_singleton(  # noqa: E501
        self,
    ):
        calls: list[str] = []
        state_store_startup_blocked = asyncio.Event()

        class MyLogger(NoOpLogger):
            async def startup(self) -> None:
                calls.append("logger startup")

            async def shutdown(self) -> None:
                calls.append("logger shutdown")

        class MyStateStore(NoOpStateStore):
            async def startup(self) -> None:
                calls.append("state store startup")
                state_store_startup_blocked.set()
                await asyncio.Event().wait()

            async def shutdown(self) -> None:
                calls.append("state store shutdown")

        create_task = asyncio.create_task(
            Gru.create(
                logger=MyLogger(),
                state_store=MyStateStore(),
                metrics=NoOpMetrics(),
            )
        )
        await state_store_startup_blocked.wait()

        create_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await create_task

        assert calls == [
            "logger startup",
            "state store startup",
            "state store shutdown",
            "logger shutdown",
        ]

        replacement = await Gru.create(
            logger=NoOpLogger(),
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
        )
        await replacement.shutdown()
