from __future__ import annotations

import asyncio
import concurrent.futures as cf
import io
import pytest
from contextlib import redirect_stdout
from collections.abc import Coroutine
from typing import Any

from minions._internal._domain.gru_result_types import StartResult
from minions._internal._domain.gru_shell import GruShell


class FakeGru:
    def __init__(self, start_result: StartResult | None = None) -> None:
        self._loop = asyncio.new_event_loop()
        self._minions_by_instance_id = {}
        self._minions_by_name = {}
        self.start_calls: list[tuple[str, str, str | None]] = []
        self._start_result = start_result or StartResult(
            success=True,
            name="ExampleMinion",
            orchestration_id="minion-1",
        )

    def start_orchestration(
        self, pipeline: str, minion: str, *, minion_config_path: str | None = None
    ) -> Coroutine[Any, Any, StartResult]:
        self.start_calls.append((pipeline, minion, minion_config_path))

        async def _start() -> StartResult:
            return self._start_result

        return _start()

    def close(self) -> None:
        self._loop.close()


@pytest.mark.skip("GruShell deprecated")
def test_start_prints_failed_start_result_with_recovery_guidance() -> None:
    gru = FakeGru(
        StartResult(
            success=False,
            reason=(
                "1 persisted workflow context could not be decoded with the current "
                "Minion event and workflow context types."
            ),
            suggestion=(
                "Run the previous compatible code and drain the orchestration before "
                "starting it with the new types."
            ),
        )
    )
    shell = GruShell(gru)  # type: ignore[arg-type]

    def submit(
        coro: Coroutine[Any, Any, StartResult]
    ) -> cf.Future[StartResult]:
        fut: cf.Future[StartResult] = cf.Future()
        fut.set_result(gru._start_result)
        coro.close()
        return fut

    shell._submit = submit  # type: ignore[method-assign]

    out = io.StringIO()
    with redirect_stdout(out):
        shell.do_start("tests.assets.minions.two_steps.simple.basic config.toml tests.assets.pipelines.simple.record_event")

    output = out.getvalue()
    assert "start queued" in output
    assert "Cannot start orchestration." in output
    assert "1 persisted workflow context could not be decoded" in output
    assert "Recommended:" in output
    assert "Run the previous compatible code and drain the orchestration" in output
    assert "No workflows were started." in output
    gru.close()


@pytest.mark.skip("GruShell deprecated")
def test_wait_uses_last_target_without_undefined_helper_crash() -> None:
    gru = FakeGru()
    shell = GruShell(gru)  # type: ignore[arg-type]

    fut: cf.Future[StartResult] = cf.Future()
    fut.set_result(StartResult(success=True, name="ExampleMinion", orchestration_id="minion-1"))

    start_ops = getattr(shell, "_start_ops")
    assert isinstance(start_ops, dict)

    start_ops["pending:1"] = fut
    shell._last_targets = ["pending:1"]

    out = io.StringIO()
    with redirect_stdout(out):
        shell.do_wait("")

    assert "pending:1 failed" not in out.getvalue()
    gru.close()


@pytest.mark.skip("GruShell deprecated")
def test_start_calls_current_gru_signature_and_rekeys_successful_result() -> None:
    gru = FakeGru()
    shell = GruShell(gru)  # type: ignore[arg-type]
    submitted: list[Coroutine[Any, Any, StartResult]] = []

    def submit(
        coro: Coroutine[Any, Any, StartResult]
    ) -> cf.Future[StartResult]:
        submitted.append(coro)
        fut: cf.Future[StartResult] = cf.Future()
        fut.set_result(StartResult(success=True, name="ExampleMinion", orchestration_id="minion-1"))
        coro.close()
        return fut

    shell._submit = submit  # type: ignore[method-assign]

    out = io.StringIO()
    with redirect_stdout(out):
        shell.do_start("tests.assets.minions.two_steps.simple.basic config.toml tests.assets.pipelines.simple.record_event")

    assert submitted
    assert gru.start_calls == [
        (
            "tests.assets.pipelines.simple.record_event",
            "tests.assets.minions.two_steps.simple.basic",
            "config.toml",
        )
    ]
    assert shell._last_targets == ["minion-1"]
    start_ops = getattr(shell, "_start_ops")
    assert isinstance(start_ops, dict)
    assert "minion-1" in start_ops
    gru.close()
