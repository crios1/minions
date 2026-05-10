from __future__ import annotations

import asyncio
import concurrent.futures as cf
import io
from contextlib import redirect_stdout

from minions._internal._domain.gru_result_types import StartMinionResult
from minions._internal._domain.gru_shell import GruShell


class FakeGru:
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._minions_by_id = {}
        self._minions_by_name = {}
        self.start_calls = []

    def start_minion(self, minion, pipeline, *, minion_config_path=None):
        self.start_calls.append((minion, pipeline, minion_config_path))

        async def _start():
            return StartMinionResult(success=True, name="ExampleMinion", instance_id="minion-1")

        return _start()

    def close(self) -> None:
        self._loop.close()


def test_wait_uses_last_target_without_undefined_helper_crash() -> None:
    gru = FakeGru()
    shell = GruShell(gru)  # type: ignore[arg-type]
    fut: cf.Future[StartMinionResult] = cf.Future()
    fut.set_result(StartMinionResult(success=True, name="ExampleMinion", instance_id="minion-1"))
    shell._start_ops["pending:1"] = fut
    shell._last_targets = ["pending:1"]

    out = io.StringIO()
    with redirect_stdout(out):
        shell.do_wait("")

    assert "pending:1 failed" not in out.getvalue()
    gru.close()


def test_start_calls_current_gru_signature_and_rekeys_successful_result() -> None:
    gru = FakeGru()
    shell = GruShell(gru)  # type: ignore[arg-type]
    submitted: list[object] = []

    def submit(coro):
        submitted.append(coro)
        fut: cf.Future[StartMinionResult] = cf.Future()
        fut.set_result(StartMinionResult(success=True, name="ExampleMinion", instance_id="minion-1"))
        coro.close()
        return fut

    shell._submit = submit  # type: ignore[method-assign]

    out = io.StringIO()
    with redirect_stdout(out):
        shell.do_start("tests.assets.minions.two_steps.simple.basic config.toml tests.assets.pipelines.simple.dict_event")

    assert submitted
    assert gru.start_calls == [
        (
            "tests.assets.minions.two_steps.simple.basic",
            "tests.assets.pipelines.simple.dict_event",
            "config.toml",
        )
    ]
    assert shell._last_targets == ["minion-1"]
    assert "minion-1" in shell._start_ops
    gru.close()
