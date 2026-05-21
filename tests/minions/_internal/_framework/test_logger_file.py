from __future__ import annotations

import json
from pathlib import Path

import msgspec
import pytest

from minions._internal._framework.logger import INFO
from minions._internal._framework.logger_file import FileLogger


class Detail(msgspec.Struct):
    count: int
    tags: list[str]


@pytest.mark.asyncio
async def test_file_logger_writes_valid_jsonl(tmp_path: Path) -> None:
    logger = FileLogger(stdout=False, log_dir=str(tmp_path), log_filename_prefix="test-log")

    await logger.log(
        INFO,
        "workflow checkpoint saved",
        workflow_id="wf-1",
        detail=Detail(count=2, tags=["a", "b"]),
    )

    log_path = tmp_path / "test-log.log"
    lines = log_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["level"] == "INFO"
    assert payload["msg"] == "workflow checkpoint saved"
    assert payload["workflow_id"] == "wf-1"
    assert payload["detail"] == {"count": 2, "tags": ["a", "b"]}


@pytest.mark.asyncio
async def test_file_logger_stdout_extras_use_json_text(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    logger = FileLogger(stdout=True, log_dir=str(tmp_path), log_filename_prefix="stdout-log")

    await logger.log(
        INFO,
        "workflow checkpoint saved",
        workflow_id="wf-1",
        detail=Detail(count=3, tags=["x"]),
    )

    out = capsys.readouterr()

    assert "[✓] workflow checkpoint saved" in out.out
    assert 'workflow_id=wf-1' in out.out
    assert 'detail={"count":3,"tags":["x"]}' in out.out
    assert out.err == ""
