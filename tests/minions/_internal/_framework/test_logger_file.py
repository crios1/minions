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
async def test_writes_valid_jsonl(tmp_path: Path):
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
async def test_stdout_formats_structured_log_fields_as_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
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


@pytest.mark.asyncio
async def test_rotation_uses_unique_paths_when_timestamp_repeats(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    logger = FileLogger(
        stdout=False,
        log_dir=str(tmp_path),
        log_filename_prefix="collision-log",
        max_log_file_bytes=1024,
        max_log_storage_bytes=None,
    )
    monkeypatch.setattr(
        logger,
        "_mn_iso_8601_ts_fs_safe",
        lambda: "2026-09-20T12-00-00Z",
    )

    for index in range(5):
        await logger.log(INFO, f"record-{index}", payload="x" * 800)

    active_file = tmp_path / "collision-log.log"
    rotated_files = sorted(tmp_path.glob("collision-log_*.log"))

    active_line_count = len(active_file.read_text(encoding="utf-8").splitlines())
    rotated_line_count = sum(
        len(path.read_text(encoding="utf-8").splitlines())
        for path in rotated_files
    )

    assert len(rotated_files) == 4
    assert rotated_line_count + active_line_count == 5


@pytest.mark.asyncio
async def test_storage_limit_includes_active_log_file(tmp_path: Path):
    logger = FileLogger(
        stdout=False,
        log_dir=str(tmp_path),
        log_filename_prefix="bounded-log",
        max_log_file_bytes=1024,
        max_log_storage_bytes=2048,
    )

    # write enough records to exercise the storage limit.
    for index in range(6):
        await logger.log(INFO, f"record-{index}", payload="x" * 800)

    managed_files = [
        tmp_path / "bounded-log.log",
        *tmp_path.glob("bounded-log_*.log"),
    ]

    assert sum(path.stat().st_size for path in managed_files) <= 2048
