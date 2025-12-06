import asyncio
import sys

from pathlib import Path
from typing import Any

from .logger import Logger, INFO, LEVEL_NAMES
from .._utils.serialization import serialize, deserialize

MIN_LOG_FILE_BYTES = 1024    # 1 KB
MIN_LOG_STORAGE_BYTES = 1024 # 1 KB

class FileLogger(Logger):
    """
    An async-aware structured logger that writes JSONL logs to disk with rotation and optional retention.

    Design Goals:
    - **Structured Logging**: Logs are written as newline-delimited JSON (JSONL), making them easy to parse
      with tools like `jq`, `grep`, or to ingest into log shippers and observability stacks.
    - **Single-Process Simplicity**: The logger avoids external dependencies and integrates cleanly into a single
      Python process, using `asyncio.to_thread()` to perform file writes without blocking the event loop.
    - **Log Rotation**: When the active log file exceeds a configured size (`max_log_file_bytes`), it is renamed
      with a filesystem-safe timestamp and a new file is created. This keeps logs manageable for inspection and tailing.
      Set to None to disable limits.
    - **Retention Policy**: When a total disk usage limit is set (`max_log_storage_bytes`), old rotated logs are
      deleted to avoid unbounded disk growth. This is useful as a failsafe in long-running deployments.
      Set to None to disable limits.
    - **Safe Defaults**: Ensures log directory exists, escapes non-ASCII characters if necessary,
      and avoids crashes if a log file cannot be deleted.

    This logger is designed to be the default for the Minions framework, requiring no additional configuration
    to provide usable, structured logs out of the box.
    """

    def __init__(
            self,
            level: int = INFO,
            stdout: bool = True,
            log_dir:str ="logs/",
            log_filename_prefix:str ="minions",
            max_log_file_bytes: int | None = 10 * (1024 ** 2),    # 10 MB
            max_log_storage_bytes: int | None = 100 * (1024 ** 2) # 100 MB
        ):
        super().__init__(level)

        self._stdout = stdout
        self._log_dir = Path(log_dir)
        self._log_filename_prefix = log_filename_prefix
        self._max_log_file_bytes = max_log_file_bytes
        self._lock = asyncio.Lock()
        self._max_log_storage_bytes = max_log_storage_bytes

        if log_filename_prefix.endswith('.log'):
            raise ValueError(
                "log_filename_prefix should not include a file extension like '.log'; "
                "it will be automatically appended during rotation"
            )

        if max_log_file_bytes is not None:
            if max_log_file_bytes < MIN_LOG_FILE_BYTES:
                raise ValueError(
                    f"max_log_file_bytes must be at least {MIN_LOG_FILE_BYTES} bytes "
                    f"(got {max_log_file_bytes}) or set to None for no limit"
                )

        if max_log_storage_bytes is not None:
            if max_log_storage_bytes < MIN_LOG_STORAGE_BYTES:
                raise ValueError(
                    f"max_log_storage_bytes must be at least {MIN_LOG_STORAGE_BYTES} bytes "
                    f"(got {max_log_storage_bytes}) or set to None for no limit"
                )
            if max_log_file_bytes is not None and max_log_storage_bytes < max_log_file_bytes:
                raise ValueError(
                    f"max_log_storage_bytes ({max_log_storage_bytes}) cannot be less than "
                    f"max_log_file_bytes ({max_log_file_bytes})"
                )

        self._log_dir.mkdir(parents=True, exist_ok=True)

        self._path = self._log_dir / f"{self._log_filename_prefix}.log"

    def _print_err(self, e: Exception):
        print(f"[FileLogger error] {e.__class__.__name__}: {e}", file=sys.stderr)

    async def log(self, level: int, msg: str, **kwargs: Any):
        try:
            if level < self._level:
                return

            entry = {
                "ts": self._iso_8601_ts(),
                "level": LEVEL_NAMES.get(level, str(level)),
                "msg": msg,
                **kwargs,
            }

            log_line = serialize(entry).decode() + "\n"

            if self._stdout:

                marker = {
                    "DEBUG": "[..]",
                    "INFO":  "[✓]",
                    "WARNING": "[!]",
                    "ERROR": "[✗]",
                    "CRITICAL": "[!]",
                }.get(entry["level"], "[?]")

                if level == INFO:
                    msg_lower = msg.lower()
                    if "starting" in msg_lower \
                    or "stopping" in msg_lower \
                    or "shutting down" in msg_lower:
                        marker = "[.]" # progress marker

                if self._level >= INFO:
                    kwargs.pop("traceback", None)

                extras = " ".join(
                    f"{k}={serialize(v).decode() if not isinstance(v, str) else v}" for k, v in kwargs.items()
                ).strip()

                print(f"{marker} {msg} {extras}", file=sys.stdout)
            
            async with self._lock:
                await self._rotate_if_needed()
                await asyncio.to_thread(self._write_line, log_line)

        except Exception as e:
            self._print_err(e)

    async def _rotate_if_needed(self):
        try:
            if (
                not self._log_dir.exists()
                or self._max_log_file_bytes is None
                or self._path.stat().st_size < self._max_log_file_bytes
            ):
                return

            ts = self._iso_8601_ts_fs_safe()
            rotated_name = f"{self._log_filename_prefix}_{ts}.log"
            rotated_path = self._log_dir / rotated_name
            
            self._path.rename(rotated_path)

            await self._enforce_storage_limit()
        except Exception as e:
            self._print_err(e)

    async def _enforce_storage_limit(self):
        try:
            if self._max_log_storage_bytes is None:
                return

            files = sorted(
                self._log_dir.glob(f"{self._log_filename_prefix}_*.log"),
                key=lambda p: p.stat().st_mtime
            )
            total_size = sum(f.stat().st_size for f in files)

            while total_size > self._max_log_storage_bytes and files:
                oldest = files.pop(0)
                total_size -= oldest.stat().st_size
                oldest.unlink()
        except Exception as e:
            self._print_err(e)

    def _write_line(self, line: str):
        try:
            with open(self._path, "a", encoding="utf-8") as file:
                file.write(line)
        except Exception as e:
            self._print_err(e)