"""Developer wrapper for the supported SQLite tuning CLI."""

from __future__ import annotations

from minions._internal._cli.sqlite_tune import main as sqlite_tune_main


def main(argv: list[str] | None = None) -> int:
    return sqlite_tune_main(argv, prog="python scripts/state_store_sqlite_perf.py")


if __name__ == "__main__":
    raise SystemExit(main())
