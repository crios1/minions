from __future__ import annotations

import argparse
import asyncio

from ._internal._domain.gru import Gru
from ._internal._domain.gru_shell import GruShell
from ._internal._framework.logger_noop import NoOpLogger
from ._internal._framework.metrics_noop import NoOpMetrics
from ._internal._framework.state_store_noop import NoOpStateStore

__all__ = ["GruShell", "main"]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m minions shell",
        description=(
            "Start deprecated GruShell for local experimentation. "
            "This helper will be replaced by the planned `minions gru serve` / "
            "`minions gru attach` control model."
        ),
    )
    p.add_argument(
        "--no-banner",
        action="store_true",
        help="Disable the startup banner.",
    )
    return p


async def _run_shell(*, show_banner: bool) -> None:
    # Intentionally use no-op implementations so exploratory usage does not create files
    # (e.g. SQLite state) or start network listeners (e.g. Prometheus).
    gru = await Gru.create(NoOpStateStore(), NoOpLogger(), NoOpMetrics())
    try:
        if show_banner:
            print(
                "Starting GruShell (deprecated local helper).\n"
                "GruShell is retained for experimentation and as serve/attach design material.\n"
                "For production control, use the planned `minions gru serve` / `minions gru attach` model once available.\n"
                "Type `help` to list commands."
            )

        shell = GruShell(gru)
        await asyncio.to_thread(shell.cmdloop)
    finally:
        if getattr(gru, "_started", False) and not getattr(gru, "_shutdown", False):
            await gru.shutdown()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    asyncio.run(_run_shell(show_banner=not args.no_banner))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
