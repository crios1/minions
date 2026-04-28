from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if argv_list[:2] == ["tune", "sqlite"]:
        from ._internal._cli.sqlite_tune import main as sqlite_tune_main

        return sqlite_tune_main(argv_list[2:], prog="python -m minions tune sqlite")

    parser = argparse.ArgumentParser(
        prog="python -m minions",
        description="CLI entrypoints for Minions.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    shell_p = sub.add_parser(
        "shell",
        help="Start an exploratory GruShell (not for production deployments).",
    )
    shell_p.add_argument(
        "--no-banner",
        action="store_true",
        help="Disable the startup banner.",
    )

    sub.add_parser(
        "tune",
        help="Run Minions tuning and diagnostics helpers.",
    )

    args, rest = parser.parse_known_args(argv_list)

    if args.command == "shell":
        from .shell import main as shell_main

        shell_argv = list(rest)
        if args.no_banner:
            shell_argv.insert(0, "--no-banner")
        return shell_main(shell_argv)
    if args.command == "tune":
        if not rest:
            parser.error("tune requires a target (currently: sqlite)")
        target, *tune_argv = rest
        if target != "sqlite":
            parser.error(f"Unknown tune target: {target}")
        parser.error("Use `minions tune sqlite ...` to run the SQLiteStateStore tuner")

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
