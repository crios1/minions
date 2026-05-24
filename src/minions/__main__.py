from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    argv_list = list(sys.argv[1:] if argv is None else argv)
    if argv_list[:2] == ["tune", "sqlite"]:
        from ._internal._cli.sqlite_tune import main as sqlite_tune_main

        return sqlite_tune_main(argv_list[2:], prog="python -m minions tune sqlite")
    if argv_list[:2] == ["stamp", "component-ids"]:
        from ._internal._cli.component_id_stamp import main as component_id_stamp_main

        return component_id_stamp_main(argv_list[2:], prog="python -m minions stamp component-ids")
    if argv_list[:2] == ["stamp", "config-ids"]:
        from ._internal._cli.config_id_stamp import main as config_id_stamp_main

        return config_id_stamp_main(argv_list[2:], prog="python -m minions stamp config-ids")
    if argv_list[:2] == ["stamp", "all"]:
        from ._internal._cli.stamp_all import main as stamp_all_main

        return stamp_all_main(argv_list[2:], prog="python -m minions stamp all")
    if argv_list[:2] == ["doctor", "ids"]:
        from ._internal._cli.doctor_ids import main as doctor_ids_main

        return doctor_ids_main(argv_list[2:], prog="python -m minions doctor ids")

    parser = argparse.ArgumentParser(
        prog="python -m minions",
        description="CLI entrypoints for Minions.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    shell_p = sub.add_parser(
        "shell",
        help="Start deprecated GruShell local helper (not for production deployments).",
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
    sub.add_parser(
        "stamp",
        help="Stamp durable metadata into Minions source files.",
    )
    sub.add_parser(
        "doctor",
        help="Run non-mutating project diagnostics.",
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
        target, *_ = rest
        if target != "sqlite":
            parser.error(f"Unknown tune target: {target}")
        parser.error("Use `minions tune sqlite ...` to run the SQLiteStateStore tuner")
    if args.command == "stamp":
        if not rest:
            parser.error("stamp requires a target (currently: component-ids, config-ids, all)")
        target, *_ = rest
        if target not in {"component-ids", "config-ids", "all"}:
            parser.error(f"Unknown stamp target: {target}")
        parser.error("Use `minions stamp component-ids ...`, `minions stamp config-ids ...`, or `minions stamp all ...`")
    if args.command == "doctor":
        if not rest:
            parser.error("doctor requires a target (currently: ids)")
        target, *_ = rest
        if target != "ids":
            parser.error(f"Unknown doctor target: {target}")
        parser.error("Use `minions doctor ids ...` to inspect durable identity metadata")

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
