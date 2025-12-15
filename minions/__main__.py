from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m minions",
        description="Developer-facing module entrypoints for Minions.",
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

    args, rest = parser.parse_known_args(argv)

    if args.command == "shell":
        from .shell import main as shell_main

        shell_argv = list(rest)
        if args.no_banner:
            shell_argv.insert(0, "--no-banner")
        return shell_main(shell_argv)

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

