"""
### Running tests locally
For quick feedback or single files:
- pytest tests/minions/test_gru.py -q

### Full suite with coverage
For a complete, production-equivalent run:
- python tools/run_cov.py
This generates coverage reports in `htmlcov/framework` and `htmlcov/support/`.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess

def run(cmd: list[str]) -> None:
    print("$", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)

def cov_test_support(html_dir: str = "htmlcov/test_support") -> None:
    if os.path.exists(html_dir):
        shutil.rmtree(html_dir)
    run(["coverage", "erase"])
    run([
        "pytest", "tests/assets/support",
        "--cov=tests/assets/support", "--cov-branch",
        "--cov-report=term-missing",
        f"--cov-report=html:{html_dir}",
    ])
    run(["coverage", "erase"])

def cov_minions(html_dir: str = "htmlcov/minions") -> None:
    if os.path.exists(html_dir):
        shutil.rmtree(html_dir)
    run(["coverage", "erase"])
    run([
        "pytest", "tests",
        "--cov=minions", "--cov-branch",
        "--cov-report=term-missing",
        f"--cov-report=html:{html_dir}",
        "--cov-config=.coveragerc",
    ])
    run(["coverage", "erase"])

def cov_all() -> None:
    cov_test_support()
    cov_minions()

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Test/coverage orchestration")
    sub = p.add_subparsers(dest="cmd")

    s1 = sub.add_parser("cov-test-support");  s1.add_argument("--out", default="htmlcov/test_support")
    s2 = sub.add_parser("cov-minions"); s2.add_argument("--out", default="htmlcov/minions")
    sub.add_parser("cov-all")

    args = p.parse_args(argv)

    if not args.cmd:
        cov_all()
        return 0

    if args.cmd == "cov-test-support":
        cov_test_support(args.out)
    elif args.cmd == "cov-minions":
        cov_minions(args.out)
    elif args.cmd == "cov-all":
        cov_all()
    else:
        p.print_help()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
