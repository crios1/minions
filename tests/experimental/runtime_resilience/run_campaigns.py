"""Run the Minions runtime-resilience campaigns in isolated pytest processes."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

CAMPAIGNS: dict[str, str] = {
    "subprocess-recovery": (
        "tests/experimental/runtime_resilience/subprocess_recovery/campaign.py"
    ),
    "stateful-lifecycle": (
        "tests/experimental/runtime_resilience/stateful_lifecycle/campaign.py"
    ),
    "lifecycle-leak": ("tests/experimental/runtime_resilience/lifecycle_leak/campaign.py"),
    "high-fanout-resource": (
        "tests/experimental/runtime_resilience/high_fanout_resource/campaign.py"
    ),
    "concurrent-lifecycle": (
        "tests/experimental/runtime_resilience/concurrent_lifecycle/campaign.py"
    ),
    "resource-failure-storm": (
        "tests/experimental/runtime_resilience/resource_failure_storm/campaign.py"
    ),
    "cancellation-pressure": (
        "tests/experimental/runtime_resilience/cancellation_pressure/campaign.py"
    ),
}
DEFAULT_TIMEOUT_SECONDS = 180.0


@dataclass(frozen=True, slots=True)
class RunSpec:
    run_id: int
    campaign: str
    repetition: int
    target: str


@dataclass(slots=True)
class ActiveRun:
    spec: RunSpec
    process: subprocess.Popen[str]
    started_at: float
    log_path: Path
    run_dir: Path
    timed_out: bool = False


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: int
    campaign: str
    repetition: int
    status: str
    returncode: int
    duration_seconds: float
    log_path: str | None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _pytest_bin(repo_root: Path, explicit: str | None) -> str:
    if explicit:
        return explicit
    venv_pytest = repo_root / ".venv" / "bin" / "pytest"
    if venv_pytest.exists():
        return str(venv_pytest)
    found = shutil.which("pytest")
    if found:
        return found
    raise FileNotFoundError("Could not find pytest; create .venv or pass --pytest-bin.")


def _artifacts_dir(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return Path(tempfile.gettempdir()) / "minions-runtime-resilience" / f"{stamp}-{os.getpid()}"


def _kill_process_tree(pid: int) -> None:
    try:
        root = psutil.Process(pid)
        children = root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return
    for process in reversed(children):
        try:
            process.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    try:
        root.kill()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        pass


def _launch(
    spec: RunSpec,
    *,
    pytest_bin: str,
    pytest_args: list[str],
    artifacts_dir: Path,
    repo_root: Path,
) -> ActiveRun:
    run_dir = artifacts_dir / f"{spec.run_id:04d}-{spec.campaign}-r{spec.repetition}"
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "pytest.log"
    command = [
        pytest_bin,
        "-q",
        "-s",
        "-p",
        "no:cacheprovider",
        f"--basetemp={run_dir / 'basetemp'}",
        spec.target,
        *pytest_args,
    ]
    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        command,
        cwd=repo_root,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    log_file.close()
    print(f"[launch] run={spec.run_id} campaign={spec.campaign} repetition={spec.repetition}")
    return ActiveRun(
        spec=spec,
        process=process,
        started_at=time.monotonic(),
        log_path=log_path,
        run_dir=run_dir,
    )


def _finish(
    run: ActiveRun,
    *,
    retain_passing_artifacts: bool,
) -> RunResult | None:
    returncode = run.process.poll()
    if returncode is None:
        return None
    duration_seconds = time.monotonic() - run.started_at
    status = "timeout" if run.timed_out else ("passed" if returncode == 0 else "failed")
    retained_log: str | None = str(run.log_path)
    if status == "passed" and not retain_passing_artifacts:
        shutil.rmtree(run.run_dir)
        retained_log = None
    print(
        f"[done] run={run.spec.run_id} campaign={run.spec.campaign} "
        f"repetition={run.spec.repetition} status={status} "
        f"duration={duration_seconds:.2f}s"
    )
    return RunResult(
        run_id=run.spec.run_id,
        campaign=run.spec.campaign,
        repetition=run.spec.repetition,
        status=status,
        returncode=returncode,
        duration_seconds=duration_seconds,
        log_path=retained_log,
    )


def _write_json_summary(
    path: Path,
    *,
    started_at: float,
    planned: int,
    results: list[RunResult],
) -> None:
    payload = {
        "elapsed_seconds": time.monotonic() - started_at,
        "planned": planned,
        "total": len(results),
        "not_run": planned - len(results),
        "passed": sum(result.status == "passed" for result in results),
        "failed": sum(result.status == "failed" for result in results),
        "timed_out": sum(result.status == "timeout" for result in results),
        "results": [asdict(result) for result in sorted(results, key=lambda item: item.run_id)],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _print_summary(
    results: list[RunResult],
    *,
    elapsed_seconds: float,
    planned: int,
) -> int:
    print()
    print("Runtime resilience soak summary")
    for campaign in CAMPAIGNS:
        campaign_results = [result for result in results if result.campaign == campaign]
        if not campaign_results:
            continue
        passed = sum(result.status == "passed" for result in campaign_results)
        failed = len(campaign_results) - passed
        duration = sum(result.duration_seconds for result in campaign_results)
        print(
            f"  {campaign}: passed={passed}/{len(campaign_results)} "
            f"failed={failed} worker_seconds={duration:.2f}"
        )
    failures = [result for result in results if result.status != "passed"]
    print(f"  elapsed_seconds: {elapsed_seconds:.2f}")
    print(f"  planned: {planned}")
    print(f"  total: {len(results)}")
    print(f"  not_run: {planned - len(results)}")
    print(f"  passed: {len(results) - len(failures)}")
    print(f"  failed_or_timed_out: {len(failures)}")
    for result in failures:
        print(
            f"  retained_log: campaign={result.campaign} "
            f"repetition={result.repetition} path={result.log_path}"
        )
    return int(bool(failures))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Run runtime-resilience campaigns in isolated pytest processes.")
    )
    parser.add_argument(
        "--campaign",
        action="append",
        choices=tuple(CAMPAIGNS),
        default=[],
        help="Campaign to run; repeatable. Defaults to all campaigns.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Repetitions per campaign. Default: 1.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Maximum concurrent campaign processes. Default: 1.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-process timeout; use 0 to disable. Default: 180.",
    )
    parser.add_argument("--pytest-bin", default=None)
    parser.add_argument("--artifacts-dir", default=None)
    parser.add_argument("--json-summary", default=None)
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Additional pytest argument; repeatable.",
    )
    parser.add_argument("--retain-passing-artifacts", action="store_true")
    parser.add_argument("--stop-on-failure", action="store_true")
    parser.add_argument(
        "--list",
        action="store_true",
        help="List campaign names and exit.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.list:
        for name, target in CAMPAIGNS.items():
            print(f"{name}: {target}")
        return 0
    if args.repeat < 1:
        parser.error("--repeat must be >= 1")
    if args.jobs < 1:
        parser.error("--jobs must be >= 1")
    if args.timeout_seconds < 0:
        parser.error("--timeout-seconds must be >= 0")

    repo_root = _repo_root()
    pytest_bin = _pytest_bin(repo_root, args.pytest_bin)
    artifacts_dir = _artifacts_dir(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    campaigns = args.campaign or list(CAMPAIGNS)
    specs = [
        RunSpec(
            run_id=index,
            campaign=campaign,
            repetition=repetition,
            target=CAMPAIGNS[campaign],
        )
        for index, (campaign, repetition) in enumerate(
            (
                (campaign, repetition)
                for repetition in range(1, args.repeat + 1)
                for campaign in campaigns
            ),
            start=1,
        )
    ]

    print(f"campaigns: {campaigns}")
    print(f"repeat: {args.repeat}")
    print(f"jobs: {args.jobs}")
    print(f"timeout_seconds: {args.timeout_seconds}")
    print(f"artifacts_dir: {artifacts_dir}")
    started_at = time.monotonic()
    pending = list(specs)
    active: list[ActiveRun] = []
    results: list[RunResult] = []
    stop_launching = False
    try:
        while pending or active:
            while pending and len(active) < args.jobs and not stop_launching:
                active.append(
                    _launch(
                        pending.pop(0),
                        pytest_bin=pytest_bin,
                        pytest_args=args.pytest_arg,
                        artifacts_dir=artifacts_dir,
                        repo_root=repo_root,
                    )
                )

            now = time.monotonic()
            for run in active:
                if (
                    args.timeout_seconds > 0
                    and not run.timed_out
                    and now - run.started_at >= args.timeout_seconds
                ):
                    run.timed_out = True
                    _kill_process_tree(run.process.pid)

            remaining: list[ActiveRun] = []
            for run in active:
                result = _finish(
                    run,
                    retain_passing_artifacts=args.retain_passing_artifacts,
                )
                if result is None:
                    remaining.append(run)
                    continue
                results.append(result)
                if args.stop_on_failure and result.status != "passed":
                    stop_launching = True
            active = remaining

            if stop_launching and not active:
                pending.clear()
            elif active:
                time.sleep(0.02)
    except KeyboardInterrupt:
        for run in active:
            _kill_process_tree(run.process.pid)
        print("\nInterrupted; active process trees were killed.")
        return 130

    elapsed_seconds = time.monotonic() - started_at
    if args.json_summary:
        summary_path = Path(args.json_summary).expanduser().resolve()
        _write_json_summary(
            summary_path,
            started_at=started_at,
            planned=len(specs),
            results=results,
        )
        print(f"json_summary: {summary_path}")

    exit_code = _print_summary(
        results,
        elapsed_seconds=elapsed_seconds,
        planned=len(specs),
    )
    if exit_code == 0 and not args.retain_passing_artifacts and artifacts_dir.exists():
        shutil.rmtree(artifacts_dir)
        print("artifacts: <cleaned; all campaigns passed>")
    else:
        print(f"artifacts: {artifacts_dir}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
