"""
Repeated pytest runner for flake hunting.

Defaults are tuned for this repository:
- uses the project venv's pytest binary when available
- disables pytest's shared cache provider
- ignores tests listed in `PARALLEL_UNSAFE_TEST_PATHS`
- gives each run its own basetemp, log, and junitxml artifact
- cleans up passing run artifacts by default

Examples:
    python scripts/flake_hunter.py --diagnose-jobs
    python scripts/flake_hunter.py --runs 1000 --jobs 48
    python scripts/flake_hunter.py --runs 1000 --jobs 48 tests/minions/_internal/_domain/gru
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


PARALLEL_UNSAFE_TEST_PATHS: tuple[str, ...] = (
    "tests/minions/_internal/_domain/test_prometheus_metrics.py",
)

MIN_FREE_RAM_BYTES = 1 * 1024 * 1024 * 1024
THROUGHPUT_GAIN_FLOOR = 1.10
PERFORMANCE_TIE_TOLERANCE = 0.02
DEFAULT_DIAGNOSE_SAMPLE_INTERVAL_S = 0.1
DEFAULT_WAITING_REPORT_INTERVAL_S = 30.0
DEFAULT_RUN_TIMEOUT_SECONDS = 300.0


@dataclass
class RunResult:
    run_id: int
    returncode: int
    duration_s: float
    timed_out: bool
    command: list[str]
    log_path: Path
    junit_path: Path
    basetemp_path: Path


@dataclass
class ActiveRun:
    run_id: int
    process: subprocess.Popen[str]
    started_at: float
    timed_out: bool
    command: list[str]
    log_path: Path
    junit_path: Path
    basetemp_path: Path


@dataclass
class CleanupPolicy:
    retain_passing_artifacts: bool


@dataclass
class DiagnosticSample:
    jobs: int
    elapsed_s: float
    throughput_runs_per_s: float
    peak_total_rss_bytes: int
    peak_single_rss_bytes: int
    available_ram_start_bytes: int
    min_available_ram_bytes: int
    swap_sout_delta_bytes: int
    swap_supported: bool
    failures: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run pytest repeatedly across multiple processes to expose flakes."
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=None,
        help="Total number of pytest invocations to execute.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Maximum concurrent pytest processes. Default: 1.",
    )
    parser.add_argument(
        "--diagnose-jobs",
        action="store_true",
        help="Benchmark this machine and recommend a safe `--jobs` value.",
    )
    parser.add_argument(
        "--diagnose-max-jobs",
        type=int,
        default=None,
        help="Maximum concurrency to test during `--diagnose-jobs`.",
    )
    parser.add_argument(
        "--target",
        dest="targets",
        action="append",
        default=[],
        help="Pytest target to run. Repeatable. Defaults to `tests`.",
    )
    parser.add_argument(
        "--pytest-bin",
        default=None,
        help="Explicit pytest executable to use. Defaults to repo `venv/bin/pytest` if present.",
    )
    parser.add_argument(
        "--artifacts-dir",
        default=None,
        help="Directory for logs, junitxml, and basetemp trees. Defaults to a unique temp dir.",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop launching new runs after the first failing pytest invocation.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not force pytest `-q`. Extra pytest args can still override verbosity.",
    )
    parser.add_argument(
        "--retain-passing-artifacts",
        action="store_true",
        help="Keep logs, junitxml, and basetemp for passing runs. Default deletes passing run artifacts.",
    )
    parser.add_argument(
        "--run-timeout-seconds",
        type=float,
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        help=(
            "Kill an individual pytest worker if it exceeds this runtime. "
            "Use 0 to disable. Default: 300."
        ),
    )
    parser.add_argument(
        "positional_targets",
        nargs="*",
        help="Optional pytest targets. Use these instead of --target for convenience.",
    )
    return parser


def resolve_repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def resolve_pytest_bin(repo_root: Path, explicit: str | None) -> str:
    if explicit:
        return explicit

    venv_pytest = repo_root / "venv" / "bin" / "pytest"
    if venv_pytest.exists():
        return str(venv_pytest)

    found = shutil.which("pytest")
    if found:
        return found

    raise FileNotFoundError(
        "Could not find pytest. Pass --pytest-bin explicitly or create the project venv."
    )


def resolve_artifacts_dir(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()

    stamp = time.strftime("%Y%m%d-%H%M%S")
    return Path(tempfile.gettempdir()) / "minions-flake-hunter" / f"{stamp}-{os.getpid()}"


def build_pytest_command(
    *,
    pytest_bin: str,
    targets: list[str],
    pytest_args: list[str],
    quiet: bool,
    junit_path: Path,
    basetemp_path: Path,
) -> list[str]:
    cmd = [pytest_bin]

    if not quiet:
        cmd.append("-q")

    cmd.extend(["-p", "no:cacheprovider"])
    for test_path in PARALLEL_UNSAFE_TEST_PATHS:
        cmd.append(f"--ignore={test_path}")

    cmd.append(f"--basetemp={basetemp_path}")
    cmd.append(f"--junitxml={junit_path}")
    cmd.extend(targets or ["tests"])
    cmd.extend(pytest_args)
    return cmd


def launch_run(
    *,
    run_id: int,
    pytest_bin: str,
    targets: list[str],
    pytest_args: list[str],
    quiet: bool,
    artifacts_dir: Path,
) -> ActiveRun:
    run_dir = artifacts_dir / f"run-{run_id:04d}"
    run_dir.mkdir(parents=True, exist_ok=True)

    junit_path = run_dir / "junit.xml"
    basetemp_path = run_dir / "basetemp"
    log_path = run_dir / "pytest.log"
    command = build_pytest_command(
        pytest_bin=pytest_bin,
        targets=targets,
        pytest_args=pytest_args,
        quiet=quiet,
        junit_path=junit_path,
        basetemp_path=basetemp_path,
    )

    log_file = log_path.open("w", encoding="utf-8")
    print(f"[launch] run={run_id} cmd={shlex.join(command)}")
    process = subprocess.Popen(
        command,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=resolve_repo_root(),
    )
    log_file.close()

    return ActiveRun(
        run_id=run_id,
        process=process,
        started_at=time.monotonic(),
        timed_out=False,
        command=command,
        log_path=log_path,
        junit_path=junit_path,
        basetemp_path=basetemp_path,
    )


def cleanup_run_dir(run: RunResult) -> None:
    run_dir = run.log_path.parent
    if run_dir.exists():
        shutil.rmtree(run_dir)


def format_waiting_status(
    *,
    active: list[ActiveRun],
    completed_runs: int,
    total_runs: int,
    now: float | None = None,
) -> str:
    current_time = now if now is not None else time.monotonic()
    active_run_ids = [run.run_id for run in active]
    active_ids_preview = ",".join(str(run_id) for run_id in active_run_ids[:8])
    if len(active_run_ids) > 8:
        active_ids_preview += ",..."
    oldest_active_s = max(current_time - run.started_at for run in active)
    return (
        "[waiting] "
        f"completed={completed_runs}/{total_runs} "
        f"active={len(active)} "
        f"active_runs=[{active_ids_preview}] "
        f"oldest_active={oldest_active_s:.1f}s"
    )


def kill_process_tree(pid: int) -> None:
    try:
        root = psutil.Process(pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return

    try:
        children = root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, PermissionError):
        children = []

    for proc in reversed(children):
        try:
            proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    try:
        root.kill()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return


def enforce_run_timeouts(
    active: list[ActiveRun],
    *,
    run_timeout_seconds: float,
) -> None:
    if run_timeout_seconds <= 0:
        return

    now = time.monotonic()
    for run in active:
        if run.timed_out:
            continue

        elapsed_s = now - run.started_at
        if elapsed_s < run_timeout_seconds:
            continue

        print(
            f"[timeout] run={run.run_id} elapsed={elapsed_s:.2f}s "
            f"limit={run_timeout_seconds:.2f}s action=kill-process-tree",
            flush=True,
        )
        kill_process_tree(run.process.pid)
        run.timed_out = True


def drain_finished(
    active: list[ActiveRun],
    results: list[RunResult],
    cleanup: CleanupPolicy,
) -> list[ActiveRun]:
    remaining: list[ActiveRun] = []

    for run in active:
        returncode = run.process.poll()
        if returncode is None:
            remaining.append(run)
            continue

        duration_s = time.monotonic() - run.started_at
        result = RunResult(
            run_id=run.run_id,
            returncode=returncode,
            duration_s=duration_s,
            timed_out=run.timed_out,
            command=run.command,
            log_path=run.log_path,
            junit_path=run.junit_path,
            basetemp_path=run.basetemp_path,
        )
        results.append(result)
        if returncode == 0:
            status = "PASS"
        elif run.timed_out:
            status = "TIMEOUT"
        else:
            status = "FAIL"
        print(
            f"[done] run={run.run_id} status={status} rc={returncode} "
            f"duration={duration_s:.2f}s log={run.log_path}"
        )
        if returncode == 0 and not cleanup.retain_passing_artifacts:
            cleanup_run_dir(result)

    return remaining


def print_summary(
    results: list[RunResult],
    artifacts_dir: Path,
    cleanup: CleanupPolicy,
) -> int:
    failures = [result for result in results if result.returncode != 0]
    timeouts = [result for result in failures if result.timed_out]
    passes = len(results) - len(failures)

    artifacts_summary = str(artifacts_dir)
    if not failures and not cleanup.retain_passing_artifacts:
        if artifacts_dir.exists():
            shutil.rmtree(artifacts_dir)
        artifacts_summary = "<cleaned: all runs passed>"

    print()
    print("Summary")
    print(f"  artifacts: {artifacts_summary}")
    print(f"  total runs: {len(results)}")
    print(f"  passed: {passes}")
    print(f"  failed: {len(failures)}")
    if timeouts:
        print(f"  timed out: {len(timeouts)}")

    if failures:
        print("  failing runs:")
        for result in failures:
            status = "timeout" if result.timed_out else "failed"
            print(
                f"    run={result.run_id} status={status} rc={result.returncode} "
                f"log={result.log_path} junit={result.junit_path}"
            )
        return 1

    return 0


def format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{num_bytes}B"


def process_tree_rss_bytes(pid: int) -> int:
    try:
        root = psutil.Process(pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0

    try:
        rss = root.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return 0

    try:
        children = root.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, PermissionError):
        children = []

    for proc in children:
        try:
            rss += proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return rss


def sample_active_memory(active: list[ActiveRun]) -> tuple[int, int]:
    total_rss = 0
    peak_single_rss = 0
    for run in active:
        rss = process_tree_rss_bytes(run.process.pid)
        total_rss += rss
        if rss > peak_single_rss:
            peak_single_rss = rss
    return total_rss, peak_single_rss


def safe_swap_sout_bytes() -> tuple[int, bool]:
    try:
        return psutil.swap_memory().sout, True
    except (AttributeError, NotImplementedError, OSError):
        return 0, False


def sample_is_usable(sample: DiagnosticSample) -> bool:
    return sample.failures == 0 and not sample_is_memory_tight(sample)


def choose_recommended_jobs(samples: list[DiagnosticSample]) -> int:
    usable = [sample for sample in samples if sample_is_usable(sample)]
    if not usable:
        return 1

    best_throughput = max(sample.throughput_runs_per_s for sample in usable)
    threshold = best_throughput * (1.0 - PERFORMANCE_TIE_TOLERANCE)
    candidates = [
        sample
        for sample in usable
        if sample.throughput_runs_per_s >= threshold
    ]
    return min(candidates, key=lambda sample: sample.jobs).jobs


def run_diagnostic_level(
    *,
    jobs: int,
    pytest_bin: str,
    targets: list[str],
    pytest_args: list[str],
    quiet: bool,
    artifacts_dir: Path,
    cleanup: CleanupPolicy,
    run_timeout_seconds: float,
) -> DiagnosticSample:
    level_dir = artifacts_dir / f"diagnose-jobs-{jobs:02d}"
    level_dir.mkdir(parents=True, exist_ok=True)

    active = [
        launch_run(
            run_id=run_id,
            pytest_bin=pytest_bin,
            targets=targets,
            pytest_args=pytest_args,
            quiet=quiet,
            artifacts_dir=level_dir,
        )
        for run_id in range(1, jobs + 1)
    ]

    results: list[RunResult] = []
    started_at = time.monotonic()
    last_progress_at = started_at
    vm_start = psutil.virtual_memory()
    swap_start_sout, swap_supported = safe_swap_sout_bytes()
    peak_total_rss_bytes = 0
    peak_single_rss_bytes = 0
    min_available_ram_bytes = vm_start.available

    while active:
        enforce_run_timeouts(
            active,
            run_timeout_seconds=run_timeout_seconds,
        )
        total_rss_bytes, single_rss_bytes = sample_active_memory(active)
        peak_total_rss_bytes = max(peak_total_rss_bytes, total_rss_bytes)
        peak_single_rss_bytes = max(peak_single_rss_bytes, single_rss_bytes)
        min_available_ram_bytes = min(min_available_ram_bytes, psutil.virtual_memory().available)

        time.sleep(DEFAULT_DIAGNOSE_SAMPLE_INTERVAL_S)
        results_before = len(results)
        active = drain_finished(active, results, cleanup)
        if len(results) > results_before:
            last_progress_at = time.monotonic()
        elif active and time.monotonic() - last_progress_at >= DEFAULT_WAITING_REPORT_INTERVAL_S:
            print(
                format_waiting_status(
                    active=active,
                    completed_runs=len(results),
                    total_runs=jobs,
                ),
                flush=True,
            )
            last_progress_at = time.monotonic()

    elapsed_s = time.monotonic() - started_at
    swap_end_sout, swap_supported_end = safe_swap_sout_bytes()
    failures = sum(1 for result in results if result.returncode != 0)

    if failures == 0 and not cleanup.retain_passing_artifacts and level_dir.exists():
        shutil.rmtree(level_dir)

    return DiagnosticSample(
        jobs=jobs,
        elapsed_s=elapsed_s,
        throughput_runs_per_s=jobs / elapsed_s if elapsed_s > 0 else 0.0,
        peak_total_rss_bytes=peak_total_rss_bytes,
        peak_single_rss_bytes=peak_single_rss_bytes,
        available_ram_start_bytes=vm_start.available,
        min_available_ram_bytes=min_available_ram_bytes,
        swap_sout_delta_bytes=max(0, swap_end_sout - swap_start_sout),
        swap_supported=swap_supported and swap_supported_end,
        failures=failures,
    )


def sample_is_memory_tight(sample: DiagnosticSample) -> bool:
    free_ram_floor = min(sample.available_ram_start_bytes * 0.15, MIN_FREE_RAM_BYTES)
    return sample.min_available_ram_bytes <= free_ram_floor


def print_diagnostic_sample(
    sample: DiagnosticSample,
    *,
    previous: DiagnosticSample | None,
) -> None:
    gain = None
    if previous is not None and previous.throughput_runs_per_s > 0:
        gain = sample.throughput_runs_per_s / previous.throughput_runs_per_s

    gain_text = f"{gain:.2f}x" if gain is not None else "-"
    swap_text = format_bytes(sample.swap_sout_delta_bytes) if sample.swap_supported else "n/a"
    print(
        "[diagnose] "
        f"jobs={sample.jobs} "
        f"elapsed={sample.elapsed_s:.2f}s "
        f"throughput={sample.throughput_runs_per_s:.2f} runs/s "
        f"gain={gain_text} "
        f"peak_total_rss={format_bytes(sample.peak_total_rss_bytes)} "
        f"peak_single_rss={format_bytes(sample.peak_single_rss_bytes)} "
        f"min_avail_ram={format_bytes(sample.min_available_ram_bytes)} "
        f"swap_out={swap_text} "
        f"failures={sample.failures}"
    )


def run_job_diagnostic(
    *,
    pytest_bin: str,
    targets: list[str],
    pytest_args: list[str],
    quiet: bool,
    artifacts_dir: Path,
    cleanup: CleanupPolicy,
    diagnose_max_jobs: int | None,
    run_timeout_seconds: float,
) -> int:
    samples_by_jobs: dict[int, DiagnosticSample] = {}
    tested_levels_in_order: list[int] = []
    stop_reason = "search completed"

    print("Diagnostic Mode")
    print(f"  targets: {targets or ['tests']}")
    ceiling_text = (
        str(diagnose_max_jobs)
        if diagnose_max_jobs is not None
        else "none (auto-search until throughput knee or guardrail)"
    )
    print(f"  max jobs to test: {ceiling_text}")
    print("  search strategy: exponential expansion, then interval bisection")
    print()

    def get_or_run_sample(level: int, reference: DiagnosticSample | None) -> DiagnosticSample:
        existing = samples_by_jobs.get(level)
        if existing is not None:
            return existing

        sample = run_diagnostic_level(
            jobs=level,
            pytest_bin=pytest_bin,
            targets=targets,
            pytest_args=pytest_args,
            quiet=quiet,
            artifacts_dir=artifacts_dir,
            cleanup=cleanup,
            run_timeout_seconds=run_timeout_seconds,
        )
        samples_by_jobs[level] = sample
        tested_levels_in_order.append(level)
        print_diagnostic_sample(sample, previous=reference)
        return sample

    previous: DiagnosticSample | None = None
    current_jobs = 1
    refine_low: int | None = None
    refine_high: int | None = None

    while True:
        current = get_or_run_sample(current_jobs, previous)

        if current.failures:
            stop_reason = f"test failures appeared at jobs={current_jobs}"
            if previous is not None:
                refine_low = previous.jobs
                refine_high = current_jobs
            break

        if sample_is_memory_tight(current):
            stop_reason = f"available RAM got tight at jobs={current_jobs}"
            if previous is not None:
                refine_low = previous.jobs
                refine_high = current_jobs
            break

        if previous is not None and previous.throughput_runs_per_s > 0:
            gain = current.throughput_runs_per_s / previous.throughput_runs_per_s
            if gain < THROUGHPUT_GAIN_FLOOR:
                stop_reason = (
                    f"throughput gain flattened below {THROUGHPUT_GAIN_FLOOR:.2f}x "
                    f"at jobs={current_jobs}"
                )
                refine_low = previous.jobs
                refine_high = current_jobs
                break

        if diagnose_max_jobs is not None and current_jobs >= diagnose_max_jobs:
            stop_reason = "user-specified maximum tested concurrency reached"
            break

        previous = current
        if diagnose_max_jobs is None:
            current_jobs = current_jobs * 2
        else:
            current_jobs = min(current_jobs * 2, diagnose_max_jobs)

    if refine_low is not None and refine_high is not None:
        low = refine_low
        high = refine_high
        while high - low > 1:
            mid = low + (high - low) // 2
            low_sample = samples_by_jobs[low]
            mid_sample = get_or_run_sample(mid, low_sample)

            if mid_sample.failures or sample_is_memory_tight(mid_sample):
                high = mid
                continue

            if low_sample.throughput_runs_per_s <= 0:
                low = mid
                continue

            gain = mid_sample.throughput_runs_per_s / low_sample.throughput_runs_per_s
            if gain < THROUGHPUT_GAIN_FLOOR:
                high = mid
            else:
                low = mid

    samples = [samples_by_jobs[level] for level in sorted(samples_by_jobs)]
    recommended_jobs = choose_recommended_jobs(samples)
    peak_single_rss_bytes = max(sample.peak_single_rss_bytes for sample in samples)
    current_available_ram_bytes = psutil.virtual_memory().available

    print()
    print("Diagnostic Summary")
    print(f"  recommended jobs: {recommended_jobs}")
    print(f"  current available RAM: {format_bytes(current_available_ram_bytes)}")
    print(f"  peak worker RSS: {format_bytes(peak_single_rss_bytes)}")
    print(f"  stop reason: {stop_reason}")
    print(f"  tested levels: {sorted(tested_levels_in_order)}")
    print("  recommendation basis: measured throughput, with memory-pressure guardrails")
    # print("  note: higher concurrency may fit in RAM and still be slower due to CPU, disk, or scheduler contention")

    if not cleanup.retain_passing_artifacts:
        had_failures = any(sample.failures > 0 for sample in samples)
        if had_failures:
            artifacts_status = str(artifacts_dir)
        else:
            if artifacts_dir.exists():
                shutil.rmtree(artifacts_dir)
            artifacts_status = "<cleaned: all diagnostic runs passed>"
    else:
        artifacts_status = str(artifacts_dir)
    print(f"  artifacts: {artifacts_status}")

    return 0


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.jobs < 1:
        parser.error("--jobs must be >= 1")
    if args.diagnose_max_jobs is not None and args.diagnose_max_jobs < 1:
        parser.error("--diagnose-max-jobs must be >= 1")
    if args.runs is not None and args.runs < 1:
        parser.error("--runs must be >= 1")
    if args.run_timeout_seconds < 0:
        parser.error("--run-timeout-seconds must be >= 0")
    if args.diagnose_jobs:
        if args.runs is not None:
            parser.error("--runs cannot be used with --diagnose-jobs")
    elif args.runs is None:
        parser.error("--runs is required unless --diagnose-jobs is used")


def main(argv: list[str] | None = None) -> int:
    overall_started_at = time.monotonic()
    parser = build_parser()
    args, pytest_args = parser.parse_known_args(argv)
    validate_args(parser, args)

    repo_root = resolve_repo_root()
    pytest_bin = resolve_pytest_bin(repo_root, args.pytest_bin)
    artifacts_dir = resolve_artifacts_dir(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    cleanup = CleanupPolicy(
        retain_passing_artifacts=args.retain_passing_artifacts,
    )

    targets = [*args.targets, *args.positional_targets]

    print(f"artifacts dir: {artifacts_dir}")
    print(f"pytest bin: {pytest_bin}")
    print(f"targets: {targets or ['tests']}")
    print(f"extra pytest args: {pytest_args or []}")
    print(f"run timeout seconds: {args.run_timeout_seconds}")

    if args.diagnose_jobs:
        print(f"diagnose jobs: true")
        print(
            "diagnose max jobs: "
            + (
                str(args.diagnose_max_jobs)
                if args.diagnose_max_jobs is not None
                else "none (auto-search)"
            )
        )
        print()
        exit_code = run_job_diagnostic(
            pytest_bin=pytest_bin,
            targets=targets,
            pytest_args=pytest_args,
            quiet=args.quiet,
            artifacts_dir=artifacts_dir,
            cleanup=cleanup,
            diagnose_max_jobs=args.diagnose_max_jobs,
            run_timeout_seconds=args.run_timeout_seconds,
        )
        print(f"Execution Elapsed: {time.monotonic() - overall_started_at:.2f}s")
        return exit_code

    active: list[ActiveRun] = []
    results: list[RunResult] = []
    next_run_id = 1
    stop_launching = False
    last_progress_at = time.monotonic()

    print(f"jobs: {args.jobs}")
    print(f"runs: {args.runs}")
    print()

    while len(results) < args.runs:
        while (
            not stop_launching
            and next_run_id <= args.runs
            and len(active) < args.jobs
        ):
            active.append(
                launch_run(
                    run_id=next_run_id,
                    pytest_bin=pytest_bin,
                    targets=targets,
                    pytest_args=pytest_args,
                    quiet=args.quiet,
                    artifacts_dir=artifacts_dir,
                )
            )
            next_run_id += 1

        if not active:
            break

        enforce_run_timeouts(
            active,
            run_timeout_seconds=args.run_timeout_seconds,
        )
        time.sleep(0.2)
        results_before = len(results)
        active = drain_finished(active, results, cleanup)
        if len(results) > results_before:
            last_progress_at = time.monotonic()
        elif active and time.monotonic() - last_progress_at >= DEFAULT_WAITING_REPORT_INTERVAL_S:
            print(
                format_waiting_status(
                    active=active,
                    completed_runs=len(results),
                    total_runs=args.runs,
                ),
                flush=True,
            )
            last_progress_at = time.monotonic()

        if args.stop_on_failure and any(result.returncode != 0 for result in results):
            stop_launching = True

    for run in active:
        run.process.wait()
    active = drain_finished(active, results, cleanup)

    exit_code = print_summary(results, artifacts_dir, cleanup)
    print(f"Execution Elapsed: {time.monotonic() - overall_started_at:.2f}s")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
