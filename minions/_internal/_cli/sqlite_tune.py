"""SQLiteStateStore transaction-grouping tuning CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import tempfile
import time
from dataclasses import dataclass

from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.state_store_sqlite import SQLiteStateStore

__all__ = ["main"]


@dataclass(frozen=True)
class RunResult:
    label: str
    batch_max_queued_writes: int
    flush_delay_ms: int | None
    interarrival_delay_ms: int | None
    ops: int
    concurrency: int
    payload_bytes: int
    elapsed_ms: float
    ops_per_sec: float
    save_p50_ms: float
    save_p95_ms: float
    commits: int
    rows_per_commit: float
    db_bytes: int


@dataclass(frozen=True)
class TunedConfigScore:
    label: str
    batch_max_queued_writes: int
    flush_delay_ms: int
    interarrival_delay_ms: int | None
    score: float
    low_p95_ms: float
    low_p95_vs_immediate: float
    medium_p95_ms: float
    medium_p95_vs_immediate: float
    upper_ops_per_sec: float
    upper_speedup: float
    upper_rows_per_commit: float
    upper_commits: float
    db_kib: float


@dataclass(frozen=True)
class SelectedRecommendation:
    name: str
    score: TunedConfigScore
    selection_status: str
    selection_reason: str


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = round((len(ordered) - 1) * percentile)
    return ordered[idx]


async def _write_wave(
    store: SQLiteStateStore,
    *,
    start: int,
    count: int,
    payload: bytes,
    latencies_ms: list[float] | None,
) -> None:
    async def save_one(i: int) -> None:
        t0 = time.perf_counter()
        await store.save_context(f"wf-{i}", f"orch-{i % 8}", payload)
        if latencies_ms is not None:
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)

    await asyncio.gather(*(save_one(i) for i in range(start, start + count)))


async def _run_once(
    *,
    label: str,
    db_path: str,
    batch_tuning: str = "manual",
    batch_max_queued_writes: int | None,
    batch_max_interarrival_delay_ms: int | None,
    ops: int,
    concurrency: int,
    payload_bytes: int,
    warmup_ops: int,
    flush_delay_ms: int | None,
) -> RunResult:
    store_kwargs: dict[str, int | str] = {"batch_tuning": batch_tuning}
    if batch_tuning == "manual":
        if batch_max_queued_writes is None:
            raise ValueError("manual tuning requires batch_max_queued_writes")
        store_kwargs["batch_max_queued_writes"] = batch_max_queued_writes
    elif batch_tuning == "calibrated":
        if batch_max_queued_writes is not None or flush_delay_ms is not None:
            raise ValueError("calibrated tuning derives batch settings from startup measurements")
    else:
        raise ValueError(f"unexpected batch_tuning: {batch_tuning!r}")
    if flush_delay_ms is not None and batch_tuning == "manual":
        store_kwargs["batch_max_flush_delay_ms"] = flush_delay_ms
    if batch_max_interarrival_delay_ms is not None:
        store_kwargs["batch_max_interarrival_delay_ms"] = batch_max_interarrival_delay_ms
    store = SQLiteStateStore(db_path=db_path, logger=NoOpLogger(), **store_kwargs)
    commit_count = 0
    row_count = 0
    original_record_commit_metrics = store._record_commit_metrics

    def record_commit_metrics(dt_ms: float, rows: int) -> None:
        nonlocal commit_count, row_count
        commit_count += 1
        row_count += rows
        original_record_commit_metrics(dt_ms, rows)

    store._record_commit_metrics = record_commit_metrics
    payload = b"x" * payload_bytes
    await store.startup()
    effective_batch_max_queued_writes = store._batch_max_queued_writes
    if effective_batch_max_queued_writes is None:
        raise RuntimeError("batch_max_queued_writes was not resolved during startup")
    effective_flush_delay_ms = None if effective_batch_max_queued_writes == 1 else store._batch_max_flush_delay_ms
    try:
        warmup = min(warmup_ops, ops)
        for start in range(0, warmup, concurrency):
            await _write_wave(
                store,
                start=start,
                count=min(concurrency, warmup - start),
                payload=payload,
                latencies_ms=None,
            )
        await store._flush()
        store._metric_commit_latency_ms_hist.clear()
        commit_count = 0
        row_count = 0

        latencies_ms: list[float] = []
        t0 = time.perf_counter()
        for start in range(0, ops, concurrency):
            await _write_wave(
                store,
                start=start + warmup,
                count=min(concurrency, ops - start),
                payload=payload,
                latencies_ms=latencies_ms,
            )
        await store._flush()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
    finally:
        await store.shutdown()

    db_bytes = sum(
        os.path.getsize(path)
        for path in (db_path, f"{db_path}-wal", f"{db_path}-shm")
        if os.path.exists(path)
    )
    return RunResult(
        label=label,
        batch_max_queued_writes=effective_batch_max_queued_writes,
        flush_delay_ms=effective_flush_delay_ms,
        interarrival_delay_ms=batch_max_interarrival_delay_ms,
        ops=ops,
        concurrency=concurrency,
        payload_bytes=payload_bytes,
        elapsed_ms=elapsed_ms,
        ops_per_sec=ops / (elapsed_ms / 1000.0),
        save_p50_ms=statistics.median(latencies_ms),
        save_p95_ms=_percentile(latencies_ms, 0.95),
        commits=commit_count,
        rows_per_commit=row_count / commit_count if commit_count else 0.0,
        db_bytes=db_bytes,
    )


async def _run_benchmark(args: argparse.Namespace) -> list[RunResult]:
    results: list[RunResult] = []
    with tempfile.TemporaryDirectory(prefix="minions-sqlite-bench-") as tmp_dir:
        for round_index in range(args.rounds):
            round_dir = os.path.join(tmp_dir, f"round-{round_index}")
            os.mkdir(round_dir)
            for label, batch_tuning, batch_max_queued_writes, flush_delay_ms in (
                ("immediate", "manual", 1, None),
                (
                    "autotuned" if args.batched_tuning == "calibrated" else "batched",
                    args.batched_tuning,
                    None if args.batched_tuning == "calibrated" else args.batched_batch_max_queued_writes,
                    None if args.batched_tuning == "calibrated" else args.flush_delay_ms,
                ),
            ):
                result = await _run_once(
                    label=label,
                    db_path=os.path.join(round_dir, f"{label}.sqlite3"),
                    batch_tuning=batch_tuning,
                    batch_max_queued_writes=batch_max_queued_writes,
                    batch_max_interarrival_delay_ms=args.batch_max_interarrival_delay_ms,
                    ops=args.ops,
                    concurrency=args.concurrency,
                    payload_bytes=args.payload_bytes,
                    warmup_ops=args.warmup_ops,
                    flush_delay_ms=flush_delay_ms,
                )
                results.append(result)
    return results


async def _run_tradeoff_profile(args: argparse.Namespace) -> list[RunResult]:
    results: list[RunResult] = []
    scenarios = (
        ("low", max(1, args.profile_low_ops), max(1, args.profile_low_concurrency)),
        ("upper", max(1, args.profile_upper_ops), max(1, args.profile_upper_concurrency)),
    )
    with tempfile.TemporaryDirectory(prefix="minions-sqlite-bench-profile-") as tmp_dir:
        for scenario, ops, concurrency in scenarios:
            warmup_ops = min(args.warmup_ops, max(concurrency, ops // 4))
            for round_index in range(args.rounds):
                round_dir = os.path.join(tmp_dir, f"{scenario}-round-{round_index}")
                os.mkdir(round_dir)
                modes = [
                    (f"{scenario}:immediate", "manual", 1, None, None),
                    (
                        f"{scenario}:autotuned" if args.batched_tuning == "calibrated" else f"{scenario}:batched",
                        args.batched_tuning,
                        None if args.batched_tuning == "calibrated" else args.batched_batch_max_queued_writes,
                        None if args.batched_tuning == "calibrated" else args.flush_delay_ms,
                        args.batch_max_interarrival_delay_ms,
                    ),
                ]
                if args.compare_interarrival and args.batch_max_interarrival_delay_ms is not None:
                    modes[1] = (
                        f"{scenario}:autotuned-base" if args.batched_tuning == "calibrated" else f"{scenario}:batched-base",
                        args.batched_tuning,
                        None if args.batched_tuning == "calibrated" else args.batched_batch_max_queued_writes,
                        None if args.batched_tuning == "calibrated" else args.flush_delay_ms,
                        None,
                    )
                    modes.append(
                        (
                            f"{scenario}:autotuned-inter" if args.batched_tuning == "calibrated" else f"{scenario}:batched-inter",
                            args.batched_tuning,
                            None if args.batched_tuning == "calibrated" else args.batched_batch_max_queued_writes,
                            None if args.batched_tuning == "calibrated" else args.flush_delay_ms,
                            args.batch_max_interarrival_delay_ms,
                        )
                    )
                for label, batch_tuning, batch_max_queued_writes, flush_delay_ms, interarrival_delay_ms in modes:
                    results.append(
                        await _run_once(
                            label=label,
                            db_path=os.path.join(round_dir, f"{label.replace(':', '-')}.sqlite3"),
                            batch_tuning=batch_tuning,
                            batch_max_queued_writes=batch_max_queued_writes,
                            batch_max_interarrival_delay_ms=interarrival_delay_ms,
                            ops=ops,
                            concurrency=concurrency,
                            payload_bytes=args.payload_bytes,
                            warmup_ops=warmup_ops,
                            flush_delay_ms=flush_delay_ms,
                        )
                    )
    return results


async def _run_tuning_sweep(args: argparse.Namespace) -> list[RunResult]:
    results: list[RunResult] = []
    batch_sizes = _parse_int_list(args.tune_batch_max_queued_writes)
    flush_delays_ms = _parse_int_list(args.tune_flush_delay_ms)
    interarrival_delays_ms = _parse_optional_int_list(args.tune_interarrival_delay_ms)
    profiles = (
        ("low", max(1, args.profile_low_ops), max(1, args.profile_low_concurrency)),
        ("medium", max(1, args.profile_medium_ops), max(1, args.profile_medium_concurrency)),
        ("upper", max(1, args.profile_upper_ops), max(1, args.profile_upper_concurrency)),
    )

    with tempfile.TemporaryDirectory(prefix="minions-sqlite-bench-tune-") as tmp_dir:
        for round_index in range(args.rounds):
            for profile, ops, concurrency in profiles:
                warmup_ops = min(args.warmup_ops, max(concurrency, ops // 4))
                profile_dir = os.path.join(tmp_dir, f"round-{round_index}-{profile}")
                os.mkdir(profile_dir)
                results.append(
                    await _run_once(
                        label=f"{profile}:immediate",
                        db_path=os.path.join(profile_dir, "immediate.sqlite3"),
                        batch_tuning="manual",
                        batch_max_queued_writes=1,
                        batch_max_interarrival_delay_ms=None,
                        ops=ops,
                        concurrency=concurrency,
                        payload_bytes=args.payload_bytes,
                        warmup_ops=warmup_ops,
                        flush_delay_ms=None,
                    )
                )
                for batch_size in batch_sizes:
                    for flush_delay_ms in flush_delays_ms:
                        for interarrival_delay_ms in interarrival_delays_ms:
                            interarrival_label = (
                                "none"
                                if interarrival_delay_ms is None
                                else str(interarrival_delay_ms)
                            )
                            config_label = f"q{batch_size}-f{flush_delay_ms}-i{interarrival_label}"
                            results.append(
                                await _run_once(
                                    label=f"{profile}:{config_label}",
                                    db_path=os.path.join(profile_dir, f"{config_label}.sqlite3"),
                                    batch_tuning="manual",
                                    batch_max_queued_writes=batch_size,
                                    batch_max_interarrival_delay_ms=interarrival_delay_ms,
                                    ops=ops,
                                    concurrency=concurrency,
                                    payload_bytes=args.payload_bytes,
                                    warmup_ops=warmup_ops,
                                    flush_delay_ms=flush_delay_ms,
                                )
                            )
    return results


async def _run_delay_sweep(args: argparse.Namespace) -> list[RunResult]:
    results: list[RunResult] = []
    delays = [int(value) for value in args.sweep_flush_delay_ms.split(",")]
    batch_sizes = (
        [int(value) for value in args.sweep_batch_max_queued_writes.split(",")]
        if args.sweep_batch_max_queued_writes
        else [args.batched_batch_max_queued_writes]
    )
    concurrencies = [int(value) for value in args.sweep_concurrency.split(",")]
    with tempfile.TemporaryDirectory(prefix="minions-sqlite-bench-sweep-") as tmp_dir:
        for concurrency in concurrencies:
            ops = max(args.ops, concurrency * args.waves)
            warmup_ops = min(args.warmup_ops, max(concurrency, ops // 4))
            for round_index in range(args.rounds):
                round_dir = os.path.join(tmp_dir, f"c{concurrency}-round-{round_index}")
                os.mkdir(round_dir)
                results.append(
                    await _run_once(
                        label="immediate",
                        db_path=os.path.join(round_dir, "immediate.sqlite3"),
                        batch_tuning="manual",
                        batch_max_queued_writes=1,
                        batch_max_interarrival_delay_ms=None,
                        ops=ops,
                        concurrency=concurrency,
                        payload_bytes=args.payload_bytes,
                        warmup_ops=warmup_ops,
                        flush_delay_ms=None,
                    )
                )
                for batch_size in batch_sizes:
                    for delay_ms in delays:
                        results.append(
                            await _run_once(
                                label="batched",
                                db_path=os.path.join(round_dir, f"batched-{batch_size}-{delay_ms}.sqlite3"),
                                batch_tuning="manual",
                                batch_max_queued_writes=batch_size,
                                batch_max_interarrival_delay_ms=args.batch_max_interarrival_delay_ms,
                                ops=ops,
                                concurrency=concurrency,
                                payload_bytes=args.payload_bytes,
                                warmup_ops=warmup_ops,
                                flush_delay_ms=delay_ms,
                            )
                        )
    return results


def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _parse_int_list(raw: str) -> list[int]:
    values = [int(value.strip()) for value in raw.split(",") if value.strip()]
    if not values:
        raise ValueError("expected at least one integer value")
    return values


def _parse_optional_int_list(raw: str) -> list[int | None]:
    values: list[int | None] = []
    for value in raw.split(","):
        stripped = value.strip()
        if not stripped:
            continue
        values.append(None if stripped.lower() in {"none", "null", "-"} else int(stripped))
    if not values:
        raise ValueError("expected at least one integer value or 'none'")
    return values


def _group_tuning_results(
    results: list[RunResult],
) -> tuple[dict[str, list[RunResult]], dict[tuple[str, str], list[RunResult]]]:
    immediate: dict[str, list[RunResult]] = {}
    by_profile_label: dict[tuple[str, str], list[RunResult]] = {}
    for result in results:
        profile, label = result.label.split(":", 1)
        by_profile_label.setdefault((profile, label), []).append(result)
        if label == "immediate":
            immediate.setdefault(profile, []).append(result)
    return immediate, by_profile_label


def _score_tuned_configs(
    results: list[RunResult],
    args: argparse.Namespace,
) -> tuple[list[TunedConfigScore], dict[str, list[RunResult]]]:
    immediate, by_profile_label = _group_tuning_results(results)
    config_labels = sorted({
        label
        for _profile, label in by_profile_label
        if label != "immediate"
    })
    best_upper_ops_per_sec = max(
        _mean([r.ops_per_sec for r in by_profile_label[("upper", label)]])
        for label in config_labels
    )
    immediate_low_p95_ms = _mean([r.save_p95_ms for r in immediate["low"]])
    immediate_medium_p95_ms = _mean([r.save_p95_ms for r in immediate["medium"]])
    immediate_upper_ops_per_sec = _mean([r.ops_per_sec for r in immediate["upper"]])

    scores: list[TunedConfigScore] = []
    for label in config_labels:
        low_runs = by_profile_label[("low", label)]
        medium_runs = by_profile_label[("medium", label)]
        upper_runs = by_profile_label[("upper", label)]
        low_p95_ms = _mean([r.save_p95_ms for r in low_runs])
        medium_p95_ms = _mean([r.save_p95_ms for r in medium_runs])
        upper_ops_per_sec = _mean([r.ops_per_sec for r in upper_runs])
        upper_rows_per_commit = _mean([r.rows_per_commit for r in upper_runs])
        upper_commits = _mean([r.commits for r in upper_runs])

        low_latency_score = min(1.0, immediate_low_p95_ms / low_p95_ms) if low_p95_ms else 0.0
        medium_latency_score = min(1.0, immediate_medium_p95_ms / medium_p95_ms) if medium_p95_ms else 0.0
        upper_throughput_score = upper_ops_per_sec / best_upper_ops_per_sec if best_upper_ops_per_sec else 0.0
        upper_batch_fullness_score = min(1.0, upper_rows_per_commit / upper_runs[0].batch_max_queued_writes)
        score = (
            args.tune_low_weight * low_latency_score
            + args.tune_medium_weight * medium_latency_score
            + args.tune_upper_throughput_weight * upper_throughput_score
            + args.tune_upper_fullness_weight * upper_batch_fullness_score
        )
        scores.append(
            TunedConfigScore(
                label=label,
                batch_max_queued_writes=upper_runs[0].batch_max_queued_writes,
                flush_delay_ms=upper_runs[0].flush_delay_ms or 0,
                interarrival_delay_ms=upper_runs[0].interarrival_delay_ms,
                score=score,
                low_p95_ms=low_p95_ms,
                low_p95_vs_immediate=low_p95_ms / immediate_low_p95_ms if immediate_low_p95_ms else 0.0,
                medium_p95_ms=medium_p95_ms,
                medium_p95_vs_immediate=medium_p95_ms / immediate_medium_p95_ms if immediate_medium_p95_ms else 0.0,
                upper_ops_per_sec=upper_ops_per_sec,
                upper_speedup=upper_ops_per_sec / immediate_upper_ops_per_sec,
                upper_rows_per_commit=upper_rows_per_commit,
                upper_commits=upper_commits,
                db_kib=_mean([r.db_bytes for r in upper_runs]) / 1024.0,
            )
        )
    return sorted(scores, key=lambda score: score.score, reverse=True), immediate


def _passes_recommendation_constraints(score: TunedConfigScore, args: argparse.Namespace) -> bool:
    if args.require_low_p95_ms is not None and score.low_p95_ms > args.require_low_p95_ms:
        return False
    if args.require_medium_p95_ms is not None and score.medium_p95_ms > args.require_medium_p95_ms:
        return False
    if args.require_upper_speedup is not None and score.upper_speedup < args.require_upper_speedup:
        return False
    if (
        args.require_upper_rows_per_commit is not None
        and score.upper_rows_per_commit < args.require_upper_rows_per_commit
    ):
        return False
    return True


def _balanced_score(score: TunedConfigScore) -> float:
    return (
        (40.0 / max(score.low_p95_vs_immediate, 0.01))
        + (35.0 / max(score.medium_p95_vs_immediate, 0.01))
        + min(score.upper_speedup, 5.0) * 5.0
    )


def _max_throughput_score(score: TunedConfigScore) -> float:
    return (
        score.upper_speedup * 100.0
        + score.upper_rows_per_commit
        + (5.0 / max(score.medium_p95_vs_immediate, 0.01))
        + (2.0 / max(score.low_p95_vs_immediate, 0.01))
    )


def _select_balanced_recommendation(
    ranked: list[TunedConfigScore],
    args: argparse.Namespace,
) -> SelectedRecommendation:
    low_ratio_max = 12.0
    medium_ratio_max = 1.25
    upper_speedup_min = 2.0
    candidates = [
        score
        for score in ranked
        if score.low_p95_vs_immediate <= low_ratio_max
        and score.medium_p95_vs_immediate <= medium_ratio_max
        and score.upper_speedup >= upper_speedup_min
        and _passes_recommendation_constraints(score, args)
    ]
    if candidates:
        return SelectedRecommendation(
            name="batched_balanced",
            score=max(candidates, key=_balanced_score),
            selection_status="strict",
            selection_reason=(
                f"passed low<={low_ratio_max:g}x immediate, "
                f"medium<={medium_ratio_max:g}x immediate, "
                f"upper>={upper_speedup_min:g}x immediate guardrails"
            ),
        )

    return SelectedRecommendation(
        name="batched_balanced",
        score=max(ranked, key=_balanced_score),
        selection_status="fallback",
        selection_reason="no candidate passed balanced guardrails; selected least-bad mixed-workload score",
    )


def _select_max_throughput_recommendation(
    ranked: list[TunedConfigScore],
    args: argparse.Namespace,
) -> SelectedRecommendation:
    low_ratio_max = 50.0
    upper_speedup_min = 1.0
    candidates = [
        score
        for score in ranked
        if score.low_p95_vs_immediate <= low_ratio_max
        and score.upper_speedup >= upper_speedup_min
        and _passes_recommendation_constraints(score, args)
    ]
    if candidates:
        return SelectedRecommendation(
            name="batched_max_throughput",
            score=max(candidates, key=_max_throughput_score),
            selection_status="strict",
            selection_reason=(
                f"maximized upper throughput with low<={low_ratio_max:g}x immediate "
                f"and upper>={upper_speedup_min:g}x immediate sanity guardrails"
            ),
        )

    return SelectedRecommendation(
        name="batched_max_throughput",
        score=max(ranked, key=_max_throughput_score),
        selection_status="fallback",
        selection_reason="no candidate passed throughput guardrails; selected highest throughput-oriented score",
    )


def _confidence_for_recommendation(
    score: TunedConfigScore,
    *,
    passed_constraints: bool,
    rounds: int,
) -> str:
    if not passed_constraints:
        return "low"
    if rounds >= 3 and score.score >= 70.0:
        return "high"
    if rounds >= 2 and score.score >= 60.0:
        return "medium"
    return "low"


def _score_to_json(score: TunedConfigScore) -> dict[str, float | int | None | str]:
    return {
        "label": score.label,
        "batch_max_queued_writes": score.batch_max_queued_writes,
        "batch_max_flush_delay_ms": score.flush_delay_ms,
        "batch_max_interarrival_delay_ms": score.interarrival_delay_ms,
        "score": round(score.score, 4),
        "low_p95_ms": round(score.low_p95_ms, 4),
        "low_p95_vs_immediate": round(score.low_p95_vs_immediate, 4),
        "medium_p95_ms": round(score.medium_p95_ms, 4),
        "medium_p95_vs_immediate": round(score.medium_p95_vs_immediate, 4),
        "upper_ops_per_sec": round(score.upper_ops_per_sec, 4),
        "upper_speedup": round(score.upper_speedup, 4),
        "upper_rows_per_commit": round(score.upper_rows_per_commit, 4),
        "upper_commits": round(score.upper_commits, 4),
        "db_kib": round(score.db_kib, 4),
    }


def _recommendation_to_json(selected: SelectedRecommendation) -> dict[str, object]:
    payload = _score_to_json(selected.score)
    payload["selection_status"] = selected.selection_status
    payload["selection_reason"] = selected.selection_reason
    return payload


def _print_results(results: list[RunResult], *, sweep: bool, tradeoff_profile: bool) -> None:
    if sweep:
        _print_sweep_results(results)
        return
    if tradeoff_profile:
        _print_tradeoff_profile_results(results)
        return

    labels = list(dict.fromkeys(result.label for result in results))
    by_label = {
        label: [result for result in results if result.label == label]
        for label in labels
    }

    sample = results[0]
    print(
        "SQLite state store write benchmark "
        f"({sample.ops} ops/run, concurrency={sample.concurrency}, "
        f"payload={sample.payload_bytes} bytes)"
    )
    print()
    print(
        f"{'mode':<10} {'batch':>6} {'delay':>7} {'ops/s':>10} "
        f"{'inter':>7} {'elapsed ms':>12} {'save p50':>10} {'save p95':>10} "
        f"{'commits':>9} {'rows/commit':>12} {'db KiB':>9}"
    )
    for label in labels:
        runs = by_label[label]
        print(
            f"{label:<10} "
            f"{runs[0].batch_max_queued_writes:>6} "
            f"{runs[0].flush_delay_ms if runs[0].flush_delay_ms is not None else '-':>7} "
            f"{_mean([r.ops_per_sec for r in runs]):>10.0f} "
            f"{runs[0].interarrival_delay_ms if runs[0].interarrival_delay_ms is not None else '-':>7} "
            f"{_mean([r.elapsed_ms for r in runs]):>12.1f} "
            f"{_mean([r.save_p50_ms for r in runs]):>10.2f} "
            f"{_mean([r.save_p95_ms for r in runs]):>10.2f} "
            f"{_mean([r.commits for r in runs]):>9.1f} "
            f"{_mean([r.rows_per_commit for r in runs]):>12.1f} "
            f"{_mean([r.db_bytes for r in runs]) / 1024.0:>9.1f}"
        )

    comparison_labels = ("autotuned", "immediate") if "autotuned" in by_label else ("batched", "immediate")
    batched_label, immediate_label = comparison_labels
    if {batched_label, immediate_label}.issubset(by_label):
        batched_rps = _mean([r.ops_per_sec for r in by_label[batched_label]])
        immediate_rps = _mean([r.ops_per_sec for r in by_label["immediate"]])
        batched_elapsed = _mean([r.elapsed_ms for r in by_label[batched_label]])
        immediate_elapsed = _mean([r.elapsed_ms for r in by_label["immediate"]])
        print()
        print(f"throughput speedup: {batched_rps / immediate_rps:.2f}x")
        print(f"elapsed-time reduction: {(1.0 - batched_elapsed / immediate_elapsed) * 100.0:.1f}%")


def _print_tradeoff_profile_results(results: list[RunResult]) -> None:
    print(
        "SQLite state store write tradeoff profile "
        f"(payload={results[0].payload_bytes} bytes)"
    )
    print()
    print(
        f"{'profile':<8} {'mode':<10} {'batch':>6} {'delay':>7} {'ops':>6} "
        f"{'conc':>5} {'inter':>7} {'ops/s':>10} {'elapsed ms':>12} {'save p50':>10} "
        f"{'save p95':>10} {'commits':>9} {'rows/commit':>12} {'db KiB':>9}"
    )
    keys = list(dict.fromkeys(result.label for result in results))
    by_label = {
        label: [result for result in results if result.label == label]
        for label in keys
    }
    for label, runs in by_label.items():
        profile, mode = label.split(":", 1)
        print(
            f"{profile:<8} "
            f"{mode:<10} "
            f"{runs[0].batch_max_queued_writes:>6} "
            f"{runs[0].flush_delay_ms if runs[0].flush_delay_ms is not None else '-':>7} "
            f"{runs[0].ops:>6} "
            f"{runs[0].concurrency:>5} "
            f"{runs[0].interarrival_delay_ms if runs[0].interarrival_delay_ms is not None else '-':>7} "
            f"{_mean([r.ops_per_sec for r in runs]):>10.0f} "
            f"{_mean([r.elapsed_ms for r in runs]):>12.1f} "
            f"{_mean([r.save_p50_ms for r in runs]):>10.2f} "
            f"{_mean([r.save_p95_ms for r in runs]):>10.2f} "
            f"{_mean([r.commits for r in runs]):>9.1f} "
            f"{_mean([r.rows_per_commit for r in runs]):>12.1f} "
            f"{_mean([r.db_bytes for r in runs]) / 1024.0:>9.1f}"
        )


def _print_tuning_results(results: list[RunResult], *, args: argparse.Namespace, top_n: int) -> None:
    profiles = ("low", "medium", "upper")
    ranked, immediate = _score_tuned_configs(results, args)
    sample = results[0]
    print(
        "SQLite state store manual tuning sweep "
        f"(payload={sample.payload_bytes} bytes, rounds={len(immediate['low'])})"
    )
    print()
    print(
        f"score = {args.tune_low_weight:g} low p95 closeness to immediate + "
        f"{args.tune_medium_weight:g} medium p95 closeness to immediate + "
        f"{args.tune_upper_throughput_weight:g} upper throughput vs best candidate + "
        f"{args.tune_upper_fullness_weight:g} upper batch fullness"
    )
    print()
    print("Immediate baselines:")
    print(
        f"{'profile':<8} {'ops':>6} {'conc':>5} {'ops/s':>10} "
        f"{'p50':>8} {'p95':>8} {'commits':>9}"
    )
    for profile in profiles:
        runs = immediate[profile]
        print(
            f"{profile:<8} "
            f"{runs[0].ops:>6} "
            f"{runs[0].concurrency:>5} "
            f"{_mean([r.ops_per_sec for r in runs]):>10.0f} "
            f"{_mean([r.save_p50_ms for r in runs]):>8.2f} "
            f"{_mean([r.save_p95_ms for r in runs]):>8.2f} "
            f"{_mean([r.commits for r in runs]):>9.1f}"
        )

    print()
    print(
        f"{'rank':>4} {'score':>7} {'batch':>6} {'flush':>6} {'inter':>7} "
        f"{'low p95':>9} {'low/imm':>8} {'med p95':>9} {'med/imm':>8} {'upper ops/s':>12} "
        f"{'upper x':>8} {'upper commits':>13} {'rows/commit':>12} {'db KiB':>8}"
    )
    for rank, score in enumerate(ranked[:top_n], start=1):
        print(
            f"{rank:>4} "
            f"{score.score:>7.2f} "
            f"{score.batch_max_queued_writes:>6} "
            f"{score.flush_delay_ms:>6} "
            f"{score.interarrival_delay_ms if score.interarrival_delay_ms is not None else '-':>7} "
            f"{score.low_p95_ms:>9.2f} "
            f"{score.low_p95_vs_immediate:>8.2f} "
            f"{score.medium_p95_ms:>9.2f} "
            f"{score.medium_p95_vs_immediate:>8.2f} "
            f"{score.upper_ops_per_sec:>12.0f} "
            f"{score.upper_speedup:>8.2f} "
            f"{score.upper_commits:>13.1f} "
            f"{score.upper_rows_per_commit:>12.1f} "
            f"{score.db_kib:>8.1f}"
        )


def _print_recommendation(results: list[RunResult], *, args: argparse.Namespace) -> None:
    ranked, immediate = _score_tuned_configs(results, args)
    balanced = _select_balanced_recommendation(ranked, args)
    max_throughput = _select_max_throughput_recommendation(ranked, args)
    balanced_confidence = _confidence_for_recommendation(
        balanced.score,
        passed_constraints=balanced.selection_status == "strict",
        rounds=args.rounds,
    )
    max_throughput_confidence = _confidence_for_recommendation(
        max_throughput.score,
        passed_constraints=max_throughput.selection_status == "strict",
        rounds=args.rounds,
    )
    immediate_baseline = {
        profile: {
            "ops": runs[0].ops,
            "concurrency": runs[0].concurrency,
            "ops_per_sec": round(_mean([r.ops_per_sec for r in runs]), 4),
            "p50_ms": round(_mean([r.save_p50_ms for r in runs]), 4),
            "p95_ms": round(_mean([r.save_p95_ms for r in runs]), 4),
            "commits": round(_mean([r.commits for r in runs]), 4),
        }
        for profile, runs in immediate.items()
    }

    if args.recommend_output == "json":
        payload = {
            "recommendation": _recommendation_to_json(balanced),
            "confidence": balanced_confidence,
            "constraints_passed": balanced.selection_status == "strict",
            "recommended_batched_balanced": {
                **_recommendation_to_json(balanced),
                "confidence": balanced_confidence,
            },
            "recommended_batched_max_throughput": {
                **_recommendation_to_json(max_throughput),
                "confidence": max_throughput_confidence,
            },
            "rounds": args.rounds,
            "weights": {
                "low": args.tune_low_weight,
                "medium": args.tune_medium_weight,
                "upper_throughput": args.tune_upper_throughput_weight,
                "upper_fullness": args.tune_upper_fullness_weight,
            },
            "constraints": {
                "require_low_p95_ms": args.require_low_p95_ms,
                "require_medium_p95_ms": args.require_medium_p95_ms,
                "require_upper_speedup": args.require_upper_speedup,
                "require_upper_rows_per_commit": args.require_upper_rows_per_commit,
            },
            "immediate_baseline": immediate_baseline,
            "immediate_baselines": immediate_baseline,
            "top_candidates": [_score_to_json(score) for score in ranked[:args.tune_top_n]],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    print("Immediate baselines:")
    print(
        f"{'profile':<8} {'ops':>6} {'conc':>5} {'ops/s':>10} "
        f"{'p50':>8} {'p95':>8} {'commits':>9}"
    )
    for profile in ("low", "medium", "upper"):
        runs = immediate[profile]
        print(
            f"{profile:<8} "
            f"{runs[0].ops:>6} "
            f"{runs[0].concurrency:>5} "
            f"{_mean([r.ops_per_sec for r in runs]):>10.0f} "
            f"{_mean([r.save_p50_ms for r in runs]):>8.2f} "
            f"{_mean([r.save_p95_ms for r in runs]):>8.2f} "
            f"{_mean([r.commits for r in runs]):>9.1f}"
        )
    print()
    for title, selected, confidence in (
        ("Recommended batched_balanced config:", balanced, balanced_confidence),
        ("Recommended batched_max_throughput config:", max_throughput, max_throughput_confidence),
    ):
        score = selected.score
        print(title)
        print(f"  batch_max_queued_writes={score.batch_max_queued_writes}")
        print(f"  batch_max_flush_delay_ms={score.flush_delay_ms}")
        print(f"  batch_max_interarrival_delay_ms={score.interarrival_delay_ms}")
        print(f"  confidence={confidence}")
        print(f"  selection_status={selected.selection_status}")
        print(f"  selection_reason={selected.selection_reason}")
        print(f"  score={score.score:.2f}")
        print(f"  low p95={score.low_p95_ms:.2f} ms ({score.low_p95_vs_immediate:.2f}x immediate)")
        print(f"  medium p95={score.medium_p95_ms:.2f} ms ({score.medium_p95_vs_immediate:.2f}x immediate)")
        print(f"  upper throughput={score.upper_ops_per_sec:.0f} ops/s ({score.upper_speedup:.2f}x immediate)")
        print(f"  upper commits={score.upper_commits:.1f}")
        print(f"  upper rows/commit={score.upper_rows_per_commit:.1f}")
        print()
    print("Top candidates:")
    print(
        f"{'rank':>4} {'pick':<11} {'score':>7} {'batch':>6} {'flush':>6} {'inter':>7} "
        f"{'low p95':>9} {'low/imm':>8} {'med p95':>9} {'med/imm':>8} {'upper ops/s':>12} "
        f"{'upper x':>8} {'rows/commit':>12}"
    )
    display_scores = list(ranked[:args.tune_top_n])
    displayed_labels = {score.label for score in display_scores}
    for selected in (balanced, max_throughput):
        if selected.score.label not in displayed_labels:
            display_scores.append(selected.score)
            displayed_labels.add(selected.score.label)
    rank_by_label = {score.label: rank for rank, score in enumerate(ranked, start=1)}
    for score in display_scores:
        picks: list[str] = []
        if score.label == balanced.score.label:
            picks.append("balanced")
        if score.label == max_throughput.score.label:
            picks.append("throughput")
        pick_label = ",".join(picks) if picks else "-"
        print(
            f"{rank_by_label[score.label]:>4} "
            f"{pick_label:<11} "
            f"{score.score:>7.2f} "
            f"{score.batch_max_queued_writes:>6} "
            f"{score.flush_delay_ms:>6} "
            f"{score.interarrival_delay_ms if score.interarrival_delay_ms is not None else '-':>7} "
            f"{score.low_p95_ms:>9.2f} "
            f"{score.low_p95_vs_immediate:>8.2f} "
            f"{score.medium_p95_ms:>9.2f} "
            f"{score.medium_p95_vs_immediate:>8.2f} "
            f"{score.upper_ops_per_sec:>12.0f} "
            f"{score.upper_speedup:>8.2f} "
            f"{score.upper_rows_per_commit:>12.1f}"
        )


def _print_sweep_results(results: list[RunResult]) -> None:
    keys = sorted({
        (
            result.concurrency,
            result.label,
            result.batch_max_queued_writes,
            result.flush_delay_ms,
        )
        for result in results
    })
    print(
        "SQLite state store flush-delay sweep "
        f"(payload={results[0].payload_bytes} bytes)"
    )
    print()
    print(
        f"{'conc':>5} {'mode':<10} {'batch':>6} {'delay':>7} {'ops':>6} "
        f"{'inter':>7} {'ops/s':>10} {'elapsed ms':>12} {'save p50':>10} "
        f"{'save p95':>10} {'commits':>9} {'rows/commit':>12}"
    )
    rows_by_key = {
        key: [
            result for result in results
            if (
                result.concurrency,
                result.label,
                result.batch_max_queued_writes,
                result.flush_delay_ms,
            ) == key
        ]
        for key in keys
    }
    for key in keys:
        concurrency, label, _batch_max_queued_writes, flush_delay_ms = key
        runs = rows_by_key[key]
        print(
            f"{concurrency:>5} "
            f"{label:<10} "
            f"{runs[0].batch_max_queued_writes:>6} "
            f"{flush_delay_ms if flush_delay_ms is not None else '-':>7} "
            f"{runs[0].ops:>6} "
            f"{runs[0].interarrival_delay_ms if runs[0].interarrival_delay_ms is not None else '-':>7} "
            f"{_mean([r.ops_per_sec for r in runs]):>10.0f} "
            f"{_mean([r.elapsed_ms for r in runs]):>12.1f} "
            f"{_mean([r.save_p50_ms for r in runs]):>10.2f} "
            f"{_mean([r.save_p95_ms for r in runs]):>10.2f} "
            f"{_mean([r.commits for r in runs]):>9.1f} "
            f"{_mean([r.rows_per_commit for r in runs]):>12.1f}"
        )


def _parse_args(
    argv: list[str] | None = None,
    *,
    prog: str = "minions tune sqlite",
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Compare one-row SQLiteStateStore transactions "
            "(batch_max_queued_writes=1) against batched transactions."
        ),
    )
    parser.add_argument("--ops", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--warmup-ops", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--flush-delay-ms", type=int, default=None)
    parser.add_argument(
        "--batched-tuning",
        choices=("manual", "calibrated"),
        default="manual",
        help="Use manual batch settings or calibrated startup autotuning for the non-immediate mode.",
    )
    parser.add_argument("--batched-batch-max-queued-writes", type=int, default=64)
    parser.add_argument("--batch-max-interarrival-delay-ms", type=int, default=None)
    parser.add_argument(
        "--compare-interarrival",
        action="store_true",
        help="In tradeoff profile mode, compare batched/autotuned without and with interarrival delay.",
    )
    parser.add_argument(
        "--tradeoff-profile",
        action="store_true",
        help="Run bounded low/upper workload points to show the latency/throughput tradeoff curve.",
    )
    parser.add_argument(
        "--tune-curve",
        action="store_true",
        help="Run a manual low/medium/upper sweep and rank candidate batch settings.",
    )
    parser.add_argument(
        "--recommend-config",
        action="store_true",
        help="Run the manual tuner and print recommended SQLiteStateStore batch settings.",
    )
    parser.add_argument(
        "--recommend-output",
        choices=("text", "json"),
        default="text",
    )
    parser.add_argument("--profile-low-ops", type=int, default=64)
    parser.add_argument("--profile-low-concurrency", type=int, default=1)
    parser.add_argument("--profile-medium-ops", type=int, default=128)
    parser.add_argument("--profile-medium-concurrency", type=int, default=16)
    parser.add_argument("--profile-upper-ops", type=int, default=1024)
    parser.add_argument("--profile-upper-concurrency", type=int, default=256)
    parser.add_argument("--tune-batch-max-queued-writes", default="32,48,64,128")
    parser.add_argument("--tune-flush-delay-ms", default="5,8,16")
    parser.add_argument("--tune-interarrival-delay-ms", default="none,1,2,5")
    parser.add_argument("--tune-top-n", type=int, default=10)
    parser.add_argument("--tune-low-weight", type=float, default=25.0)
    parser.add_argument("--tune-medium-weight", type=float, default=20.0)
    parser.add_argument("--tune-upper-throughput-weight", type=float, default=40.0)
    parser.add_argument("--tune-upper-fullness-weight", type=float, default=15.0)
    parser.add_argument("--require-low-p95-ms", type=float, default=None)
    parser.add_argument("--require-medium-p95-ms", type=float, default=None)
    parser.add_argument("--require-upper-speedup", type=float, default=None)
    parser.add_argument("--require-upper-rows-per-commit", type=float, default=None)
    parser.add_argument("--sweep-flush-delay-ms", default=None)
    parser.add_argument("--sweep-batch-max-queued-writes", default=None)
    parser.add_argument("--sweep-concurrency", default="1,8,64")
    parser.add_argument(
        "--waves",
        type=int,
        default=4,
        help="Minimum measured waves per concurrency level in sweep mode.",
    )
    return parser.parse_args(argv)


def main(
    argv: list[str] | None = None,
    *,
    prog: str = "minions tune sqlite",
) -> int:
    args = _parse_args(argv, prog=prog)
    if args.tradeoff_profile and args.sweep_flush_delay_ms:
        raise SystemExit("--tradeoff-profile cannot be combined with --sweep-flush-delay-ms")
    if args.tune_curve and (args.tradeoff_profile or args.sweep_flush_delay_ms or args.recommend_config):
        raise SystemExit(
            "--tune-curve cannot be combined with --tradeoff-profile, "
            "--sweep-flush-delay-ms, or --recommend-config"
        )
    if args.recommend_config and (args.tradeoff_profile or args.sweep_flush_delay_ms):
        raise SystemExit("--recommend-config cannot be combined with --tradeoff-profile or --sweep-flush-delay-ms")
    if args.sweep_flush_delay_ms and args.batched_tuning != "manual":
        raise SystemExit("--sweep-flush-delay-ms only supports --batched-tuning manual")

    if args.recommend_config:
        results = asyncio.run(_run_tuning_sweep(args))
        _print_recommendation(results, args=args)
        return
    if args.tune_curve:
        results = asyncio.run(_run_tuning_sweep(args))
        _print_tuning_results(results, args=args, top_n=args.tune_top_n)
        return
    if args.tradeoff_profile:
        results = asyncio.run(_run_tradeoff_profile(args))
        sweep = False
    elif args.sweep_flush_delay_ms:
        results = asyncio.run(_run_delay_sweep(args))
        sweep = True
    else:
        results = asyncio.run(_run_benchmark(args))
        sweep = False
    _print_results(results, sweep=sweep, tradeoff_profile=args.tradeoff_profile)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
