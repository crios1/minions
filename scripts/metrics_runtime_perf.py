"""
Benchmark Minions metrics emission overhead.

This developer tool exercises the framework metrics API directly so metrics
cost can be measured separately from Gru orchestration cost.

Examples:
    python scripts/metrics_runtime_perf.py
    python scripts/metrics_runtime_perf.py --backend prometheus --scenario persistence-mixed --iterations 100000
    python scripts/metrics_runtime_perf.py --backend all --scenario all --cardinality 1000
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from prometheus_client import CollectorRegistry, generate_latest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_MINION_COMPOSITE_KEY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_STATE_STORE,
    LABEL_STATUS,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
)
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.metrics_prometheus import PrometheusMetrics


BackendName = Literal["noop", "inmemory", "prometheus"]
ModeName = Literal["async", "unsafe"]
ScenarioName = Literal[
    "counter-low",
    "counter-many",
    "histogram-low",
    "histogram-many",
    "persistence-mixed",
]
MetricKind = Literal["counter", "gauge", "histogram"]


@dataclass(frozen=True)
class Operation:
    kind: MetricKind
    metric_name: str
    labels: dict[str, str]
    value: float = 1.0


@dataclass(frozen=True)
class BenchmarkResult:
    backend: BackendName
    scenario: ScenarioName
    mode: ModeName
    iterations: int
    operations: int
    cardinality: int
    elapsed_s: float
    ops_per_s: float
    label_sets: int
    estimated_prometheus_series: int
    scrape_bytes: int | None
    peak_tracemalloc_bytes: int


@dataclass(frozen=True)
class BackendRuntime:
    metrics: Metrics
    registry: CollectorRegistry | None = None


def make_backend(name: BackendName) -> BackendRuntime:
    if name == "noop":
        return BackendRuntime(metrics=NoOpMetrics())
    if name == "prometheus":
        registry = CollectorRegistry()
        return BackendRuntime(
            metrics=PrometheusMetrics(logger=NoOpLogger(), registry=registry),
            registry=registry,
        )
    if name == "inmemory":
        from tests.assets.support.metrics_inmemory import InMemoryMetrics

        return BackendRuntime(metrics=InMemoryMetrics())
    raise ValueError(f"Unknown backend: {name}")


def label_value(prefix: str, index: int, cardinality: int) -> str:
    return f"{prefix}-{index % cardinality}"


def counter_low(iteration: int, cardinality: int) -> Iterable[Operation]:
    del iteration, cardinality
    yield Operation(
        kind="counter",
        metric_name=MINION_WORKFLOW_STARTED_TOTAL,
        labels={LABEL_MINION_COMPOSITE_KEY: "orchestration-0"},
    )


def counter_many(iteration: int, cardinality: int) -> Iterable[Operation]:
    yield Operation(
        kind="counter",
        metric_name=MINION_WORKFLOW_STARTED_TOTAL,
        labels={LABEL_MINION_COMPOSITE_KEY: label_value("orchestration", iteration, cardinality)},
    )


def histogram_low(iteration: int, cardinality: int) -> Iterable[Operation]:
    del cardinality
    yield Operation(
        kind="histogram",
        metric_name=MINION_WORKFLOW_DURATION_SECONDS,
        labels={
            LABEL_MINION_COMPOSITE_KEY: "orchestration-0",
            LABEL_STATUS: "succeeded",
        },
        value=(iteration % 100) / 1000.0,
    )


def histogram_many(iteration: int, cardinality: int) -> Iterable[Operation]:
    yield Operation(
        kind="histogram",
        metric_name=MINION_WORKFLOW_DURATION_SECONDS,
        labels={
            LABEL_MINION_COMPOSITE_KEY: label_value("orchestration", iteration, cardinality),
            LABEL_STATUS: "succeeded",
        },
        value=(iteration % 100) / 1000.0,
    )


def persistence_mixed(iteration: int, cardinality: int) -> Iterable[Operation]:
    orchestration_id = label_value("orchestration", iteration, cardinality)
    checkpoint_type = "before_step" if iteration % 5 else "workflow_start"
    policy = "idle-until-persisted" if iteration % 2 else "continue-on-failure"
    state_store = "SQLiteStateStore" if iteration % 3 else "InMemoryStateStore"
    base_labels = {
        LABEL_MINION_COMPOSITE_KEY: orchestration_id,
        LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: checkpoint_type,
        LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: policy,
        LABEL_STATE_STORE: state_store,
    }
    yield Operation(
        kind="counter",
        metric_name=MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
        labels=base_labels,
    )
    yield Operation(
        kind="histogram",
        metric_name=MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
        labels=base_labels,
        value=(iteration % 50) / 10000.0,
    )
    if iteration % 10:
        yield Operation(
            kind="counter",
            metric_name=MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
            labels=base_labels,
        )
    else:
        yield Operation(
            kind="counter",
            metric_name=MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
            labels={
                **base_labels,
                LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
                LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: "true",
            },
        )
        yield Operation(
            kind="counter",
            metric_name=MINION_WORKFLOW_FAILED_TOTAL,
            labels={
                LABEL_MINION_COMPOSITE_KEY: orchestration_id,
                LABEL_ERROR_TYPE: "WorkflowPersistenceNonRetryableError",
            },
        )


SCENARIOS: dict[ScenarioName, Callable[[int, int], Iterable[Operation]]] = {
    "counter-low": counter_low,
    "counter-many": counter_many,
    "histogram-low": histogram_low,
    "histogram-many": histogram_many,
    "persistence-mixed": persistence_mixed,
}


def emit_unsafe(metrics: Metrics, operation: Operation) -> None:
    if operation.kind == "counter":
        metrics._inc_unsafe(operation.metric_name, amount=operation.value, labels=operation.labels)
    elif operation.kind == "gauge":
        metrics._set_unsafe(operation.metric_name, value=operation.value, labels=operation.labels)
    elif operation.kind == "histogram":
        metrics._observe_unsafe(operation.metric_name, value=operation.value, labels=operation.labels)
    else:
        raise ValueError(f"Unknown operation kind: {operation.kind}")


async def emit_async(metrics: Metrics, operation: Operation) -> None:
    if operation.kind == "counter":
        await metrics._inc(operation.metric_name, amount=operation.value, labels=operation.labels)
    elif operation.kind == "gauge":
        await metrics._set(operation.metric_name, value=operation.value, labels=operation.labels)
    elif operation.kind == "histogram":
        await metrics._observe(operation.metric_name, value=operation.value, labels=operation.labels)
    else:
        raise ValueError(f"Unknown operation kind: {operation.kind}")


def estimate_series(label_sets_by_metric: dict[tuple[str, MetricKind], set[tuple[tuple[str, str], ...]]]) -> int:
    total = 0
    for (_metric_name, kind), label_sets in label_sets_by_metric.items():
        multiplier = 17 if kind == "histogram" else 1
        total += len(label_sets) * multiplier
    return total


def record_label_set(
    label_sets_by_metric: dict[tuple[str, MetricKind], set[tuple[tuple[str, str], ...]]],
    operation: Operation,
) -> None:
    key = (operation.metric_name, operation.kind)
    label_sets_by_metric.setdefault(key, set()).add(tuple(sorted(operation.labels.items())))


async def run_benchmark(
    *,
    backend_name: BackendName,
    scenario_name: ScenarioName,
    mode: ModeName,
    iterations: int,
    cardinality: int,
) -> BenchmarkResult:
    backend_runtime = make_backend(backend_name)
    metrics = backend_runtime.metrics
    scenario = SCENARIOS[scenario_name]
    label_sets_by_metric: dict[tuple[str, MetricKind], set[tuple[tuple[str, str], ...]]] = {}

    gc.collect()
    tracemalloc.start()
    started_at = time.perf_counter()
    operations = 0
    if mode == "unsafe":
        for iteration in range(iterations):
            for operation in scenario(iteration, cardinality):
                emit_unsafe(metrics, operation)
                record_label_set(label_sets_by_metric, operation)
                operations += 1
    else:
        for iteration in range(iterations):
            for operation in scenario(iteration, cardinality):
                await emit_async(metrics, operation)
                record_label_set(label_sets_by_metric, operation)
                operations += 1
    elapsed_s = time.perf_counter() - started_at
    _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    scrape_bytes = (
        len(generate_latest(backend_runtime.registry))
        if backend_runtime.registry is not None else
        None
    )

    return BenchmarkResult(
        backend=backend_name,
        scenario=scenario_name,
        mode=mode,
        iterations=iterations,
        operations=operations,
        cardinality=cardinality,
        elapsed_s=elapsed_s,
        ops_per_s=(operations / elapsed_s) if elapsed_s else 0.0,
        label_sets=sum(len(label_sets) for label_sets in label_sets_by_metric.values()),
        estimated_prometheus_series=estimate_series(label_sets_by_metric),
        scrape_bytes=scrape_bytes,
        peak_tracemalloc_bytes=peak_bytes,
    )


def expand_choice(value: str, all_values: list[str]) -> list[str]:
    return all_values if value == "all" else [value]


def print_results(results: list[BenchmarkResult]) -> None:
    headers = (
        "backend",
        "scenario",
        "mode",
        "iters",
        "ops",
        "card",
        "seconds",
        "ops/s",
        "labelsets",
        "est_series",
        "scrape_kib",
        "peak_kib",
    )
    rows = [
        (
            result.backend,
            result.scenario,
            result.mode,
            str(result.iterations),
            str(result.operations),
            str(result.cardinality),
            f"{result.elapsed_s:.4f}",
            f"{result.ops_per_s:.0f}",
            str(result.label_sets),
            str(result.estimated_prometheus_series),
            "-" if result.scrape_bytes is None else f"{result.scrape_bytes / 1024:.1f}",
            f"{result.peak_tracemalloc_bytes / 1024:.1f}",
        )
        for result in results
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(value.rjust(widths[index]) for index, value in enumerate(row)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark Minions metrics emission overhead.")
    parser.add_argument(
        "--backend",
        choices=("all", "noop", "inmemory", "prometheus"),
        default="all",
        help="Metrics backend to benchmark. Default: all.",
    )
    parser.add_argument(
        "--scenario",
        choices=("all", *SCENARIOS.keys()),
        default="all",
        help="Emission scenario to benchmark. Default: all.",
    )
    parser.add_argument(
        "--mode",
        choices=("all", "async", "unsafe"),
        default="async",
        help="Metrics API path to benchmark. Default: async.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=50_000,
        help="Iterations per backend/scenario/mode. Default: 50000.",
    )
    parser.add_argument(
        "--cardinality",
        type=int,
        default=100,
        help="Distinct orchestration ids for many-label scenarios. Default: 100.",
    )
    return parser


async def async_main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.iterations <= 0:
        parser.error("--iterations must be positive")
    if args.cardinality <= 0:
        parser.error("--cardinality must be positive")

    backends = expand_choice(args.backend, ["noop", "inmemory", "prometheus"])
    scenarios = expand_choice(args.scenario, list(SCENARIOS))
    modes = expand_choice(args.mode, ["async", "unsafe"])

    results: list[BenchmarkResult] = []
    for backend in backends:
        for scenario in scenarios:
            for mode in modes:
                results.append(
                    await run_benchmark(
                        backend_name=backend,  # type: ignore[arg-type]
                        scenario_name=scenario,  # type: ignore[arg-type]
                        mode=mode,  # type: ignore[arg-type]
                        iterations=args.iterations,
                        cardinality=args.cardinality,
                    )
                )

    print_results(results)
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
