"""
Estimate metric cardinality from a Minions metrics snapshot or Prometheus text.

This is a developer diagnostic, not a correctness test. It helps identify which
metrics and labels are likely to create the most Prometheus series under a
representative workload.

Examples:
    python scripts/metrics_cardinality_audit.py snapshot.json
    python scripts/metrics_cardinality_audit.py --format prometheus metrics.txt
    curl -s http://localhost:8081/metrics | python scripts/metrics_cardinality_audit.py --format prometheus -
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal


MetricKind = Literal["counter", "gauge", "histogram", "unknown"]
LabelSet = tuple[tuple[str, str], ...]

PROMETHEUS_SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+[-+0-9.eE]+(?:\s+\d+)?$"
)
PROMETHEUS_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


@dataclass
class MetricAudit:
    metric_name: str
    kind: MetricKind
    label_sets: set[LabelSet]
    observed_series: int | None = None

    @property
    def label_set_count(self) -> int:
        return len(self.label_sets)

    def estimated_series_count(self, histogram_series_per_labelset: int) -> int:
        if self.observed_series is not None:
            return self.observed_series
        if self.kind == "histogram":
            return self.label_set_count * histogram_series_per_labelset
        return self.label_set_count

    def label_value_counts(self) -> dict[str, int]:
        values: dict[str, set[str]] = defaultdict(set)
        for label_set in self.label_sets:
            for name, value in label_set:
                values[name].add(value)
        return {name: len(label_values) for name, label_values in values.items()}


def read_input(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8")


def normalize_label_set(labels: dict[str, Any]) -> LabelSet:
    return tuple(sorted((str(name), str(value)) for name, value in labels.items()))


def infer_json_metric_kind(section_name: str) -> MetricKind:
    normalized = section_name.lower()
    if normalized in {"counter", "counters"}:
        return "counter"
    if normalized in {"gauge", "gauges"}:
        return "gauge"
    if normalized in {"histogram", "histograms"}:
        return "histogram"
    return "unknown"


def iter_json_sections(payload: dict[str, Any]) -> Iterable[tuple[MetricKind, dict[str, Any]]]:
    for section_name, section in payload.items():
        kind = infer_json_metric_kind(section_name)
        if kind == "unknown" or not isinstance(section, dict):
            continue
        yield kind, section


def parse_json_snapshot(text: str) -> list[MetricAudit]:
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("JSON snapshot must be an object")

    audits: dict[tuple[str, MetricKind], MetricAudit] = {}
    for kind, section in iter_json_sections(payload):
        for metric_name, samples in section.items():
            if not isinstance(samples, list):
                continue
            audit = audits.setdefault(
                (metric_name, kind),
                MetricAudit(metric_name=metric_name, kind=kind, label_sets=set()),
            )
            for sample in samples:
                if not isinstance(sample, dict):
                    continue
                labels = sample.get("labels", {})
                if isinstance(labels, dict):
                    audit.label_sets.add(normalize_label_set(labels))

    return list(audits.values())


def prometheus_base_name(sample_name: str) -> tuple[str, MetricKind, bool]:
    if sample_name.endswith("_bucket"):
        return sample_name.removesuffix("_bucket"), "histogram", True
    if sample_name.endswith("_sum"):
        return sample_name.removesuffix("_sum"), "histogram", True
    if sample_name.endswith("_count"):
        return sample_name.removesuffix("_count"), "histogram", True
    if sample_name.endswith("_created"):
        return sample_name.removesuffix("_created"), "unknown", True
    return sample_name, "unknown", False


def parse_prometheus_labels(raw_labels: str | None) -> dict[str, str]:
    if not raw_labels:
        return {}
    labels: dict[str, str] = {}
    for match in PROMETHEUS_LABEL_RE.finditer(raw_labels):
        labels[match.group(1)] = bytes(match.group(2), "utf-8").decode("unicode_escape")
    return labels


def parse_prometheus_text(text: str) -> list[MetricAudit]:
    audits: dict[str, MetricAudit] = {}
    observed_series: dict[str, int] = defaultdict(int)

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = PROMETHEUS_SAMPLE_RE.match(line)
        if not match:
            continue

        sample_name = match.group("name")
        metric_name, inferred_kind, is_prometheus_child = prometheus_base_name(sample_name)
        labels = parse_prometheus_labels(match.group("labels"))
        labels.pop("le", None)

        audit = audits.get(metric_name)
        if audit is None:
            audit = MetricAudit(
                metric_name=metric_name,
                kind=inferred_kind,
                label_sets=set(),
                observed_series=0,
            )
            audits[metric_name] = audit
        elif audit.kind == "unknown" and inferred_kind != "unknown":
            audit.kind = inferred_kind

        audit.label_sets.add(normalize_label_set(labels))
        if not sample_name.endswith("_created"):
            observed_series[metric_name] += 1
        elif not is_prometheus_child:
            observed_series[metric_name] += 1

    for metric_name, count in observed_series.items():
        audits[metric_name].observed_series = count

    return list(audits.values())


def autodetect_format(text: str) -> Literal["json", "prometheus"]:
    stripped = text.lstrip()
    if stripped.startswith("{"):
        return "json"
    return "prometheus"


def sort_audits(
    audits: list[MetricAudit],
    histogram_series_per_labelset: int,
) -> list[MetricAudit]:
    return sorted(
        audits,
        key=lambda audit: (
            audit.estimated_series_count(histogram_series_per_labelset),
            audit.label_set_count,
            audit.metric_name,
        ),
        reverse=True,
    )


def format_label_counts(label_counts: dict[str, int], limit: int) -> str:
    if not label_counts:
        return "-"
    ranked = sorted(label_counts.items(), key=lambda item: (item[1], item[0]), reverse=True)
    rendered = ", ".join(f"{name}={count}" for name, count in ranked[:limit])
    if len(ranked) > limit:
        rendered += f", ... +{len(ranked) - limit}"
    return rendered


def print_table(
    audits: list[MetricAudit],
    *,
    histogram_series_per_labelset: int,
    limit: int,
    label_limit: int,
    warn_series: int | None,
) -> int:
    sorted_audits = sort_audits(audits, histogram_series_per_labelset)
    shown = sorted_audits[:limit]
    max_name_len = max([len("metric"), *(len(audit.metric_name) for audit in shown)], default=6)

    print(
        f"{'metric':<{max_name_len}}  kind       labelsets  est_series  top_label_values"
    )
    print(
        f"{'-' * max_name_len}  {'-' * 9}  {'-' * 9}  {'-' * 10}  {'-' * 16}"
    )

    exit_code = 0
    for audit in shown:
        estimated_series = audit.estimated_series_count(histogram_series_per_labelset)
        if warn_series is not None and estimated_series >= warn_series:
            exit_code = 1
        marker = " !" if warn_series is not None and estimated_series >= warn_series else "  "
        print(
            f"{audit.metric_name:<{max_name_len}}  "
            f"{audit.kind:<9}  "
            f"{audit.label_set_count:>9}  "
            f"{estimated_series:>10}{marker}  "
            f"{format_label_counts(audit.label_value_counts(), label_limit)}"
        )

    total_estimated = sum(
        audit.estimated_series_count(histogram_series_per_labelset)
        for audit in audits
    )
    print()
    print(f"metrics: {len(audits)}")
    print(f"estimated_series_total: {total_estimated}")
    if warn_series is not None:
        print(f"warn_series_threshold: {warn_series}")
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Estimate metric cardinality from JSON snapshots or Prometheus text exposition."
    )
    parser.add_argument(
        "path",
        help="Input file path, or '-' for stdin.",
    )
    parser.add_argument(
        "--format",
        choices=("auto", "json", "prometheus"),
        default="auto",
        help="Input format. Default: auto.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of metrics to print. Default: 25.",
    )
    parser.add_argument(
        "--label-limit",
        type=int,
        default=6,
        help="Maximum top label cardinalities to print per metric. Default: 6.",
    )
    parser.add_argument(
        "--histogram-series-per-labelset",
        type=int,
        default=17,
        help=(
            "Series multiplier for histogram samples in JSON snapshots. "
            "Default 17 approximates prometheus_client default buckets plus _sum/_count."
        ),
    )
    parser.add_argument(
        "--warn-series",
        type=int,
        default=None,
        help="Exit non-zero if any metric has at least this many estimated series.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    text = read_input(args.path)
    input_format = autodetect_format(text) if args.format == "auto" else args.format

    if input_format == "json":
        audits = parse_json_snapshot(text)
    else:
        audits = parse_prometheus_text(text)

    return print_table(
        audits,
        histogram_series_per_labelset=args.histogram_series_per_labelset,
        limit=args.limit,
        label_limit=args.label_limit,
        warn_series=args.warn_series,
    )


if __name__ == "__main__":
    raise SystemExit(main())
