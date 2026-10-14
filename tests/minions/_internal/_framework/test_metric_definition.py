import pytest

from minions._internal._framework.metric_definition import MetricDefinition


def test_represents_label_names_as_sorted_tuple():
    label_names = {"status", "route"}
    definition = MetricDefinition(
        name="label_order",
        kind="gauge",
        label_names=label_names,
    )

    assert definition.label_names == ("route", "status")


def test_label_names_are_unchanged_when_input_set_mutates():
    label_names = {"route"}
    definition = MetricDefinition(
        name="label_names_snapshot",
        kind="gauge",
        label_names=label_names,
    )

    label_names.add("method")

    assert definition.label_names == ("route",)


def test_rejects_prometheus_reserved_label_prefix():
    with pytest.raises(ValueError, match="Metric label names cannot start with '__'"):
        MetricDefinition(
            name="invalid_label_metric",
            kind="gauge",
            label_names={"__reserved"},
        )


def test_rejects_unknown_kind():
    with pytest.raises(ValueError, match="Unknown metric kind"):
        MetricDefinition(
            name="invalid_metric",
            kind="bogus_kind",  # type: ignore[arg-type]
            label_names=set(),
        )
