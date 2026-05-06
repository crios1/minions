import pytest

from minions._internal._framework.metrics_constants import MINION_WORKFLOW_STARTED_TOTAL
from tests.assets.crash.support.metrics_boom_create_metric import BoomCreateMetricMetrics
from tests.assets.crash.support.boom_metrics import BoomMetrics
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_metrics_create_metric_failure_is_contained():
    logger = InMemoryLogger()
    metrics = BoomCreateMetricMetrics(logger=logger)

    assert await metrics._inc(MINION_WORKFLOW_STARTED_TOTAL) is None
    assert logger.has_log("BoomCreateMetricMetrics._inc_unsafe failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["inc", "set", "observe"])
async def test_metrics_child_operation_failure_is_contained(operation):
    logger = InMemoryLogger()
    metrics = BoomMetrics(logger=logger)

    if operation == "inc":
        assert await metrics._inc(MINION_WORKFLOW_STARTED_TOTAL) is None
        assert logger.has_log("BoomMetrics._inc_unsafe failed")
    elif operation == "set":
        assert await metrics._set("boom_gauge", 1.0) is None
        assert logger.has_log("BoomMetrics._set_unsafe failed")
    elif operation == "observe":
        assert await metrics._observe("boom_histogram", 1.0) is None
        assert logger.has_log("BoomMetrics._observe_unsafe failed")
    else:
        raise Exception("unhandled operation")


@pytest.mark.asyncio
async def test_metrics_snapshot_failures_are_contained():
    logger = InMemoryLogger()
    metrics = BoomMetrics(logger=logger)

    assert await metrics._snapshot() == {"counter": {}, "gauge": {}, "histogram": {}}
    assert logger.has_log("BoomMetrics.snapshot_counters failed")
    assert logger.has_log("BoomMetrics.snapshot_gauges failed")
    assert logger.has_log("BoomMetrics.snapshot_histograms failed")
