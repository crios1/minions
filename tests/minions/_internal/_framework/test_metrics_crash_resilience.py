import pytest

from minions._internal._framework.metrics_constants import MINION_WORKFLOW_STARTED_TOTAL
from tests.assets.crash.support.metrics.boom_child_operations import (
    AssetMetrics as BoomChildOperationsMetrics,
)
from tests.assets.crash.support.metrics.boom_create_metric import (
    AssetMetrics as BoomCreateMetricMetrics,
)
from tests.assets.crash.support.metrics.boom_snapshots import (
    AssetMetrics as BoomSnapshotsMetrics,
)
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_metrics_create_metric_failure_is_contained():
    logger = InMemoryLogger()
    metrics = BoomCreateMetricMetrics(logger=logger)

    assert await metrics._mn_inc(MINION_WORKFLOW_STARTED_TOTAL) is None
    assert logger.has_log("AssetMetrics._mn_inc_unsafe failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["inc", "set", "observe"])
async def test_metrics_child_operation_failure_is_contained(operation: str) -> None:
    logger = InMemoryLogger()
    metrics = BoomChildOperationsMetrics(logger=logger)

    if operation == "inc":
        assert await metrics._mn_inc(MINION_WORKFLOW_STARTED_TOTAL) is None
        assert logger.has_log("AssetMetrics._mn_inc_unsafe failed")
    elif operation == "set":
        assert await metrics._mn_set("boom_gauge", 1.0) is None
        assert logger.has_log("AssetMetrics._mn_set_unsafe failed")
    elif operation == "observe":
        assert await metrics._mn_observe("boom_histogram", 1.0) is None
        assert logger.has_log("AssetMetrics._mn_observe_unsafe failed")
    else:
        raise Exception("unhandled operation")


@pytest.mark.asyncio
async def test_metrics_snapshot_failures_are_contained():
    logger = InMemoryLogger()
    metrics = BoomSnapshotsMetrics(logger=logger)

    assert await metrics._mn_snapshot() == {"counter": {}, "gauge": {}, "histogram": {}}
    assert logger.has_log("AssetMetrics.snapshot_counters failed")
    assert logger.has_log("AssetMetrics.snapshot_gauges failed")
    assert logger.has_log("AssetMetrics.snapshot_histograms failed")
