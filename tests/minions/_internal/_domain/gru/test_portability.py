import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Literal, cast

from minions._internal._domain.gru import Gru
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from tests.assets.portability import relocatable_system as relocatable_system_source
from tests.assets.portability.relocatable_system import (
    RESOURCE_COMPONENT_ID,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.pipeline_triggered import TriggeredPipeline

_PROCESS_MODULE = "tests.minions._internal._domain.gru.test_portability"
_PROCESS_TIMEOUT_SECONDS = 10.0
_REPOSITORY_ROOT = Path(__file__).resolve().parents[5]

_ChildCommand = Literal[
    "start-orchestration-and-stop-once-workflow-is-inflight",
    "start-orchestration-and-complete-unfinished-workflow",
]


# Parent-process test and launcher.


def test_unfinished_workflow_continues_after_process_restart_with_relocated_package_and_config(
    tmp_path: Path,
):
    original_package_name = "original_system"
    relocated_package_name = "relocated_system"

    original_components_module = f"{original_package_name}.components"
    relocated_components_module = f"{relocated_package_name}.components"

    original_package_dir = tmp_path / original_package_name
    relocated_package_dir = tmp_path / relocated_package_name

    original_config_path = original_package_dir / "minion.toml"
    relocated_config_path = relocated_package_dir / "minion.toml"

    state_store_path = tmp_path / "state.db"
    stop_result_path = tmp_path / "stop-result.json"
    completion_result_path = tmp_path / "completion-result.json"

    shutil.copytree(
        Path(relocatable_system_source.__file__).parent,
        original_package_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    stop_result = _run_child_process(
        command="start-orchestration-and-stop-once-workflow-is-inflight",
        module_name=original_components_module,
        config_path=original_config_path,
        state_store_path=state_store_path,
        result_path=stop_result_path,
        import_root=tmp_path,
    )
    assert stop_result["start_success"] is True
    assert stop_result["stop_success"] is True
    assert stop_result["shutdown_success"] is True
    assert stop_result["stored_workflow_context_count"] == 1
    assert (
        stop_result["stored_workflow_orchestration_id"]
        == stop_result["orchestration_id"]
    )
    assert stop_result["started_step_names"] == [
        "capture_config_marker",
        "record_context_marker",
    ]
    orchestration_id = stop_result["orchestration_id"]
    workflow_id = stop_result["workflow_id"]
    assert isinstance(orchestration_id, str)
    assert isinstance(workflow_id, str)

    # Relocate the system package, including its config, between child-process lifetimes.
    original_package_dir.rename(relocated_package_dir)

    completion = _run_child_process(
        command="start-orchestration-and-complete-unfinished-workflow",
        module_name=relocated_components_module,
        config_path=relocated_config_path,
        state_store_path=state_store_path,
        result_path=completion_result_path,
        import_root=tmp_path,
        workflow_id=workflow_id,
    )
    assert completion["start_success"] is True
    assert completion["orchestration_id"] == orchestration_id
    assert completion["pipeline_module"] == relocated_components_module
    assert completion["minion_module"] == relocated_components_module
    assert completion["config_module"] == relocated_components_module
    assert completion["config_marker"] == "marker"
    assert completion["resource_module"] == relocated_components_module
    assert completion["runtime_resource_ids"] == [RESOURCE_COMPONENT_ID]
    assert completion["workflow_resumed"] is True
    assert completion["started_step_names"] == ["record_context_marker"]
    assert completion["workflow_succeeded"] is True
    assert completion["recorded_markers"] == ["marker"]
    assert completion["stored_workflow_context_count"] == 0
    assert completion["stop_success"] is True
    assert completion["shutdown_success"] is True


def _run_child_process(
    *,
    command: _ChildCommand,
    module_name: str,
    config_path: Path,
    state_store_path: Path,
    result_path: Path,
    import_root: Path,
    workflow_id: str | None = None,
) -> dict[str, object]:
    process_command = [
        sys.executable,
        "-m", _PROCESS_MODULE,
        "--command", command,
        "--module", module_name,
        "--config-path", str(config_path),
        "--state-store-path", str(state_store_path),
        "--result-path", str(result_path),
    ]
    if workflow_id is not None:
        process_command.extend(("--workflow-id", workflow_id))

    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        path
        for path in (
            str(_REPOSITORY_ROOT),
            str(_REPOSITORY_ROOT / "src"),
            environment.get("PYTHONPATH"),
        )
        if path
    )
    completed = subprocess.run(
        process_command,
        cwd=import_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=_PROCESS_TIMEOUT_SECONDS,
    )
    assert completed.returncode == 0, (
        f"{command} subprocess failed in {import_root}:\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    assert result_path.is_file(), f"{command} subprocess did not write {result_path}"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert isinstance(result, dict)
    return cast(dict[str, object], result)


# Child-process entrypoint and behavior.


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--command",
        choices=(
            "start-orchestration-and-stop-once-workflow-is-inflight",
            "start-orchestration-and-complete-unfinished-workflow",
        ),
        required=True,
    )
    parser.add_argument("--module", required=True)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--state-store-path", required=True)
    parser.add_argument("--result-path", type=Path, required=True)
    parser.add_argument("--workflow-id")
    args = parser.parse_args()

    result = asyncio.run(
        _run_child_command(
            command=cast(_ChildCommand, args.command),
            module_name=cast(str, args.module),
            config_path=cast(str, args.config_path),
            state_store_path=cast(str, args.state_store_path),
            workflow_id=cast(str | None, args.workflow_id),
        )
    )
    result_path = cast(Path, args.result_path)
    result_path.write_text(json.dumps(result), encoding="utf-8")


async def _run_child_command(
    *,
    command: _ChildCommand,
    module_name: str,
    config_path: str,
    state_store_path: str,
    workflow_id: str | None,
) -> dict[str, object]:
    logger = InMemoryLogger()
    state_store = SQLiteStateStore(
        db_path=state_store_path,
        logger=logger,
        batch_max_queued_writes=1,
    )
    gru = await Gru.create(
        state_store=state_store,
        logger=logger,
        metrics=NoOpMetrics(),
    )
    result: dict[str, object] = {}
    try:
        started = await gru.start_orchestration(
            pipeline=module_name,
            minion=module_name,
            minion_config_path=config_path,
        )
        result.update(
            start_success=started.success,
            start_reason=started.reason,
            orchestration_id=started.orchestration_id,
        )
        if not started.success:
            return result

        assert started.orchestration_id is not None
        orchestration_id = started.orchestration_id

        if command == "start-orchestration-and-stop-once-workflow-is-inflight":
            observations = await _trigger_workflow_and_stop_orchestration_while_inflight(
                gru=gru,
                state_store=state_store,
                logger=logger,
                orchestration_id=orchestration_id,
            )
        elif command == "start-orchestration-and-complete-unfinished-workflow":
            if workflow_id is None:
                raise ValueError(
                    f"{command} command requires --workflow-id"
                )
            observations = await _complete_unfinished_workflow(
                gru=gru,
                state_store=state_store,
                logger=logger,
                orchestration_id=orchestration_id,
                workflow_id=workflow_id,
            )
        else:
            raise AssertionError(f"Unexpected child command: {command}")
        result.update(observations)
        return result
    finally:
        shutdown = await gru.shutdown()
        result.update(
            shutdown_success=shutdown.success,
            shutdown_reason=shutdown.reason,
        )


async def _trigger_workflow_and_stop_orchestration_while_inflight(
    *,
    gru: Gru,
    state_store: SQLiteStateStore,
    logger: InMemoryLogger,
    orchestration_id: str,
) -> dict[str, object]:
    orchestration = gru._orchestrations[orchestration_id]
    pipeline = orchestration.pipeline
    if not isinstance(pipeline, TriggeredPipeline):
        raise RuntimeError(f"Pipeline is not triggerable: {type(pipeline).__name__}")
    resource = _require_injected_resource(
        orchestration.minion,
        resource_id=RESOURCE_COMPONENT_ID,
    )
    recording_started, _ = _require_recording_events(resource)

    await pipeline.wait_for_subscribers_then_emit_event()
    await asyncio.wait_for(recording_started.wait(), timeout=2.0)

    stopped = await gru.stop_orchestration(orchestration_id)
    stored_workflow_contexts = await state_store.get_all_contexts()
    observations: dict[str, object] = {
        "stop_success": stopped.success,
        "stop_reason": stopped.reason,
        "stored_workflow_context_count": len(stored_workflow_contexts),
    }
    if len(stored_workflow_contexts) == 1:
        stored_workflow_context = stored_workflow_contexts[0]
        observations.update(
            stored_workflow_orchestration_id=stored_workflow_context.orchestration_id,
            workflow_id=stored_workflow_context.workflow_id,
            started_step_names=_started_step_names(
                logger,
                workflow_id=stored_workflow_context.workflow_id,
                orchestration_id=orchestration_id,
            ),
        )
    return observations


async def _complete_unfinished_workflow(
    *,
    gru: Gru,
    state_store: SQLiteStateStore,
    logger: InMemoryLogger,
    orchestration_id: str,
    workflow_id: str,
) -> dict[str, object]:
    orchestration = gru._orchestrations[orchestration_id]
    config = getattr(orchestration.minion, "config", None)
    if config is None:
        raise RuntimeError("Minion has no loaded config")
    resource = _require_injected_resource(
        orchestration.minion,
        resource_id=RESOURCE_COMPONENT_ID,
    )
    recording_started, allow_recording = _require_recording_events(resource)
    snapshot = await gru.runtime_state_snapshot()

    await asyncio.wait_for(recording_started.wait(), timeout=2.0)
    workflow_log_kwargs: dict[str, object] = {
        "workflow_id": workflow_id,
        "orchestration_id": orchestration_id,
    }
    workflow_resumed = await logger.wait_for_log(
        "Workflow resumed",
        timeout=2.0,
        log_kwargs=workflow_log_kwargs,
    )
    started_step_names = _started_step_names(
        logger,
        workflow_id=workflow_id,
        orchestration_id=orchestration_id,
    )

    allow_recording.set()
    workflow_succeeded = await logger.wait_for_log(
        "Workflow succeeded",
        timeout=2.0,
        log_kwargs=workflow_log_kwargs,
    )
    stored_workflow_contexts = await state_store.get_all_contexts()
    stopped = await gru.stop_orchestration(orchestration_id)
    return {
        "pipeline_module": type(orchestration.pipeline).__module__,
        "minion_module": type(orchestration.minion).__module__,
        "config_module": type(config).__module__,
        "config_marker": getattr(config, "marker", None),
        "resource_module": type(resource).__module__,
        "runtime_resource_ids": sorted(snapshot.resources),
        "workflow_resumed": workflow_resumed,
        "started_step_names": started_step_names,
        "workflow_succeeded": workflow_succeeded,
        "recorded_markers": getattr(resource, "recorded_markers", None),
        "stored_workflow_context_count": len(stored_workflow_contexts),
        "stop_success": stopped.success,
        "stop_reason": stopped.reason,
    }


def _require_injected_resource(
    minion: object,
    *,
    resource_id: str,
) -> object:
    resource = getattr(minion, "resource", None)
    assert resource is not None
    assert getattr(resource, "_mn_resource_id", None) == resource_id
    return resource


def _require_recording_events(resource: object) -> tuple[asyncio.Event, asyncio.Event]:
    recording_started = getattr(resource, "recording_started", None)
    allow_recording = getattr(resource, "allow_recording", None)
    if not isinstance(recording_started, asyncio.Event):
        raise RuntimeError("Resource recording_started is not an asyncio.Event")
    if not isinstance(allow_recording, asyncio.Event):
        raise RuntimeError("Resource allow_recording is not an asyncio.Event")
    return recording_started, allow_recording


def _started_step_names(
    logger: InMemoryLogger,
    *,
    workflow_id: str,
    orchestration_id: str,
) -> list[str]:
    step_names: list[str] = []
    for log in logger.logs:
        if (
            log.msg != "Workflow Step started"
            or log.kwargs.get("workflow_id") != workflow_id
            or log.kwargs.get("orchestration_id") != orchestration_id
        ):
            continue
        step_name = log.kwargs.get("step_name")
        if not isinstance(step_name, str):
            raise RuntimeError("Workflow step log has no string step_name")
        step_names.append(step_name)
    return step_names


if __name__ == "__main__":
    main()
