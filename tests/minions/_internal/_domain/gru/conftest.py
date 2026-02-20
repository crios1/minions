import importlib
from pathlib import Path

import pytest

collect_ignore_glob = ["archived/*.py"]


@pytest.fixture
def tests_dir() -> Path:
    return Path(__file__).resolve().parents[4]


@pytest.fixture
def reload_wait_for_subs_pipeline():
    def _reload(*, expected_subs: int):
        from tests.assets.support import pipeline_wait_for_subs

        importlib.reload(pipeline_wait_for_subs)
        from tests.assets.support.pipeline_wait_for_subs import WaitForSubsPipeline

        WaitForSubsPipeline.reset_gate(expected_subs=expected_subs)

    return _reload


@pytest.fixture
def reload_pipeline_module():
    def _reload(module_name: str):
        mod = importlib.import_module(module_name)
        pipeline_cls = getattr(mod, "pipeline", None)
        if pipeline_cls is None:
            return
        if hasattr(pipeline_cls, "reset_gate"):
            pipeline_cls.reset_gate()
            return
        if hasattr(pipeline_cls, "_emitted"):
            pipeline_cls._emitted = False

    return _reload
