# pyright: reportUnusedClass=false

from dataclasses import dataclass

import pytest

from minions import Minion, minion_step
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent


class TestMinionConfiguration:
    def test_minion_without_config_does_not_expose_config(self):
        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        assert not hasattr(m, "config")

    @pytest.mark.asyncio
    async def test_minion_binds_loaded_config_to_declared_attribute(self):
        @dataclass
        class MyConfig:
            name: str

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            config: MyConfig

            async def load_config(self, config_path: str) -> MyConfig:
                return MyConfig(name=config_path)

            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path="dummy-config-path",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        await m._mn_load_config("alpha")
        assert m.config == MyConfig(name="alpha")

    @pytest.mark.asyncio
    async def test_minion_requires_config_attribute_annotation(self):
        @dataclass
        class MyConfig:
            name: str

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            async def load_config(self, config_path: str) -> MyConfig:
                return MyConfig(name=config_path)

            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path="dummy-config-path",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        with pytest.raises(
            TypeError,
            match="MyMinion must declare a `config` type annotation",
        ):
            await m._mn_load_config("dummy-config-path")

    @pytest.mark.asyncio
    async def test_minion_rejects_config_type_mismatch(self):
        @dataclass
        class DeclaredConfig:
            name: str

        @dataclass
        class LoadedConfig:
            name: str

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            config: DeclaredConfig

            async def load_config(self, config_path: str) -> LoadedConfig:
                return LoadedConfig(name=config_path)

            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path="dummy-config-path",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        with pytest.raises(
            TypeError,
            match="MyMinion.config expects .*DeclaredConfig.* got LoadedConfig",
        ):
            await m._mn_load_config("dummy-config-path")

    @pytest.mark.asyncio
    async def test_minion_rejects_non_model_config_loads(self):
        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            async def load_config(self, config_path: str) -> object:
                return {"name": "alpha"}

            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path="dummy-config-path",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        with pytest.raises(TypeError) as excinfo:
            await m._mn_load_config("dummy-config-path")
        assert str(excinfo.value) == (
            "MyMinion.load_config: config type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )

    @pytest.mark.asyncio
    async def test_minion_requires_file_config_loader_override(self):
        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path="dummy-config-path",
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        with pytest.raises(
            NotImplementedError,
            match="MyMinion.load_config must be overridden",
        ):
            await m.load_config("dummy-config-path")
