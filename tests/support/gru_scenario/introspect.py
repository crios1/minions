from typing import Any

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.resource import Resource


class GruIntrospector:
    def __init__(self, gru: Gru):
        self._gru = gru

    def get_minion_class(self, modpath: str) -> type[Minion[Any, Any]]:
        return self._gru._get_minion_class(modpath)

    def get_pipeline_class(self, modpath: str) -> type[Pipeline[Any]]:
        return self._gru._get_pipeline_class(modpath)

    def get_all_resource_dependencies(
        self,
        cls: type[Minion[Any, Any]] | type[Pipeline[Any]] | type[Resource],
    ) -> set[type[Resource]]:
        return self._gru._get_all_resource_dependencies(cls)

    def make_pipeline_id(self, pipeline_modpath: str) -> str:
        return self._gru._make_pipeline_id(pipeline_modpath)

    def get_minion_instance(self, instance_id: str) -> Minion[Any, Any] | None:
        return self._gru._minions_by_id.get(instance_id)

    def get_minion_by_orchestration_id(self, orchestration_id: str) -> Minion[Any, Any] | None:
        return self._gru._minions_by_orchestration_id.get(orchestration_id)

    def get_minions_by_name(self, name: str) -> list[Minion[Any, Any]]:
        return list(self._gru._minions_by_name.get(name, []))

    def get_pipeline_instance(self, pipeline_modpath: str) -> object | None:
        pid = self.make_pipeline_id(pipeline_modpath)
        return self._gru._pipelines.get(pid)

    def get_pipeline_modpath_for_minion(self, instance_id: str) -> str | None:
        pipeline_id = self._gru._minion_pipeline_map.get(instance_id)
        if not pipeline_id:
            return None
        pipeline_inst = self._gru._pipelines.get(pipeline_id)
        if pipeline_inst is None:
            return None
        return getattr(pipeline_inst, "_mn_pipeline_modpath", None)

    def resource_ids_for(self, *, minion_instance_id: str | None, pipeline_modpath: str) -> set[str]:
        pid = self.make_pipeline_id(pipeline_modpath)
        resource_ids: set[str] = set()
        if minion_instance_id is not None:
            resource_ids.update(self._gru._minion_resource_map.get(minion_instance_id, set()))
        resource_ids.update(self._gru._pipeline_resource_map.get(pid, set()))
        return resource_ids

    def get_resource_instance(self, rid: str) -> object | None:
        return self._gru._resources.get(rid)
