from minions._internal._framework.state_store_noop import NoOpStateStore

from tests.assets.crash.boom import boom


class BoomSaveStateStore(NoOpStateStore):
    async def save_context(self, workflow_id: str, orchestration_id: str, context: bytes) -> None:
        boom()
