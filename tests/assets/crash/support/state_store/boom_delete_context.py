from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.crash.boom import boom


class AssetStateStore(NoOpStateStore):
    async def delete_context(self, workflow_id: str) -> None:
        boom()
