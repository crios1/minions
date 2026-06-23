from minions._internal._framework.state_store import StoredWorkflowContext
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.crash.boom import boom


class AssetStateStore(NoOpStateStore):
    async def get_all_contexts(self) -> list[StoredWorkflowContext]: # pyright: ignore[reportReturnType]
        boom()
