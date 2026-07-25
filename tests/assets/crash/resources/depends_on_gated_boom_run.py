from tests.assets.crash.resources.gated_boom_run import (
    AssetResource as GatedBoomRunResource,
)
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    gated_boom_run_resource: GatedBoomRunResource


resource = AssetResource
