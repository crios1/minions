from tests.assets.crash.resources.boom_run import AssetResource as BoomRunResource
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    boom_run_resource: BoomRunResource


resource = AssetResource
