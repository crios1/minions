import asyncio

from minions._internal._domain.pipeline import Pipeline
from .mixin_spy import SpyMixin

from minions._internal._domain.types import T_Event


class SpiedPipeline(SpyMixin, Pipeline[T_Event], defer_pipeline_setup=True):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    async def wait_for_subscribers(
        self,
        expected_subs: int = 1,
        *,
        timeout: float | None = 1.0,
    ) -> None:
        """Test-only synchronization helper for deterministic pipeline fixtures."""
        deadline = (
            asyncio.get_running_loop().time() + timeout
            if timeout is not None
            else None
        )

        while True:
            async with self._mn_subs_lock:
                observed_subs = len(self._mn_subs)
                if observed_subs >= expected_subs:
                    return

            if deadline is None:
                await asyncio.sleep(0.01)
                continue

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(
                    "Timed out waiting for pipeline subscribers "
                    f"(expected>={expected_subs}, observed={observed_subs})."
                )
            await asyncio.sleep(min(0.01, remaining))
