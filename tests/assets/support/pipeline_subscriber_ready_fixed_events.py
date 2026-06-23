import asyncio
import sys

from minions._internal._domain.types import T_Event

from .pipeline_spied import SpiedPipeline


class SubscriberReadyFixedEventsPipeline(
    SpiedPipeline[T_Event],
    defer_pipeline_setup=True,
):
    """Test pipeline that waits for subscribers, emits total_events, then blocks.

    Subclasses implement `produce_event` and can set `total_events` and `expected_subs`.
    The base `run` loop emits exactly `total_events` events, then blocks.
    """

    total_events = 1
    expected_subs = 1

    @classmethod
    def configure_gate(cls, *, expected_subs: int) -> None:
        cls.expected_subs = expected_subs

    async def wait_for_subscribers(
        self,
        expected_subs: int = 1,
        *,
        timeout: float | None = 1.0,
    ) -> None:
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

    async def wait_for_expected_subscribers(self) -> None:
        await self.wait_for_subscribers(type(self).expected_subs)

    async def run(self):
        for _ in range(type(self).total_events):
            await self.wait_for_expected_subscribers()
            await self._mn_produce_and_handle_event()

        while True:
            await asyncio.sleep(sys.maxsize)
