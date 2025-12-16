"""
Example: a websocket-backed Pipeline using a background reader + asyncio.Queue.

This is the "buffered" approach:
  - A reader task continuously receives websocket messages and enqueues events.
  - `produce_event()` dequeues one event at a time.

Why do this?
  - Handle bursts (buffering).
  - Apply explicit backpressure with `Queue(maxsize=...)`.
  - Centralize reconnect logic in the reader loop (if you add it).

Important in Minions user code:
  - Don't call `asyncio.create_task(...)`; use `self.safe_create_task(...)` instead.

Install dependency:
  pip install websockets

Run (requires a websocket endpoint that sends JSON messages):
  WS_URL='wss://example.com/events' python examples/websocket_buffered_pipeline.py
"""

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any

from minions import Gru, Minion, Pipeline, minion_step


@dataclass
class WsEvent:
    payload: dict[str, Any]


class BufferedWebsocketPipeline(Pipeline[WsEvent]):
    async def startup(self):
        import websockets  # type: ignore[import-not-found]

        self._ws_url = os.environ.get("WS_URL") or "wss://example.com/events"
        self._queue: asyncio.Queue[WsEvent] = asyncio.Queue(maxsize=1_000)

        self._ws = await websockets.connect(self._ws_url)
        self._reader_task = self.safe_create_task(self._reader_loop())

    async def _reader_loop(self):
        while True:
            raw = await self._ws.recv()
            message = json.loads(raw)
            payload = message if isinstance(message, dict) else {"data": message}
            await self._queue.put(WsEvent(payload=payload))

    async def produce_event(self) -> WsEvent:
        return await self._queue.get()

    async def shutdown(self):
        self._reader_task.cancel()
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass
        await self._ws.close()


@dataclass
class PrintCtx:
    count: int = 0


class PrintMinion(Minion[WsEvent, PrintCtx]):
    @minion_step
    async def print_event(self):
        self.context.count += 1
        print(self.context.count, self.event)


async def main():
    gru = await Gru.create()
    await gru.start_minion(PrintMinion, BufferedWebsocketPipeline)


if __name__ == "__main__":
    asyncio.run(main())

