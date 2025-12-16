"""
Example: a websocket-backed Pipeline that produces one event per received message.

This uses the "simple" approach: `produce_event()` blocks on `ws.recv()` and returns the
next event. The Pipeline's internal `run()` loop will keep calling `produce_event()` forever.

Install dependency:
  pip install websockets

Run (requires a websocket endpoint that sends JSON messages):
  WS_URL='wss://example.com/events' python examples/websocket_simple_pipeline.py
"""

import json
import os
from dataclasses import dataclass
from typing import Any

from minions import Gru, Minion, Pipeline, minion_step


@dataclass
class WsEvent:
    kind: str
    payload: dict[str, Any]


class WebsocketPipeline(Pipeline[WsEvent]):
    async def startup(self):
        import websockets  # type: ignore[import-not-found]

        self._ws_url = os.environ.get("WS_URL") or "wss://example.com/events"
        self._ws = await websockets.connect(self._ws_url)

    async def produce_event(self) -> WsEvent:
        raw = await self._ws.recv()
        message = json.loads(raw)

        # Adjust this mapping to match your server's message shape.
        kind = message.get("type", "message")
        payload = message if isinstance(message, dict) else {"data": message}

        return WsEvent(kind=kind, payload=payload)

    async def shutdown(self):
        await self._ws.close()


@dataclass
class PrintCtx:
    last_kind: str | None = None


class PrintMinion(Minion[WsEvent, PrintCtx]):
    @minion_step
    async def print_event(self):
        self.context.last_kind = self.event.kind
        print(self.event)


async def main():
    gru = await Gru.create()
    await gru.start_minion(PrintMinion, WebsocketPipeline)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

