import asyncio
from dataclasses import dataclass
from typing import Any

from minions import Minion, Pipeline, Resource, Gru, minion_step


@dataclass
class NewTokenEvent:
    token_id: str

class NewTokensPipeline(Pipeline[NewTokenEvent]):
    async def produce_event(self):
        return NewTokenEvent(token_id='mock-token')
    

class PriceOracle(Resource):
    async def get_price(self, token_id: str):
        return int(token_id)


@dataclass
class MyContext:
    my_attribute: Any = None

class MockStrategy(Minion[NewTokenEvent, MyContext]):
    price_oracle: PriceOracle

    @minion_step
    async def step_1(self):
        price = self.price_oracle.get_price(self.event.token_id)
        self.context.my_attribute = price

    @minion_step
    async def step_2(self):
        print(self.context.my_attribute)


async def main():
    gru = await Gru.create()
    await gru.start_minion(MockStrategy, NewTokensPipeline)


if __name__ == "__main__":
    asyncio.run(main())
