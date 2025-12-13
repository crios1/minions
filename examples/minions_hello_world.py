import asyncio
from dataclasses import dataclass
from typing import Any
from minions import Minion, Pipeline, Gru, minion_step

# Uncomment to use uvloop:
# import uvloop
# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

@dataclass
class MyEvent:
    greeting: str = "hello world"

class MyPipeline(Pipeline[MyEvent]):
    async def produce_event(self):
        return MyEvent()

@dataclass
class MyContext:
    my_attribute: Any = None

class MyMinion(Minion[MyEvent, MyContext]):
    @minion_step
    async def step_1(self):
        self.context.my_attribute = self.event.greeting
    @minion_step
    async def step_2(self):
        print(self.context.my_attribute)

async def main():
    gru = await Gru.create()
    await gru.start_minion(MyMinion, MyPipeline)

if __name__ == "__main__":
    asyncio.run(main())
