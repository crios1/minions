from minions import Minion, minion_step

# Draft of a Minion using all the Minion Features

class MyMinion(Minion[MyEvent, MyContext]):
    name = "my-minion"

    r1: MyResource1
    r2: MyResource2
    r3: MyResource3

    config: int

    async def load_config(self, config_path: str) -> MyConfig:
        ...

    async def startup(self):
        ...
    
    async def shutdown(self):
        ...

    @minion_step
    async def step_1(self):
        ...
        self.config

    @minion_step
    async def step_2(self):
        ...