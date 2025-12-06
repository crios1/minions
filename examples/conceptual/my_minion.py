from dataclasses import dataclass
from minions import Minion, minion_step
from minions.exceptions import AbortWorkflow

@dataclass
class MyWorkflowCtx:
    user_id: str
    retries: int = 0

class MyMinion(Minion[dict, MyWorkflowCtx]):
    name = "my-minion" # can add env to name with like os.getenv('ENV'), can override in minion-config-file

    async def startup(self): # startup lifecyle hook
        return
    
    async def shutdown(self): # shutdown lifecycle hook
        return
    
    async def run(self): # can be used for long running background tasks
        return

    # optional override
    # def load_config(self):
    #     return parse_file(self._config_path, into=self.config_class)

    @minion_step
    async def step_1(self, ctx: MyWorkflowCtx):
        print(ctx.user_id)

    @minion_step
    async def step_2(self, ctx: MyWorkflowCtx):
        print(ctx.retries)

    @minion_step
    async def step_3(self, ctx: MyWorkflowCtx):
        raise AbortWorkflow('raise to gracefully abort minion workflow')
    
    @minion_step
    async def step_4(self, ctx: MyWorkflowCtx):
        print(f"won't reach step 4")