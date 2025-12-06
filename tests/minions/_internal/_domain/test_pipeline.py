# TODO: test that the following fails cuz i require users to pass event type when subclassing

# @dataclass
# class SimpleEvent:
#     timestamp: float

# class SingleEventPipeline(Pipeline):
#     async def produce_event(self) -> SimpleEvent:
#         return SimpleEvent(time.time())