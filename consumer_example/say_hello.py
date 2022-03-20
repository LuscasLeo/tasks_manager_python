import json
from logging import getLogger
import logging
from typing import List

from pydantic import BaseModel
from tasks_manager_python.consumer import TaskExecutionData, TaskConsumer, TaskPayloadParser


class Hobbie(BaseModel):
    name: str

logger = getLogger(__name__)

class SayHelloPayload(BaseModel):
    name: str
    hobbies: List[Hobbie]


class SayHelloParser(TaskPayloadParser[SayHelloPayload]):
    def parse(self, message: bytes) -> SayHelloPayload:
        return SayHelloPayload(**json.loads(message))

class SayHelloApplication:
    def __init__(self):
        self.parser = SayHelloParser()
        self.consumer = TaskConsumer(self.parser, self.on_message)

        self.logger = getLogger(f'{__name__}.{self.__class__.__name__}')
        self.logger.addHandler(self.consumer.generateLoggerHandler())
        self.logger.info('SayHelloApplication initialized')
        self.logger.setLevel(logging.DEBUG)

    def on_message(self, context: TaskExecutionData[SayHelloPayload]):
        self.logger.debug(f'Executing callback with context: {context}')
        self.logger.info(
            f"Hello {context.payload.name}!, I see that youm have {len(context.payload.hobbies)} hobbies")
        self.logger.info(f"They are: {', '.join([hobbie.name for hobbie in context.payload.hobbies])}")

    def run(self):
        self.logger.info('SayHelloApplication started')
        self.logger.debug('Consuming messages')
        self.consumer.consume_messages()



if __name__ == '__main__':
    application = SayHelloApplication()
    application.run()
