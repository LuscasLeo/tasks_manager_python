from dataclasses import dataclass
import json
from logging import StreamHandler, getLogger
import logging
from time import sleep
from typing import List

from pydantic import BaseModel
from logging_utils import CustomFormatter

from tasks_manager_python.consumer import TaskExecutionData, TaskConsumer, TaskPayloadParser
from tasks_manager_python.consumer.providers.rabbitmq import RabbitMQTaskMetadata, RabbitMQTaskProvider
import pika

logger = getLogger(__name__)


class Hobbie(BaseModel):
    name: str


class SayHelloPayload(BaseModel):
    name: str
    hobbies: List[Hobbie]


class SayHelloParser(TaskPayloadParser[SayHelloPayload]):
    def parse(self, payload: dict) -> SayHelloPayload:
        return SayHelloPayload(**payload)


@dataclass
class RMQConfig:
    host: str
    port: int
    user: str
    password: str
    queue: str


class SayHelloApplication:
    def __init__(self, rmq_config: RMQConfig):

        self.parser = SayHelloParser()

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rmq_config.host,
                port=rmq_config.port,
                credentials=pika.PlainCredentials(
                    rmq_config.user, rmq_config.password)
            )
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=rmq_config.queue)

        self.consumer = TaskConsumer(
            self.parser,
            self.on_message,
            task_provider=RabbitMQTaskProvider(
                self.channel,
                rmq_config.queue
            )
        )

        self.logger = getLogger(f'{__name__}.{self.__class__.__name__}')

        stream_handler = StreamHandler()
        stream_formatter = CustomFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        stream_handler.setLevel(logging.DEBUG)

        self.logger.addHandler(stream_handler)

        self.logger.addHandler(self.consumer.get_logger_handler())

        self.logger.info('SayHelloApplication initialized')
        self.logger.setLevel(logging.DEBUG)

    def on_message(self, context: TaskExecutionData[SayHelloPayload, RabbitMQTaskMetadata]):

        self.logger.debug(f'Executing callback with context: {context}')
        self.logger.info(
            f"Hello {context.payload.name}!, I see that youm have {len(context.payload.hobbies)} hobbies")
        self.logger.info(
            f"They are: {', '.join([hobbie.name for hobbie in context.payload.hobbies])}")
        self.logger.info("waiting 10 seconds")
        sleep(10)
        self.logger.info("done")

    def run(self):
        self.logger.info('SayHelloApplication started')
        self.logger.debug('Consuming messages')
        self.consumer.consume_messages()


if __name__ == '__main__':
    rmq_config = RMQConfig(
        host='localhost',
        port=5672,
        user='guest',
        password='guest',
        queue='hello_queue'
    )
    application = SayHelloApplication(
        rmq_config
    )
    application.run()
