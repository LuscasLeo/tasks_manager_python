
import logging
from dataclasses import dataclass
from logging import Logger, StreamHandler, getLogger
from typing import Callable, Generic, TypeVar

import pika
from pydantic import BaseModel
from logging_utils import CustomFormatter
from tasks_manager_python.consumer import (TaskConsumer, TaskDecodeError,
                                           TaskParseError,
                                           TaskParsePayloadError,
                                           TaskPayloadParser,
                                           TaskProcessingError)
from tasks_manager_python.consumer.types import TaskExecutionData
from tasks_manager_python.rabbitmq import (RabbitMQTaskExecutionLogEmitter,
                                           RabbitMQTaskMetadata,
                                           RabbitMQTaskProvider)

T = TypeVar('T')


class RMQConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    messages_queue: str
    logs_queue: str


class BasicRabbitMQApplication(Generic[T]):
    def __init__(self, rmq_config: RMQConfig, parser: TaskPayloadParser[T], callback: Callable[[TaskExecutionData[T, RabbitMQTaskMetadata], Logger], None]):
        self.callback = callback
        self.parser = parser
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rmq_config.host,
                port=rmq_config.port,
                credentials=pika.PlainCredentials(
                    rmq_config.user, rmq_config.password)
            )
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=rmq_config.messages_queue,
            durable=True,
        )

        self.channel.queue_declare(
            queue=rmq_config.logs_queue,
            durable=True,
        )

        self.consumer = TaskConsumer(
            self.parser,
            self.on_message,
            self.on_message_processing_error,
            task_provider=RabbitMQTaskProvider(
                self.channel,
                rmq_config.messages_queue
            ),
            task_execution_emitter=RabbitMQTaskExecutionLogEmitter(
                channel=self.channel, queue_name=rmq_config.logs_queue)
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

    def on_message(self, context: TaskExecutionData[T, RabbitMQTaskMetadata]):
        self.callback(context, self.logger)

    def on_message_processing_error(self, error: TaskProcessingError[RabbitMQTaskMetadata]):
        if isinstance(error, TaskDecodeError):
            self.logger.exception(
                f'Error decoding message: {error.data}: {error.raised_exception}')
        elif isinstance(error, TaskParseError):
            self.logger.exception(
                f'Error parsing message: {error.data}: {error.raised_exception}')
        elif isinstance(error, TaskParsePayloadError):
            self.logger.exception(
                f'Error parsing payload: {error.data}: {error.raised_exception}')

        self.channel.basic_reject(
            error.task_provider_data.metadata.delivery_tag, requeue=False)

    def run(self):
        self.logger.info('Starting Application')
        self.logger.debug('Consuming messages')
        self.consumer.consume_messages()
