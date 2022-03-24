import json
from logging import LogRecord
from typing import Generator

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pydantic import BaseModel
from tasks_manager_python.consumer.boradcasting.task_execution import (
    TaskExecutionLogEmitter, TaskExecutionLogReport)
from tasks_manager_python.consumer.providers import (TaskProvider,
                                                     TaskProviderData)
from tasks_manager_python.consumer.types import TaskExecutionData


class RabbitMQTaskMetadata(BaseModel):
    delivery_tag: int
    routing_key: str
    exchange: str
    headers: dict


class RabbitMQTaskProvider(TaskProvider[RabbitMQTaskMetadata]):

    def __init__(self, channel: BlockingChannel, queue_name: str):
        self.queue_name = queue_name
        self.channel = channel

    def get_tasks(self) -> Generator[TaskProviderData[RabbitMQTaskMetadata], None, None]:
        for method_frame, properties, body in self.channel.consume(self.queue_name):
            assert isinstance(body, bytes), 'Body is not bytes'
            yield TaskProviderData(
                data=body,
                metadata=RabbitMQTaskMetadata(
                    delivery_tag=method_frame.delivery_tag,
                    routing_key=method_frame.routing_key,
                    exchange=method_frame.exchange,
                    headers=properties.headers
                ),
            )


class RabbitMQTaskExecutionLogEmitter(TaskExecutionLogEmitter):

    def __init__(self, channel: BlockingChannel, queue_name: str):
        self.queue_name = queue_name
        self.channel = channel

    def emit(self, execution_data: TaskExecutionData, log: LogRecord) -> None:
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=json.dumps(
                TaskExecutionLogReport(
                    args=str(log.args),
                    level=log.levelname,
                    message=log.getMessage(),
                    execution_data=execution_data,
                    date_created=log.created,
                    date_registered=log.created,
                    stack_trace=log.exc_text,
                ).dict(),
                default=str
            ),
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers={
                    'task_execution_id': execution_data.task_execution_id if execution_data is not None else None,
                    'level': log.levelname
                }
            )
        )
