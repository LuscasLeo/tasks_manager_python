from dataclasses import dataclass
from time import sleep
from typing import Generator, Tuple
from tasks_manager_python.consumer.providers import TaskProvider, TaskProviderData
from pika.adapters.blocking_connection import BlockingChannel



@dataclass
class RabbitMQTaskMetadata:
    delivery_tag: int

class RabbitMQTaskProvider(TaskProvider[RabbitMQTaskMetadata]):

    def __init__(self, channel: BlockingChannel, queue_name: str):
        self.queue_name = queue_name
        self.channel = channel


    
    def get_tasks(self) -> Generator[TaskProviderData[RabbitMQTaskMetadata], None, None]:
        for method_frame, properties, body in self.channel.consume(self.queue_name):
            properties
            assert isinstance(body, bytes), 'Body is not bytes'
            yield TaskProviderData(data=body, metadata=RabbitMQTaskMetadata(delivery_tag=method_frame.delivery_tag))