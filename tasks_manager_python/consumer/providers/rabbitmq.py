from time import sleep
from typing import Generator
from tasks_manager_python.consumer.providers import TaskProvider
from pika.adapters.blocking_connection import BlockingChannel




class RabbitMQTaskProvider(TaskProvider):

    def __init__(self, channel: BlockingChannel, queue_name: str):
        self.queue_name = queue_name
        self.channel = channel


    
    def get_tasks(self) -> Generator[bytes, None, None]:
        for method_frame, properties, body in self.channel.consume(self.queue_name):
            properties
            self.channel.basic_ack(method_frame.delivery_tag)
            yield body