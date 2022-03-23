from time import sleep
from typing import Generator
from tasks_manager_python.consumer.providers import TaskProvider
import pika




class RabbitMQTaskProvider(TaskProvider):

    def __init__(self, connection: pika.BlockingConnection):
        self.connection = connection
        self.channel = connection.channel()
        self.channel.queue_declare(queue='task_queue')


    
    def get_tasks(self) -> Generator[bytes, None, None]:
        for method_frame, properties, body in self.channel.consume('task_queue'):
            properties
            self.channel.basic_ack(method_frame.delivery_tag)
            yield body