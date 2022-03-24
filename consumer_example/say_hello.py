from logging import StreamHandler, getLogger
from typing import List

from pydantic import BaseModel
from tasks_manager_python.consumer import TaskPayloadParser
from tasks_manager_python.rabbitmq.app import (BasicRabbitMQApplication,
                                               RMQConfig)

logger = getLogger(__name__)
logger.addHandler(StreamHandler())

class Hobbie(BaseModel):
    name: str


class SayHelloPayload(BaseModel):
    name: str
    hobbies: List[Hobbie]


class SayHelloParser(TaskPayloadParser[SayHelloPayload]):
    def parse(self, payload: dict) -> SayHelloPayload:
        return SayHelloPayload(**payload)


if __name__ == '__main__':
    rmq_config = RMQConfig(
        host='localhost',
        port=5672,
        user='guest',
        password='guest',
        messages_queue='hello_queue',
        logs_queue='hello_logs_queue'
    )

    application = BasicRabbitMQApplication(
        rmq_config=rmq_config,
        parser=SayHelloParser(),
        callback=lambda task_execution_data, logger: logger.info(
            f'Task execution data: {task_execution_data}')
    ) 

    logger.addHandler(
        application.consumer.get_logger_handler()
    )

    application.run()
