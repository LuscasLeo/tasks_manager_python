from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import Formatter, Handler, StreamHandler, getLogger
from time import sleep
from typing import Any, Callable, Generic, TypeVar

T = TypeVar('T')

logger = getLogger(__name__)

@dataclass
class TaskExecutionData(Generic[T]):
    payload: T

class TaskPayloadParser(ABC, Generic[T]):
    @abstractmethod
    def parse(self, message: bytes) -> T:
        raise NotImplementedError('Method parse() is not implemented')

class TaskConsumer(Generic[T]):

    def __init__(self, parser: TaskPayloadParser[T], callback: Callable[[TaskExecutionData[T]], Any]):
        self.parser = parser
        self.callback = callback

    def consume_messages(self):
        while True:
            with open('tasks.txt', 'rb') as file:
                for line in file:
                    self.execute(line)
            
            with open('tasks.txt', 'w') as file:
                file.truncate()

            sleep(1)
    
    def execute(self, data: Any):
        try:
            payload = self.parser.parse(data)
        except Exception as e:
            logger.exception(f'Error while parsing message: {e}', e)
            return
        
        execution_data = TaskExecutionData(payload=payload)
        with TaskExecutionContext(execution_data) as context:
            try:

                self.callback(context.execution_data)
            except Exception as e:
                import traceback
                logger.exception(f'Error executing callback: {e}')
                logger.error(traceback.format_exc())

    def generateLoggerHandler(self) -> Handler:
        stream_handler = StreamHandler()
        formatter = Formatter('%(threadName)s: %(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        return stream_handler

class TaskExecutionContext(Generic[T]):
    def __init__(self, execution_data: TaskExecutionData[T]):
        self.execution_data = execution_data
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        exc_type