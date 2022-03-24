from abc import ABC, abstractmethod
from time import sleep
from typing import Generator, Generic, TypeVar

from pydantic import BaseModel



T = TypeVar('T')

class TaskProviderData(BaseModel, ABC, Generic[T]):
    data: bytes
    metadata: T
class TaskProvider(ABC, Generic[T]):
    @abstractmethod
    def get_tasks(self) -> Generator[TaskProviderData[T], None, None]:
        raise NotImplementedError('Method get_tasks() is not implemented')
        

class TextFileTaskProvider(TaskProvider[bytes]):
    def get_tasks(self) -> Generator[TaskProviderData, None, None]:
        while True:
            with open('tasks.txt', 'rb') as file:
                for line in file:
                    yield TaskProviderData(data=line, metadata=None)

            with open('tasks.txt', 'w') as file:
                file.truncate()

            sleep(1)
