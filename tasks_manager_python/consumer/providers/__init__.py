from time import sleep
from typing import Generator


class TaskProvider:
    def get_tasks(self) -> Generator[bytes, None, None]:
        while True:
            with open('tasks.txt', 'rb') as file:
                for line in file:
                    yield line

            with open('tasks.txt', 'w') as file:
                file.truncate()

            sleep(1)
