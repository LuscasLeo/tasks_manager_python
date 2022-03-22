

from logging import Handler, LogRecord, StreamHandler
from sys import stdin
from typing import Generic, TypeVar

from tasks_manager_python.consumer.types import TaskExecutionData
from tasks_manager_python.repository.task_execution import TaskExecutionLogRepository


class TaskExecutionLogHandler(Handler):

    current_execution_data: TaskExecutionData = None

    def __init__(self, task_execution_repository: TaskExecutionLogRepository):
        super().__init__()
        self.task_execution_repository = task_execution_repository

    def emit(self, record: LogRecord):
        if self.current_execution_data is not None:
            self.task_execution_repository.register_log(
                task_execution_id=self.current_execution_data.task_execution_id,
                level=record.levelname,
                message=record.msg,
                execution_data=self.current_execution_data
            )

    def set_execution_data(self, execution_data: TaskExecutionData):
        self.current_execution_data = execution_data
