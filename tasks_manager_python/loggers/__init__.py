

from logging import Handler, LogRecord

from tasks_manager_python.consumer.boradcasting.task_execution import \
    TaskExecutionLogEmitter
from tasks_manager_python.consumer.types import TaskExecutionData


class TaskExecutionLogHandler(Handler):

    current_execution_data: TaskExecutionData = None

    def __init__(self, task_execution_repository: TaskExecutionLogEmitter):
        super().__init__()
        self.task_execution_repository = task_execution_repository

    def emit(self, record: LogRecord):
        self.task_execution_repository.emit(
            execution_data=self.current_execution_data,
            log=record
        )

    def set_execution_data(self, execution_data: TaskExecutionData):
        self.current_execution_data = execution_data
