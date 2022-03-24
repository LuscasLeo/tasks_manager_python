

from abc import ABC, abstractmethod
from datetime import datetime
from logging import LogRecord
from typing import Dict, List, Optional

from pydantic import BaseModel

from tasks_manager_python.consumer.types import TaskExecutionData


class TaskExecutionLogReport(BaseModel):
    level: str
    message: str
    execution_data: Optional[TaskExecutionData]
    date_created: datetime
    date_registered: datetime
    args: str
    stack_trace: Optional[str]


class TaskExecutionLogEmitter(ABC):

    @abstractmethod
    def emit(self, execution_data: TaskExecutionData, log: LogRecord) -> None:
        """
        Register a log
        """
        raise NotImplementedError('TaskExecutionLogEmitter.emit')


class InMemoryTaskExecutionLogEmitter(TaskExecutionLogEmitter):

    __logs: Dict[str, List[TaskExecutionLogReport]] = {}
    """
        Dictionary of logs by task_execution_id
    """

    def __init__(self) -> None:
        """
        Initialize the repository
        """

    def emit(self, execution_data: TaskExecutionData, log: LogRecord) -> None:
        """
        Register a log
        """
        if execution_data is not None:
            self.__logs.setdefault(execution_data.task_execution_id, []).append(
                TaskExecutionLogReport(
                    level=log.levelname,
                    message=log.message,
                    execution_data=execution_data,
                    date_created=datetime.now(),
                    date_registered=datetime.now(),
                    args=str(log.args),
                    stack_trace=log.stack_info
                )
            )

    def get_logs(self, task_execution_id: str) -> List[TaskExecutionLogReport]:
        """
        Get logs for a task execution
        """
        return self.__logs[task_execution_id]


class NullTaskExecutionLogEmitter(TaskExecutionLogEmitter):
    def emit(self, execution_data: TaskExecutionData, log: LogRecord) -> None:
        pass
