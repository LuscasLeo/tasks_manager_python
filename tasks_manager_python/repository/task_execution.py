

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Dict, List
import signal

from tasks_manager_python.consumer.types import TaskExecutionData


@dataclass
class TaskExecutionLogReport:
    level: str
    message: str
    execution_data: TaskExecutionData
    date_created: datetime
    date_registered: datetime

class TaskExecutionLogRepository:
    
    __logs: Dict[str, List[TaskExecutionLogReport]] = {}
    """
        Dictionary of logs by task_execution_id
    """

    def __init__(self) -> None:
        """
        Initialize the repository
        """

    def register_log(self, task_execution_id: str, level: str, message: str, execution_data: TaskExecutionData) -> None:
        """
        Register a log
        """
        self.__logs.setdefault(task_execution_id, []).append(
            TaskExecutionLogReport(
                level=level,
                message=message,
                execution_data=execution_data,
                date_created=datetime.now(),
                date_registered=datetime.now()
            )
        )


    def get_logs(self, task_execution_id: str) -> List[TaskExecutionLogReport]:
        """
        Get logs for a task execution
        """
        return self.__logs[task_execution_id]