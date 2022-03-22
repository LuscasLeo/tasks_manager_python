from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar('T')


@dataclass
class TaskExecutionData(Generic[T]):
    task_execution_id: str
    payload: T


@dataclass
class TaskExecutionRawData:
    task_execution_id: str
    payload: bytes