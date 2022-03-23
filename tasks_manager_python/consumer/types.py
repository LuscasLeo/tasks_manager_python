from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar('T')
METADATATYPE = TypeVar('METADATATYPE')


@dataclass
class TaskExecutionData(Generic[T, METADATATYPE]):
    task_execution_id: str
    payload: T
    metadata: METADATATYPE


@dataclass
class TaskExecutionRawData:
    task_execution_id: str
    payload: bytes