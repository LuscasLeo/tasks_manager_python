from dataclasses import dataclass
from typing import Generic, TypeVar
from pydantic import BaseModel


T = TypeVar('T')
METADATATYPE = TypeVar('METADATATYPE')


@dataclass
class TaskExecutionData(Generic[T, METADATATYPE]):
    task_execution_id: str
    payload: T
    metadata: METADATATYPE


class TaskExecutionRawData(BaseModel):
    task_execution_id: str
    payload: dict