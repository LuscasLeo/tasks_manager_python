from typing import Generic, TypeVar
from pydantic import BaseModel


T = TypeVar('T')
METADATATYPE = TypeVar('METADATATYPE')


class TaskExecutionData(BaseModel, Generic[T, METADATATYPE]):
    task_execution_id: str
    payload: T
    metadata: METADATATYPE


class TaskExecutionRawData(BaseModel):
    task_execution_id: str
    payload: dict