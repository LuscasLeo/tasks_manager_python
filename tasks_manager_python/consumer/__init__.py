import json
from abc import ABC, abstractmethod
from logging import Handler, getLogger
from typing import Any, Callable, Generic, TypeVar

from pydantic import ValidationError
from tasks_manager_python.consumer.boradcasting.task_execution import (
    TaskExecutionLogEmitter)
from tasks_manager_python.consumer.providers import (TaskProvider,
                                                     TaskProviderData)
from tasks_manager_python.consumer.types import (TaskExecutionData,
                                                 TaskExecutionRawData)
from tasks_manager_python.loggers import TaskExecutionLogHandler

T = TypeVar('T')
METADATATYPE = TypeVar('METADATATYPE')

logger = getLogger(__name__)


class TaskProcessingError(Exception, Generic[METADATATYPE]):
    def __init__(self, raised_exception: Exception, task_provider_data: TaskProviderData[METADATATYPE]) -> None:
        self.raised_exception = raised_exception
        self.task_provider_data = task_provider_data
        super().__init__()


class TaskDecodeError(TaskProcessingError[METADATATYPE]):
    def __init__(self, raised_exception: Exception, data: bytes, task_provider_data: TaskProviderData[METADATATYPE], ) -> None:
        self.data = data
        super().__init__(raised_exception, task_provider_data)


class TaskParseError(TaskProcessingError[METADATATYPE]):
    def __init__(self, raised_exception: Exception, data: dict, task_provider_data: TaskProviderData[METADATATYPE]) -> None:
        self.data = data
        super().__init__(raised_exception, task_provider_data)


class TaskParsePayloadError(TaskProcessingError[METADATATYPE]):
    def __init__(self, raised_exception: Exception, data: dict, task_provider_data: TaskProviderData[METADATATYPE]) -> None:
        self.data = data
        super().__init__(raised_exception, task_provider_data)


class TaskManagementService:

    def register_task_base_decode_error(self, raw_data: bytes, err: Exception) -> None:
        logger.error(f'Error parsing task raw data: {err}')
        logger.error(f'Raw data: {raw_data.decode()}')

    def register_task_base_parse_error(self, data: dict, err: Exception) -> None:
        logger.error(f'Error parsing task raw data: {err}')
        logger.error(f'Data data: {data}')

    def register_task_payload_parse_error(self, raw_data: TaskExecutionRawData, err: Exception) -> None:
        logger.error(f'Error parsing task payload: {err}')
        logger.error(f'Raw data: {(raw_data.dict())}')

    def register_task_execution_error(self, task_execution_data: TaskExecutionData, err: Exception) -> None:
        logger.error(f'Error executing task: {err}')
        logger.error(f'Task execution data: {(task_execution_data.dict())}')

    def register_task_execution_success(self, task_execution_data: TaskExecutionData) -> None:
        logger.info(f'Task execution success: {(task_execution_data.dict())}')


class TaskPayloadParser(ABC, Generic[T]):
    @abstractmethod
    def parse(self, payload: dict) -> T:
        raise NotImplementedError('Method parse() is not implemented')


class TaskConsumer(Generic[T, METADATATYPE]):

    def __init__(self,
                 parser: TaskPayloadParser[T],
                 callback: Callable[[TaskExecutionData[T, METADATATYPE]], Any],
                 on_message_error: Callable[[Any], Any],
                 task_provider: TaskProvider[METADATATYPE],
                 task_execution_emitter: TaskExecutionLogEmitter,
                 task_management_service: TaskManagementService = TaskManagementService()

                 ):
        self.parser = parser
        self.callback = callback
        self.on_message_error = on_message_error
        self.task_provider = task_provider
        self.task_execution_emitter = task_execution_emitter
        self.task_management_service = task_management_service

        self.logger_handler = TaskExecutionLogHandler(
            self.task_execution_emitter)

    def consume_messages(self):
    # try:
        for data in self.task_provider.get_tasks():
            self.execute(data)
    # except Exception as e:
    #     logger.exception(f'Error while consuming messages: {e}', e)

    def execute(self, provider_data: TaskProviderData[METADATATYPE]) -> None:
        bytes_data = provider_data.data
        try:
            base_data = json.loads(bytes_data)
        except json.JSONDecodeError as err:
            logger.exception(
                f'Failed to decode message: {bytes_data.decode()}', err)
            self.on_message_error(TaskDecodeError(
                err, bytes_data, provider_data))
            self.task_management_service.register_task_base_decode_error(
                bytes_data, err)
            return

        try:
            task_execution_raw_data = TaskExecutionRawData(**base_data)
        except ValidationError as err:
            logger.exception(f'Failed to parse message: {base_data}', err)
            self.on_message_error(TaskParseError(
                err, bytes_data, provider_data))
            self.task_management_service.register_task_base_parse_error(
                base_data, err)
            return

        # Parse Execution Payload

        try:
            payload = self.parser.parse(task_execution_raw_data.payload)
        except Exception as err:
            logger.exception('Error while parsing message payload: %s', err)
            self.on_message_error(
                TaskParsePayloadError(err, bytes_data, provider_data))

            self.task_management_service.register_task_payload_parse_error(
                task_execution_raw_data, err)
            return

        execution_data = TaskExecutionData(**{
            **(task_execution_raw_data.dict()),
            'payload': payload,
            'metadata': provider_data.metadata
        })

        self.logger_handler.set_execution_data(execution_data)
        try:
            self.callback(execution_data)

            self.task_management_service.register_task_execution_success(
                execution_data)

        except Exception as err:
            import traceback
            logger.exception(f'Error executing callback: %s\n%s',
                             err, traceback.format_exc())
            logger.error(traceback.format_exc())

            self.task_management_service.register_task_execution_error(
                execution_data, err)
        finally:
            self.logger_handler.set_execution_data(None)

    def get_logger_handler(self) -> Handler:
        return self.logger_handler
