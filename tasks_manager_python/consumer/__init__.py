import json
from abc import ABC, abstractmethod
from dataclasses import asdict
from logging import Handler, getLogger
from typing import Any, Callable, Generic, TypeVar
from tasks_manager_python.consumer.providers import TaskProvider

from tasks_manager_python.consumer.types import (TaskExecutionData,
                                                 TaskExecutionRawData)
from tasks_manager_python.loggers import TaskExecutionLogHandler
from tasks_manager_python.repository.task_execution import \
    TaskExecutionLogRepository

T = TypeVar('T')

logger = getLogger(__name__)


class TaskManagementService:

    def register_task_base_decode_error(self, raw_data: bytes, err: Exception) -> None:
        logger.error(f'Error parsing task raw data: {err}')
        logger.error(f'Raw data: {raw_data.decode()}')

    def register_task_base_parse_error(self, data: dict, err: Exception) -> None:
        logger.error(f'Error parsing task raw data: {err}')
        logger.error(f'Data data: {data}')

    def register_task_payload_parse_error(self, raw_data: TaskExecutionRawData, err: Exception) -> None:
        logger.error(f'Error parsing task payload: {err}')
        logger.error(f'Raw data: {asdict(raw_data)}')

    def register_task_execution_error(self, task_execution_data: TaskExecutionData, err: Exception) -> None:
        logger.error(f'Error executing task: {err}')
        logger.error(f'Task execution data: {asdict(task_execution_data)}')

    def register_task_execution_success(self, task_execution_data: TaskExecutionData) -> None:
        logger.info(f'Task execution success: {asdict(task_execution_data)}')


class TaskPayloadParser(ABC, Generic[T]):
    @abstractmethod
    def parse(self, payload: dict) -> T:
        raise NotImplementedError('Method parse() is not implemented')


class TaskConsumer(Generic[T]):

    def __init__(self,
                 parser: TaskPayloadParser[T],
                 callback: Callable[[TaskExecutionData[T]], Any],
                 task_provider: TaskProvider = TaskProvider(),
                 task_execution_repository: TaskExecutionLogRepository = TaskExecutionLogRepository(),
                 task_management_service: TaskManagementService = TaskManagementService()

                 ):
        self.parser = parser
        self.callback = callback
        self.task_provider = task_provider
        self.task_execution_repository = task_execution_repository
        self.task_management_service = task_management_service
        self.logger_handler = TaskExecutionLogHandler(
            self.task_execution_repository)

    def consume_messages(self):
        while True:
            try:
                for data in self.task_provider.get_tasks():
                    self.execute(data)
            except Exception as e:
                logger.exception(f'Error while consuming messages: {e}', e)

    def execute(self, data: bytes):

        try:
            base_data = json.loads(data)
        except json.JSONDecodeError as err:
            logger.exception(f'Failed to decode message: {data.decode()}', err)
            self.task_management_service.register_task_base_decode_error(
                data, err)
            return

        try:
            task_execution_raw_data = TaskExecutionRawData(**base_data)
        except TypeError as err:
            logger.exception(f'Failed to parse message: {base_data}', err)
            self.task_management_service.register_task_base_parse_error(
                base_data, err)
            return

        # Parse Execution Payload

        try:
            payload = self.parser.parse(task_execution_raw_data.payload)
        except Exception as e:
            logger.exception('Error while parsing message: %s', e)
            self.task_management_service.register_task_payload_parse_error(
                task_execution_raw_data, e)
            return

        execution_data = TaskExecutionData(**{
            **asdict(task_execution_raw_data),
            'payload': payload
        })
        self.logger_handler.set_execution_data(execution_data)
        try:
            self.callback(execution_data)

            self.task_management_service.register_task_execution_success(
                execution_data)

        except Exception as e:
            import traceback
            logger.exception(f'Error executing callback: %s\n%s',
                             e, traceback.format_exc())
            logger.error(traceback.format_exc())

            self.task_management_service.register_task_execution_error(
                execution_data, e)
        finally:
            self.logger_handler.set_execution_data(None)

    def get_logger_handler(self) -> Handler:
        return self.logger_handler
