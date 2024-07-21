import logging
from typing import Generator, List, Optional

import azure.durable_functions as df

from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.config.environment import get_settings
from azfn_starter_kit.utilities.logger import get_logger


class CoreEntity:
    """
    A class that provides settings, logging, and filesystem access to the inheriting classes.

    This class is designed to be used as a mixin and should not be instantiated directly. Instead, it
    should be inherited by other classes to provide them with common functionalities.

    Attributes:
        settings (Settings): An object containing the application-wide settings.
        fs_ (DataLakeGen2FileSystemClient): A client to interact with Azure Data Lake storage based on the given
         settings.
        logger (Logger): A logger object, configured specifically for the inheriting class.

    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.fs_ = DataLakeGen2FileSystemClient(
            self.settings.DLS_SETTINGS.STORAGE_NAME,
            self.settings.DLS_SETTINGS.CONTAINER_NAME,
            self.settings.DLS_SETTINGS.RAW_PATH,
            self.settings.DLS_SETTINGS.TRANSFORMED_PATH,
            self.settings.DLS_SETTINGS.COMPUTED_PATH,
            self.settings.DLS_SETTINGS.ACCOUNT_KEY,
        )
        self.logger = get_logger(self.__class__.__name__)
        if type(self) is CoreEntity:  
            self.logger.warning("%s should not be instantiated directly.", __name__)

    def conditional_log(
        self, context: df.DurableOrchestrationContext, message: str, *args, level: Optional[int] = logging.INFO
    ):
        """
        Log a message conditionally if the context is not replaying.

        Parameters:
        - context (df.DurableOrchestrationContext): The durable orchestration context.
        - message (str): Message string to be logged.
        - args: Additional arguments.
        - level (int, optional): The logging level (e.g., logging.INFO, logging.ERROR). Defaults to logging.INFO.
        """
        if not context.is_replaying:
            log_method = self.logger.info
            if level:
                log_method = getattr(self.logger, logging.getLevelName(level).lower(), self.logger.info)
            log_method(message, *args)

    def run_activities(
        self, context: df.DurableOrchestrationContext, activity_name: str, inputs: List[dict]
    ) -> Generator:
        activities_ = [context.call_activity(activity_name, input_) for input_ in inputs]
        outputs_: List[str] = yield context.task_all(activities_)

        self.conditional_log(context, "%s status: %s", activity_name, outputs_)

        return outputs_
