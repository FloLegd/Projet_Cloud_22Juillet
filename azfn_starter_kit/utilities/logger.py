import logging

from azfn_starter_kit.config.environment import get_settings

_SETTINGS = get_settings()
_LOG_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"


def __get_stream_handler() -> logging.StreamHandler:
    """
    This method create a log stream handler to be able to display logs in stdout

    Returns
    -------
    log_stream_handler (logging.StreamHandler): A stream handler to display
    messages to the stdout
    """
    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setLevel(_SETTINGS.LOG_LEVEL)
    log_stream_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    return log_stream_handler


def get_logger(name: str, log_level: str = _SETTINGS.LOG_LEVEL) -> logging.Logger:
    """
    This method create a logger with a level based on the environment variable
    Example of message to be shown:
    2021-11-17 17:31:48,648 - [INFO] - main - (main.py).process_data(36) - something

    Parameters
    ----------
    @param: name (str): Name of the class to be used in the log

    Returns
    -------
    logger (logging.Logger): The logger to be used to log messages
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        logger.addHandler(__get_stream_handler())
        logger.propagate = False
    return logger
