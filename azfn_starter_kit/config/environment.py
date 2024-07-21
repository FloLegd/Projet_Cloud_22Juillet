import os
from typing import Callable

from pydantic import BaseSettings

from azfn_starter_kit.config.datalake_config import DataLakeConfig


class Settings(BaseSettings):
    ENV: str = os.getenv("ENV", "local")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG")
    APP_DEBUG: bool = True
    DLS_SETTINGS: DataLakeConfig = DataLakeConfig()


def _configure_initial_settings() -> Callable[[], Settings]:
    settings = Settings()

    def func_layer() -> Settings:
        return settings

    return func_layer


get_settings = _configure_initial_settings()
