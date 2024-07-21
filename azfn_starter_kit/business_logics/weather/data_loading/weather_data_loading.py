from pathlib import Path

from azfn_starter_kit.common.db.database import DatabaseEngine
from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.config.database_config import AzureSQLConfig
from azfn_starter_kit.utilities.logger import get_logger

_LOGGER = get_logger(__name__)


def weather_loading_process(
    city: str, fs_: DataLakeGen2FileSystemClient, src_path: str, archive_path: str, prefix_file_name: str = "WEATHER"
) -> None:
    """
    Transforms the data from a csv file and writes the transformed data to a parquet file.

    Args:
        fs_ (DataLakeGen2FileSystemClient): An instance of the DataLakeGen2FileSystemClient to interact with Azure
        Data Lake storage.
        city (str): Path to the source files.
        src_path (str): Path to the source files.
        archive_path (str): Path to write the transformed data.
        prefix_file_name(str, optional)

    Returns:
        None
    """
    file_to_load = fs_.list_files(src_path, pattern=f"{prefix_file_name}_{city}", descending_sort=True)[0]

    _LOGGER.info("Processing file: %s", file_to_load)

    data_to_load = fs_.read_parquet(src_path, file_to_load)
    target_file_name = str(Path(file_to_load).with_suffix(".parquet"))

    fs_.write_parquet(archive_path, target_file_name, data_to_load)

    config = AzureSQLConfig.from_env()
    db_ = DatabaseEngine(config)

    db_.df_to_sql(
        data_to_load,
        "weather",
        ["city", "date"],
        "upsert",
        columns=["city", "date", "temperature", "humidity_level", "weather_description"],
    )

    _LOGGER.info("Successfully processed and saved: %s", file_to_load)
