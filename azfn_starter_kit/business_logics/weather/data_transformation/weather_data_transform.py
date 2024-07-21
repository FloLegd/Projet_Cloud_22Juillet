import pandas as pd

from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.utilities.logger import get_logger

_LOGGER = get_logger(__name__)


def _weather_transform(city: str, df_weather: pd.DataFrame) -> pd.DataFrame:
    df_weather = pd.json_normalize(df_weather["properties"], record_path="timeseries", sep="_")
    df_weather.rename(
        columns={
            "time": "date",
            "data_instant_details_air_temperature": "temperature",
            "data_instant_details_relative_humidity": "humidity_level",
            "data_next_1_hours_summary_symbol_code": "weather_description",
        },
        inplace=True,
    )
    df_weather = df_weather[["date", "temperature", "humidity_level", "weather_description"]]
    df_weather["city"] = city
    return df_weather


def weather_transform_process(
    city: str, fs_: DataLakeGen2FileSystemClient, src_path: str, dest_path: str, prefix_file_name: str = "WEATHER"
) -> None:
    """
    Transforms the data from a csv file and writes the transformed data to a parquet file.

    Args:
        fs_ (DataLakeGen2FileSystemClient): An instance of the DataLakeGen2FileSystemClient to interact with Azure
        Data Lake storage.
        city (str): Path to the source files.
        src_path (str): Path to the source files.
        dest_path (str): Path to write the transformed data.
        prefix_file_name(str, optional)

    Returns:
        None
    """

    file_to_transform = fs_.list_files(src_path, pattern=f"{prefix_file_name}_{city}", descending_sort=True)[0]
    
    _LOGGER.info("Processing file: %s", file_to_transform)

    data_df = fs_.read_json(src_path, file_to_transform, lines=True)
    transformed_data = _weather_transform(city, data_df)
    fs_.write_parquet(dest_path, file_to_transform, transformed_data)

    _LOGGER.info("Successfully processed and saved: %s", file_to_transform)
