import datetime
import json
import time
from typing import Any

import requests
from geopy.geocoders import Nominatim

from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.config.weather_api import WeatherApiSettings
from azfn_starter_kit.utilities.logger import get_logger

_LOGGER = get_logger(__name__)

_GEO_USER_AGENT = "testapplication"

_WEATHER_API_SETTINGS = WeatherApiSettings()


class WeatherAPIError(Exception):
    pass


def _call_weather_api(url: str, headers: dict, max_retries: int) -> Any:
    retry_count = 0
    while retry_count < max_retries:
        try:
            data = requests.get(url, headers=headers, timeout=3).json()
            return data
        except requests.exceptions.RequestException as req_exc:
            _LOGGER.warning("API call failed:%s", req_exc)
            retry_count += 1
            time.sleep(3)
    raise WeatherAPIError("Maximum retries reached. API call failed.")


def _get_data_from_weather_api(city: str) -> str:
    geolocator = Nominatim(user_agent=_GEO_USER_AGENT)
    location = geolocator.geocode(city)
    data = _call_weather_api(
        f"{_WEATHER_API_SETTINGS.API_URI}?lat={location.latitude}&lon={location.longitude}",
        headers=_WEATHER_API_SETTINGS.HEADER,
        max_retries=5,
    )
    return json.dumps(data)


def extract_weather_data(
    city: str, fs_: DataLakeGen2FileSystemClient, dest_path: str, prefix_file_name: str = "WEATHER"
) -> None:
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    dest_file_name = f"{prefix_file_name}_{city}_{current_date}.json"
    weather_data = _get_data_from_weather_api(city)
    fs_.write_file(dest_path, dest_file_name, weather_data)
