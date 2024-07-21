import datetime
from unittest import mock
from unittest.mock import patch

import pytest
from requests import RequestException

from azfn_starter_kit.business_logics.weather.data_extraction.weather_data_extraction import (
    WeatherAPIError,
    extract_weather_data,
)


def test_extract_weather_data_success():
    """
    Test to extract weather data from Weather API successfully and write to Data Lake Gen2 File System.
    """
    with mock.patch("requests.get") as mocked_get:
        expected_data = '{"weather": "Sunny"}'
        mocked_response = mock.Mock()
        mocked_response.status_code = 200
        mocked_response.json.return_value = {"weather": "Sunny"}

        mocked_get.return_value = mocked_response

        mock_fs = mock.Mock()
        mock_fs.write_file.return_value = True

        extract_weather_data("Paris", mock_fs, "/path/to/file")

        current_date = datetime.datetime.now().strftime("%Y%m%d")
        expected_file_name = f"WEATHER_Paris_{current_date}.json"
        mock_fs.write_file.assert_called_with("/path/to/file", expected_file_name, expected_data)


@patch("azfn_starter_kit.business_logics.weather.data_extraction.weather_data_extraction.time.sleep")
def test_extract_weather_data_failure(mock_sleep):
    with mock.patch("requests.get", side_effect=RequestException("error")):
        mock_fs = mock.Mock()
        with pytest.raises(WeatherAPIError, match="Maximum retries reached. API call failed."):
            extract_weather_data("Paris", mock_fs, "/path/to/file")
