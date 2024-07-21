from unittest import mock

import pandas as pd

from azfn_starter_kit.business_logics.weather.data_transformation.weather_data_transform import (
    weather_transform_process,
)


def test_weather_transform_process():
    mock_fs = mock.Mock()
    mock_fs.list_files.return_value = ["element1"]
    mock_fs.read_json.return_value = pd.DataFrame(
        {
            "properties": [
                {
                    "timeseries": [
                        {
                            "time": "2024-10-04T07:00:00.000Z",
                            "data_instant_details_air_temperature": 22.5,
                            "data_instant_details_relative_humidity": 50.0,
                            "data_next_1_hours_summary_symbol_code": "cloudy",
                        }
                    ]
                }
            ],
        }
    )
    mock_fs.write_parquet.return_value = True
    weather_transform_process("paris", mock_fs, "/src/path", "/dest/path")

    data = {
        "date": ["2024-10-04T07:00:00.000Z"],
        "temperature": [22.5],
        "humidity_level": [50.0],
        "weather_description": ["cloudy"],
        "city": ["paris"],
    }
    excepted_result = pd.DataFrame(data)

    args = mock_fs.write_parquet.call_args[0]
    assert args[0] == "/dest/path"
    assert args[1] == "element1"
    assert args[2].equals(excepted_result)
    mock_fs.write_parquet.assert_called_once()
