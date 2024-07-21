import os
from unittest import mock

import pandas as pd
import pytest
from azure.identity import DefaultAzureCredential

from azfn_starter_kit.business_logics.weather.data_loading.weather_data_loading import weather_loading_process


@pytest.fixture(autouse=True)
def token_mock(monkeypatch):
    def mock_get_token(self, *args, **kwargs):
        class DummyToken:
            def __init__(self, cool):
                self.token = cool

        return DummyToken("abc")

    monkeypatch.setattr(DefaultAzureCredential, "get_token", mock_get_token)


def test_weather_loading_process_with_data():
    df_weather = pd.DataFrame(
        {
            "city": ["Paris"],
            "date": ["2024-10-04T07:00:00.000Z"],
            "temperature": [22.5],
            "humidity_level": [50.0],
            "weather_description": ["10d"],
        }
    )

    with mock.patch(
        "azfn_starter_kit.business_logics.weather.data_loading.weather_data_loading.DatabaseEngine.df_to_sql"
    ) as db_:
        db_.return_value = True
        fs_ = mock.Mock()
        fs_.list_files.return_value = ["element1"]
        fs_.read_parquet.return_value = df_weather
        fs_.write_parquet.return_value = True
        os.environ["DB_SERVER"] = "tata"
        os.environ["DB_WEATHER"] = "tete"

        weather_loading_process("Paris", fs_, "/mnt/source", "/mnt/destination")

        db_.assert_called_once_with(
            df_weather,
            "weather",
            ["city", "date"],
            "upsert",
            columns=["city", "date", "temperature", "humidity_level", "weather_description"],
        )
