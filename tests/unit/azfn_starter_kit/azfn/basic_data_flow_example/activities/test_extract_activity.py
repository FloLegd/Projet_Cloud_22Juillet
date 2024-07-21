from unittest.mock import MagicMock, patch

import pytest

from azfn_starter_kit.azfn.basic_data_flow_example.activities.extract_activity import ExtractActivity


@pytest.fixture
def extract_activity():
    activity = ExtractActivity()
    activity.logger = MagicMock()
    activity.fs_ = MagicMock()
    return activity


@pytest.mark.parametrize(
    "input_, expected_output, exception",
    [
        ({"city": "TestCity", "dest_path": "TestPath"}, "EXTRACTION WEATHER TestCity SUCCESS", None),
        ({}, "EXTRACTION MISSING_KEY", KeyError("Missing key in input data: 'city'")),
        ({"city": "TestCity", "dest_path": "TestPath"}, "EXTRACTION FAILURE", Exception("Global exception")),
    ],
)
def test_process(extract_activity, input_, expected_output, exception):
    with patch(
        "azfn_starter_kit.azfn.basic_data_flow_example.activities.extract_activity.extract_weather_data"
    ) as mock_extract_weather_data:
        if exception:
            mock_extract_weather_data.side_effect = exception
        else:
            mock_extract_weather_data.return_value = None
        assert extract_activity.process(input_) == expected_output
