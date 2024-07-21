from unittest.mock import MagicMock, patch

import pytest

from azfn_starter_kit.azfn.basic_data_flow_example.activities.load_activity import LoadActivity


@pytest.fixture
def load_activity():
    activity = LoadActivity()
    activity.logger = MagicMock()
    activity.fs_ = MagicMock()
    return activity


@pytest.mark.parametrize(
    "input_, expected_output, exception",
    [
        (
            {"city": "TestCity", "src_path": "TestPath", "archive_path": "TestPath"},
            "LOADING WEATHER TestCity SUCCESS",
            None,
        ),
        ({}, "LOADING MISSING_KEY", KeyError("Missing key in input data: 'city'")),
        (
            {"city": "TestCity", "src_path": "TestPath", "archive_path": "TestPath"},
            "LOADING FAILURE",
            Exception("Global exception"),
        ),
    ],
)
def test_process(load_activity, input_, expected_output, exception):
    with patch(
        "azfn_starter_kit.azfn.basic_data_flow_example.activities.load_activity.weather_loading_process"
    ) as load_activity_mock:
        if exception:
            load_activity_mock.side_effect = exception
        else:
            load_activity_mock.return_value = None
        assert load_activity.process(input_) == expected_output
