from unittest.mock import MagicMock, patch

import pytest

from azfn_starter_kit.azfn.basic_data_flow_example.activities.transform_activity import TransformActivity


@pytest.fixture
def transform_activity():
    activity = TransformActivity()
    activity.logger = MagicMock()
    activity.fs_ = MagicMock()
    return activity

@pytest.mark.parametrize(
    "input_, expected_output, exception",
    [
        (
            {"city": "TestCity", "src_path": "TestPath", "dest_path": "TestPath"},
            "TRANSFORMATION WEATHER TestCity SUCCESS",
            None,
        ),
        ({}, "TRANSFORMATION MISSING_KEY", KeyError("Missing key in input data: 'city'")),
        (
            {"city": "TestCity", "src_path": "TestPath", "dest_path": "TestPath"},
            "TRANSFORMATION FAILURE",
            Exception("Global exception"),
        ),
    ],
)
def test_process(transform_activity, input_, expected_output, exception):
    with patch(
        "azfn_starter_kit.azfn.basic_data_flow_example.activities.transform_activity.weather_transform_process"
    ) as transform_activity_mock:
        if exception:
            transform_activity_mock.side_effect = exception
        else:
            transform_activity_mock.return_value = None
        assert transform_activity.process(input_) == expected_output
