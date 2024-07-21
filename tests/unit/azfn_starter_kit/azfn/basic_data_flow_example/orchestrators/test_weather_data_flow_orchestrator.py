import logging
from unittest.mock import ANY, MagicMock, patch

from azfn_starter_kit.azfn.basic_data_flow_example.orchestrators.weather_data_flow_orchestrator import (
    WeatherDataflowOrchestrator,
)


def test_build_inputs():
    orchestrator = WeatherDataflowOrchestrator()
    cities = ["CITY_A", "CITY_B"]
    expected_extract = [{"city": city, "dest_path": f"exec/internal/raw/{city}"} for city in cities]
    expected_transform = [
        {"city": city, "src_path": f"exec/internal/raw/{city}", "dest_path": f"exec/internal/transformed/{city}"}
        for city in cities
    ]
    expected_load = [
        {"city": city, "src_path": f"exec/internal/transformed/{city}", "archive_path": f"exec/exposed/computed/{city}"}
        for city in cities
    ]

    inputs_extract, inputs_transform, inputs_load = orchestrator._build_inputs(cities)

    assert inputs_extract == expected_extract
    assert inputs_transform == expected_transform
    assert inputs_load == expected_load


def test_call_activities():
    mock_context = MagicMock()
    orchestrator = WeatherDataflowOrchestrator()
    with patch.object(orchestrator, "run_activities") as mock_run_activities:
        list(orchestrator.call_activities(mock_context))
        test_inputs_extract = [
            {"city": "PARIS", "dest_path": "exec/internal/raw/PARIS"},
            {"city": "MARSEILLE", "dest_path": "exec/internal/raw/MARSEILLE"},
            {"city": "LYON", "dest_path": "exec/internal/raw/LYON"},
        ]
        test_inputs_transform = [
            {"city": "PARIS", "src_path": "exec/internal/raw/PARIS", "dest_path": "exec/internal/transformed/PARIS"},
            {
                "city": "MARSEILLE",
                "src_path": "exec/internal/raw/MARSEILLE",
                "dest_path": "exec/internal/transformed/MARSEILLE",
            },
            {"city": "LYON", "src_path": "exec/internal/raw/LYON", "dest_path": "exec/internal/transformed/LYON"},
        ]
        test_inputs_load = [
            {
                "city": "PARIS",
                "src_path": "exec/internal/transformed/PARIS",
                "archive_path": "exec/exposed/computed/PARIS",
            },
            {
                "city": "MARSEILLE",
                "src_path": "exec/internal/transformed/MARSEILLE",
                "archive_path": "exec/exposed/computed/MARSEILLE",
            },
            {
                "city": "LYON",
                "src_path": "exec/internal/transformed/LYON",
                "archive_path": "exec/exposed/computed/LYON",
            },
        ]

        assert mock_run_activities.call_count == 3

        mock_run_activities.assert_any_call(mock_context, "weather_data_flow_extract_activity", test_inputs_extract)
        mock_run_activities.assert_any_call(mock_context, "weather_data_flow_transform_activity", test_inputs_transform)
        mock_run_activities.assert_any_call(mock_context, "weather_data_flow_load_activity", test_inputs_load)


def test_call_activities_failure():
    mock_context = MagicMock()
    orchestrator = WeatherDataflowOrchestrator()

    with (
        patch.object(orchestrator, "run_activities", side_effect=Exception("Error!")),
        patch.object(orchestrator, "conditional_log") as mock_log,
    ):
        list(orchestrator.call_activities(mock_context))
        mock_log.assert_called_once_with(mock_context, "%s: \n%s", "Error!", ANY, level=logging.ERROR)
