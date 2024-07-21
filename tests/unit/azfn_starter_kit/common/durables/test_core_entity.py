import logging
from unittest.mock import MagicMock, Mock, patch

import pytest
from azure.durable_functions import DurableOrchestrationContext

from azfn_starter_kit.common.durables.core_entity import CoreEntity
from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.config.environment import get_settings


class TestCoreEntity:
    def test_init_should_set_variables(self):
        settings = get_settings()
        entity = CoreEntity()
        assert entity.settings == settings

        assert entity.logger is not None
        assert entity.logger.name == "CoreEntity"

        assert isinstance(entity.fs_, DataLakeGen2FileSystemClient)
        assert entity.fs_.storage_name == settings.DLS_SETTINGS.STORAGE_NAME
        assert entity.fs_.raw_path == settings.DLS_SETTINGS.RAW_PATH
        assert entity.fs_.transformed_path == settings.DLS_SETTINGS.TRANSFORMED_PATH
        assert entity.fs_.computed_path == settings.DLS_SETTINGS.COMPUTED_PATH

    @pytest.mark.parametrize(
        ("is_replaying", "log_msg", "log_level"),
        [(False, "message", logging.INFO), (False, "message", None), (True, "message", None)],
    )
    def test_conditional_log(self, is_replaying, log_msg, log_level):
        context = Mock()
        context.is_replaying = is_replaying
        entity = CoreEntity()
        entity.logger = Mock()

        entity.conditional_log(context, log_msg, level=log_level)

        if not is_replaying:
            entity.logger.info.assert_called()
        else:
            entity.logger.assert_not_called()

    def test_run_activities(self):
        with patch(
            "azure.durable_functions.DurableOrchestrationContext", spec=DurableOrchestrationContext
        ) as context_mock:
            context_mock.call_activity = MagicMock()
            context_mock.task_all.side_effect = MagicMock(return_value=["result1", "result2"])
            inputs = [{"input": "data1"}, {"input": "data2"}]
            activity_name = "TestActivity"

            entity = CoreEntity()
            entity.conditional_log = Mock()

            gen = entity.run_activities(context_mock, activity_name, inputs)
            output = next(gen)
            try:
                outputs = gen.send(output)
            except StopIteration as e:
                outputs = e.value

            assert context_mock.call_activity.call_count == len(inputs)
            context_mock.task_all.assert_called_once()
            assert outputs == ["result1", "result2"]
            entity.conditional_log.assert_called_once_with(
                context_mock, "%s status: %s", activity_name, ["result1", "result2"]
            )
