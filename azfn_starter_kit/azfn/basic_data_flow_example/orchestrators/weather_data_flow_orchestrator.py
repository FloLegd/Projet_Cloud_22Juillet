import logging
import traceback
from typing import Generator, List

import azure.durable_functions as df

from azfn_starter_kit.common.durables.core_entity import CoreEntity
from azfn_starter_kit.utilities.file_system import path_builder


class WeatherDataflowOrchestrator(CoreEntity):
    def call_activities(self, context: df.DurableOrchestrationContext) -> Generator:
        try:
            cities = ["PARIS", "MARSEILLE", "LYON"]
            inputs_extract, inputs_transform, inputs_load = self._build_inputs(cities)
            yield from self.run_activities(context, "weather_data_flow_extract_activity", inputs_extract)
            yield from self.run_activities(context, "weather_data_flow_transform_activity", inputs_transform)
            yield from self.run_activities(context, "weather_data_flow_load_activity", inputs_load)

        except Exception as catched_ex: 
            self.conditional_log(context, "%s: \n%s", str(catched_ex), traceback.format_exc(), level=logging.ERROR)

    def _build_inputs(self, cities: List[str]):
        inputs_extract: list = [
            {
                "city": city,
                "dest_path": path_builder(self.fs_.raw_path, city),
            }
            for city in cities
        ]
        inputs_transform: list = [
            {
                "city": city,
                "src_path": path_builder(self.fs_.raw_path, city),
                "dest_path": path_builder(self.fs_.transformed_path, city),
            }
            for city in cities
        ]
        inputs_load: list = [
            {
                "city": city,
                "src_path": path_builder(self.fs_.transformed_path, city),
                "archive_path": path_builder(self.fs_.computed_path, city),
            }
            for city in cities
        ]

        return inputs_extract, inputs_transform, inputs_load
