import traceback

from azfn_starter_kit.business_logics.weather.data_extraction.weather_data_extraction import extract_weather_data
from azfn_starter_kit.common.durables.core_entity import CoreEntity


class ExtractActivity(CoreEntity):
    def process(self, input_: dict) -> str:
        try:
            city: str = input_["city"]
            dest_path: str = input_["dest_path"]
            extract_weather_data(city, self.fs_, dest_path)
            return f"EXTRACTION WEATHER {city} SUCCESS"

        except KeyError as key_err:
            self.logger.error("Missing key in input data: %s", str(key_err))
            return "EXTRACTION MISSING_KEY"
        except Exception as _ex:  # pylint: disable=broad-except
            self.logger.error("%s: \n%s", str(_ex), traceback.format_exc())
            return "EXTRACTION FAILURE"
