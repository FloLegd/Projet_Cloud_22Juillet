import traceback

from azfn_starter_kit.business_logics.weather.data_loading.weather_data_loading import weather_loading_process
from azfn_starter_kit.common.durables.core_entity import CoreEntity


class LoadActivity(CoreEntity):
    def process(self, input_: dict) -> str:
        try:
            city: str = input_["city"]
            src_path: str = input_["src_path"]
            archive_path: str = input_["archive_path"]
            weather_loading_process(city, self.fs_, src_path, archive_path)
            return f"LOADING WEATHER {city} SUCCESS"

        except KeyError as key_err:
            self.logger.error("Missing key in input data: %s", str(key_err))
            return "LOADING MISSING_KEY"
        except Exception as _ex:  # pylint: disable=broad-except
            self.logger.error("%s: \n%s", str(_ex), traceback.format_exc())
            return "LOADING FAILURE"
