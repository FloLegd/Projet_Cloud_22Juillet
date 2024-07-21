import traceback

from azfn_starter_kit.business_logics.weather.data_transformation.weather_data_transform import (
    weather_transform_process,
)
from azfn_starter_kit.common.durables.core_entity import CoreEntity


class TransformActivity(CoreEntity):
    def process(self, input_: dict) -> str:
        try:
            city: str = input_["city"]
            src_path: str = input_["src_path"]
            dest_path: str = input_["dest_path"]
            weather_transform_process(city, self.fs_, src_path, dest_path)
            return f"TRANSFORMATION WEATHER {city} SUCCESS"
        except KeyError as key_err:
            self.logger.error("Missing key in input data: %s", str(key_err))
            return "TRANSFORMATION MISSING_KEY"
        except Exception as _ex: 
            self.logger.error("%s: \n%s", str(_ex), traceback.format_exc())
            return "TRANSFORMATION FAILURE"
