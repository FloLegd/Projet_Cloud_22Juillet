from azfn_starter_kit.azfn import shared_bp
from azfn_starter_kit.azfn.basic_data_flow_example.activities.extract_activity import ExtractActivity
from azfn_starter_kit.azfn.basic_data_flow_example.activities.load_activity import LoadActivity
from azfn_starter_kit.azfn.basic_data_flow_example.activities.transform_activity import TransformActivity


@shared_bp.activity_trigger(input_name="inputs")
def weather_data_flow_transform_activity(inputs: dict) -> str:
    return TransformActivity().process(inputs)


@shared_bp.activity_trigger(input_name="inputs")
def weather_data_flow_extract_activity(inputs: dict) -> str:
    return ExtractActivity().process(inputs)


@shared_bp.activity_trigger(input_name="inputs")
def weather_data_flow_load_activity(inputs: dict) -> str:
    return LoadActivity().process(inputs)
