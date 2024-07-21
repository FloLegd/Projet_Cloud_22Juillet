from typing import Generator

import azure.durable_functions as df

from azfn_starter_kit.azfn import shared_bp
from azfn_starter_kit.azfn.basic_data_flow_example.orchestrators.weather_data_flow_orchestrator import (
    WeatherDataflowOrchestrator,
)


@shared_bp.orchestration_trigger(context_name="context")
def weather_data_flow_orchestrator(
    context: df.DurableOrchestrationContext,
) -> Generator:
    return WeatherDataflowOrchestrator().call_activities(context)
