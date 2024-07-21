import azure.durable_functions as df
import azure.functions as func

from azfn_starter_kit.azfn import shared_bp
from azfn_starter_kit.common.durables.triggers import HttpTrigger, TimerTrigger


@shared_bp.route(route="weatherDataflowhttp")
@shared_bp.durable_client_input(client_name="client")
async def weather_data_flow_http_trigger(
    req: func.HttpRequest, client: df.models.DurableOrchestrationClient
) -> func.HttpResponse:
    return await HttpTrigger(client, "weather_data_flow_orchestrator", req).start()


@shared_bp.timer_trigger(arg_name="timer", schedule="0 30 6 * * *", run_on_startup=False)
@shared_bp.durable_client_input(client_name="client")
async def weather_data_flow_time_trigger(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    return await TimerTrigger(client, "weather_data_flow_orchestrator", timer).start()
