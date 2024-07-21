import traceback
from abc import ABC, abstractmethod
from typing import List, Optional

import azure.durable_functions as df
import azure.functions as func

from azfn_starter_kit.common.durables.core_entity import CoreEntity


class Trigger(CoreEntity, ABC):
    def __init__(self, client: df.DurableOrchestrationClient, orchestration_function_name: str) -> None:
        """
        Initialize a Trigger object.

        :param orchestration_function_name: The name of the orchestration function to trigger.
        """
        super().__init__()
        self.client = client
        self.orchestration_function_name = orchestration_function_name

    @abstractmethod
    async def start(self):
        pass 


class TimerTrigger(Trigger):
    def __init__(
        self, client: df.DurableOrchestrationClient, orchestration_function_name: str, timer_request: func.TimerRequest
    ) -> None:
        super().__init__(client, orchestration_function_name)
        self.timer_request = timer_request

    async def start(
        self,
        instance_id: Optional[str] = None,
        client_input: Optional[dict] = None,
    ) -> None:
        try:
            if self.timer_request.past_due:
                self.logger.info("The timer is past due!")
            else:
                instance_id = await self.client.start_new(
                    self.orchestration_function_name, instance_id=instance_id, client_input=client_input
                )
                self.logger.info("Started orchestration with ID = %s", instance_id)
        except Exception as catched_exception:  
            self.logger.error("Failed to start the orchestration: %s", catched_exception)


class InvalidBodyException(Exception):
    pass


class HttpTrigger(Trigger):
    def __init__(
        self,
        client: df.DurableOrchestrationClient,
        orchestration_function_name: str,
        http_request: func.HttpRequest,
        request_params: Optional[List[str]] = None,
    ) -> None:
        super().__init__(client, orchestration_function_name)
        self.http_request = http_request
        self.request_params = request_params

    def _validate_request(self) -> dict:
        """Validates the request and returns the parsed request body.

        Raises:
            ValueError: If there's no valid body in the request.
            InvalidBodyException: If the request body doesn't contain the required parameters.
        """
        if self.request_params is None:
            return {}

        request_body: dict = self.http_request.get_json()
        if any(param not in request_body.keys() for param in self.request_params):
            raise InvalidBodyException(f"Your request body doesn't contain {self.request_params} parameters")

        return request_body

    async def start(
        self,
        instance_id: Optional[str] = None,
    ) -> func.HttpResponse:
        try:
            request_body = self._validate_request()
        except ValueError:
            return func.HttpResponse("No body found in your request", status_code=400)
        except InvalidBodyException as invalid_body_exc:
            return func.HttpResponse(str(invalid_body_exc), status_code=400)

        try:
            instance_id = await self.client.start_new(
                self.orchestration_function_name, instance_id=instance_id, client_input=request_body
            )
            self.logger.info("Started orchestration with ID = '%s'.", instance_id)

            status: func.HttpResponse = self.client.create_check_status_response(self.http_request, instance_id)
            return status

        except Exception as catched_exc: 
            return func.HttpResponse(str(catched_exc) + ": \n" + traceback.format_exc(), status_code=400)
