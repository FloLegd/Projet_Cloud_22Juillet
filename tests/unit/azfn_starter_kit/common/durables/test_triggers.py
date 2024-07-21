from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from azure.durable_functions import DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse, TimerRequest

from azfn_starter_kit.common.durables.triggers import HttpTrigger, InvalidBodyException, TimerTrigger


@pytest.fixture
def client_mock():
    return MagicMock(spec=DurableOrchestrationClient)


@pytest.fixture
def timer_request_mock():
    mock = MagicMock(spec=TimerRequest)
    mock.past_due = False
    return mock


@pytest.fixture
def http_request_mock():
    mock = MagicMock(spec=HttpRequest)
    mock.get_json.return_value = {"key": "value"}
    return mock


def test_timer_trigger_initialization(client_mock, timer_request_mock):
    timer_trigger = TimerTrigger(client_mock, "OrchestratorFunction", timer_request_mock)
    assert timer_trigger.client == client_mock
    assert timer_trigger.timer_request == timer_request_mock


@patch("logging.getLogger")
@pytest.mark.asyncio
async def test_timer_trigger_start_not_past_due(get_logger_mock, client_mock, timer_request_mock):
    logger_mock = get_logger_mock.return_value
    client_mock.start_new = AsyncMock(return_value="instance_id")
    timer_trigger = TimerTrigger(client_mock, "OrchestratorFunction", timer_request_mock)
    await timer_trigger.start()
    logger_mock.info.assert_called_with("Started orchestration with ID = %s", "instance_id")


@patch("logging.getLogger")
@pytest.mark.asyncio
async def test_timer_trigger_start_past_due(get_logger_mock, client_mock, timer_request_mock):
    logger_mock = get_logger_mock.return_value
    timer_request_mock.past_due = True
    timer_trigger = TimerTrigger(client_mock, "OrchestratorFunction", timer_request_mock)
    await timer_trigger.start()
    logger_mock.info.assert_called_once_with("The timer is past due!")


@patch("logging.getLogger")
@pytest.mark.asyncio
async def test_timer_trigger_start_failure(get_logger_mock, client_mock, timer_request_mock):
    client_mock = client_mock.return_value
    exc = Exception("Error!")
    client_mock.start_new.side_effect = exc
    logger_mock = get_logger_mock.return_value
    timer_trigger = TimerTrigger(client_mock, "OrchestratorFunction", timer_request_mock)

    await timer_trigger.start()

    expected_call = call("Failed to start the orchestration: %s", exc)
    actual_call = logger_mock.error.call_args
    assert actual_call == expected_call


def test_http_trigger_initialization(client_mock, http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["key"])
    assert http_trigger.client == client_mock
    assert http_trigger.http_request == http_request_mock
    assert "key" in http_trigger.request_params


@pytest.mark.asyncio
async def test_http_trigger_start_value_error(client_mock, http_request_mock):
    http_request_mock.get_json.side_effect = ValueError()
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["key"])
    response = await http_trigger.start()
    assert isinstance(response, HttpResponse)
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_http_trigger_start_invalid_body(client_mock, http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["missing_key"])
    client_mock.start_new = AsyncMock(side_effect=InvalidBodyException("Test Exception"))

    with (
        patch("azfn_starter_kit.common.durables.triggers.traceback.format_exc", return_value="Traceback"),
    ):
        response = await http_trigger.start()
        assert isinstance(response, HttpResponse)
        assert response.status_code == 400


def test_http_trigger_validate_request_success(http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["key"])
    assert http_trigger._validate_request() == {"key": "value"}


def test_http_trigger_validate_request_empty(http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, None)
    assert http_trigger._validate_request() == {}


def test_http_trigger_validate_request_missing_key(http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["missing_key"])
    with pytest.raises(InvalidBodyException) as exc_info:
        http_trigger._validate_request()
    assert "doesn't contain" in str(exc_info.value)


@patch("logging.getLogger")
@pytest.mark.asyncio
async def test_http_trigger_start_success(get_logger_mock, client_mock, http_request_mock):
    logger_mock = get_logger_mock.return_value
    client_mock.start_new = AsyncMock(return_value="instance_id")
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["key"])
    with (
        patch.object(
            http_trigger.client, "create_check_status_response", return_value=HttpResponse("OK", status_code=202)
        ) as mock_status_response,
    ):
        response = await http_trigger.start()
        assert response.status_code == 202
        logger_mock.info.assert_called_with("Started orchestration with ID = '%s'.", "instance_id")
        mock_status_response.assert_called_once()


@pytest.mark.asyncio
async def test_http_trigger_start_exception(client_mock, http_request_mock):
    http_trigger = HttpTrigger(client_mock, "OrchestratorFunction", http_request_mock, ["key"])
    client_mock.start_new = AsyncMock(side_effect=Exception("Test Exception"))

    with (
        patch("azfn_starter_kit.common.durables.triggers.traceback.format_exc", return_value="Traceback"),
    ):
        response = await http_trigger.start()
        assert response.status_code == 400
        assert "Test Exception" in response.get_body().decode()
        assert "Traceback" in response.get_body().decode()
