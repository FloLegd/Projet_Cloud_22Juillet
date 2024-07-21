import logging

import pytest

from azfn_starter_kit.utilities.logger import get_logger


def test_get_logger_with_valid_name():
    logger = get_logger("test_logger")
    assert logging.getLogger("test_logger") == logger


def test_get_logger_with_existing_logger_name():
    logger = get_logger("test_logger")
    new_logger = get_logger("test_logger")
    assert logger == new_logger


@pytest.mark.parametrize(
    ("log_level", "expected_level"),
    [
        ("INFO", logging.INFO),
        ("DEBUG", logging.DEBUG),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_get_logger_with_level(log_level, expected_level):
    logger = get_logger("test_logger", log_level=log_level)
    assert logger.level == expected_level
