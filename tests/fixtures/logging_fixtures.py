import logging
import sys

import pytest

CONFIGURE_FILE_HANDLER = False


@pytest.fixture(scope="session", autouse=True)
def configure_logging() -> None:
    # pass
    # ...to ensure logging is configured before any tests run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    root_logger = logging.getLogger()

    if CONFIGURE_FILE_HANDLER:
        file_handler = logging.FileHandler(f"{__file__}.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root_logger.addHandler(console_handler)


@pytest.fixture(scope="session", autouse=True)
def logger() -> logging.Logger:
    # pass
    # ...to ensure logging is configured before any tests run
    logger = logging.getLogger("viva_api:pytest")
    logger.info("Logging is configured for the test session.")
    return logger
