import logging
import os


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured for use in AWS environments."""
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    return logging.getLogger(name)
