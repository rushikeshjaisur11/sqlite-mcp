import logging

from .config import LOG_FORMAT, LOG_LEVEL


def get_logger(name:str) -> logging.Logger:
    """Get a configured logger instance."""
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(LOG_LEVEL)
    return logger
