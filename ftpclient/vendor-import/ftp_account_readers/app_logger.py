"""The package's single logging entry point. Never pass passwords or credential URLs in."""

import logging
import sys

_LOGGER_NAME = "ftp_account_readers"


class AppLogger:
    _configured = False

    @classmethod
    def configure(cls, level: str = "INFO") -> None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger = logging.getLogger(_LOGGER_NAME)
        logger.handlers = [handler]
        logger.setLevel(level.upper())
        logger.propagate = False
        cls._configured = True

    @classmethod
    def _logger(cls) -> logging.Logger:
        if not cls._configured:
            cls.configure()
        return logging.getLogger(_LOGGER_NAME)

    @classmethod
    def debug(cls, message: str) -> None:
        cls._logger().debug(message)

    @classmethod
    def info(cls, message: str) -> None:
        cls._logger().info(message)

    @classmethod
    def warning(cls, message: str) -> None:
        cls._logger().warning(f"WARNING: {message}")

    @classmethod
    def error(cls, message: str) -> None:
        cls._logger().error(f"ERROR: {message}")
