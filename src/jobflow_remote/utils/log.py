"""Tools for logging."""

import logging
import logging.config
from pathlib import Path


def initialize_runner_logger(log_folder: str | Path, level: int = logging.INFO):
    """Initialize the default logger.

    Parameters
    ----------
    level
        The log level.

    Returns
    -------
    Logger
        A logging instance with customized formatter and handlers.
    """

    # TODO expose other configuration options?

    # TODO if the directory it is not present it does not initialize the logger with
    # an unclear error message. It may be worth leaving this:
    # makedirs_p(log_folder)

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        },
        "handlers": {
            "default": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": level,
                "formatter": "standard",
                "filename": str(Path(log_folder) / "runner.log"),
                "mode": "a",
                "backupCount": 5,
                "encoding": "utf8",
                "maxBytes": 5000000,
            },
            "stream": {
                "level": level,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",  # Default is stderr
            },
        },
        "loggers": {
            "jobflow_remote": {  # root logger
                # 'handlers': ['default'],
                "handlers": ["default", "stream"],
                "level": level,
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(config)
