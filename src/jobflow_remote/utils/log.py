"""Tools for logging."""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

from monty.os import makedirs_p


def initialize_runner_logger(
    log_folder: str | Path, level: int = logging.INFO, runner_id: str | None = None
):
    """
    Initialize the runner logger.

    Parameters
    ----------
    level
        The log level.
    """

    # If the directory it is not present it does not initialize the logger with
    # an unclear error message. Since this is only for the runner logger
    # it should not be a problem to check that the directory exists when the
    # runner is started.
    makedirs_p(log_folder)

    if runner_id:
        msg_format = f"%(asctime)s [%(levelname)s] ID {runner_id} %(name)s: %(message)s"
    else:
        msg_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {"format": msg_format},
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


def initialize_cli_logger(level: int = logging.WARNING, full_exc_info: bool = True):
    """
    Initialize the logger for the CLI based on rich.

    Parameters
    ----------
    level
        The log level.
    """

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "cli_formatter": {
                "()": lambda: CLIFormatter(
                    log_exception_trace=full_exc_info, datefmt="[%X]"
                ),
            }
        },
        "handlers": {
            "rich": {
                "class": "rich.logging.RichHandler",
                "level": level,
                "formatter": "cli_formatter",
                "show_path": False,
            },
        },
        "loggers": {
            "jobflow_remote": {  # root logger
                "handlers": ["rich"],
                "level": level,
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(config)


def initialize_remote_run_log(level: int = logging.INFO):
    """
    Initialize the logger for the execution of the jobs.

    Parameters
    ----------
    level
        The log level.
    """

    config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        },
        "handlers": {
            "stream": {
                "level": level,
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",  # Default is stderr
            },
        },
        "loggers": {
            "jobflow_remote": {  # root logger
                "handlers": ["stream"],
                "level": level,
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(config)


class CLIFormatter(logging.Formatter):
    def __init__(self, log_exception_trace: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.log_exception_trace = log_exception_trace

    def formatException(self, ei):
        if self.log_exception_trace:
            return super().formatException(ei)
        else:
            return f"{ei[0].__name__}: {str(ei[1])}"
