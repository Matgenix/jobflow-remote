from __future__ import annotations

import logging
from contextlib import contextmanager
from enum import Enum

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

err_console = Console(stderr=True)
out_console = Console()


fmt_datetime = "%Y-%m-%d %H:%M"


class Verbosity(Enum):
    MINIMAL = "minimal"
    NORMAL = "normal"
    DETAILED = "detailed"
    DIAGNOSTIC = "diagnostic"

    def to_int(self) -> int:
        return {
            Verbosity.MINIMAL: 0,
            Verbosity.NORMAL: 10,
            Verbosity.DETAILED: 20,
            Verbosity.DIAGNOSTIC: 30,
        }[self]


class LogLevel(Enum):
    ERROR = "error"
    WARN = "warn"
    INFO = "info"
    DEBUG = "debug"

    def to_logging(self) -> int:
        return {
            LogLevel.ERROR: logging.ERROR,
            LogLevel.WARN: logging.WARN,
            LogLevel.INFO: logging.INFO,
            LogLevel.DEBUG: logging.DEBUG,
        }[self]


def exit_with_error_msg(message, code=1, **kwargs):
    kwargs.setdefault("style", "red")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def exit_with_warning_msg(message, code=0, **kwargs):
    kwargs.setdefault("style", "gold1")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def check_incompatible_opt(d: dict):
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) > 1:
        options_list = ", ".join(not_none)
        exit_with_error_msg(f"Options {options_list} are incompatible")


def check_at_least_one_opt(d: dict):
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) > 1:
        options_list = ", ".join(d.keys())
        exit_with_error_msg(
            f"At least one of the options {options_list} should be defined"
        )


def check_only_one_opt(d: dict):
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) != 1:
        options_list = ", ".join(d.keys())
        exit_with_error_msg(
            f"One and only one of the options {options_list} should be defined"
        )


@contextmanager
def loading_spinner(processing: bool = True):
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        if processing:
            progress.add_task(description="Processing...", total=None)
        yield progress
