from __future__ import annotations

import functools
from contextlib import contextmanager
from enum import Enum

import typer
from click import ClickException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from jobflow_remote.config.base import ProjectUndefined

err_console = Console(stderr=True)
out_console = Console()


fmt_datetime = "%Y-%m-%d %H:%M"


class SortOption(Enum):
    CREATED_ON = "created_on"
    UPDATED_ON = "updated_on"
    DB_ID = "db_id"

    @property
    def query_field(self) -> str:
        if self == SortOption.DB_ID:
            return "fw_id"
        return self.value


class SerializeFileFormat(Enum):
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"


def exit_with_error_msg(message, code=1, **kwargs):
    kwargs.setdefault("style", "red")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def exit_with_warning_msg(message, code=0, **kwargs):
    kwargs.setdefault("style", "gold1")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def print_success_msg(message="operation completed", **kwargs):
    kwargs.setdefault("style", "green")
    out_console.print(message, **kwargs)


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


def get_job_db_ids(db_id, job_id):
    if db_id:
        try:
            db_id_value = int(job_id)
        except ValueError:
            raise typer.BadParameter(
                "if --db-id is selected the ID should be an integer"
            )
        job_id_value = None
    else:
        job_id_value = job_id
        db_id_value = None
    return db_id_value, job_id_value


def cli_error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (typer.Exit, typer.Abort, ClickException):
            raise  # Do not capture click or typer exceptions
        except ProjectUndefined:
            exit_with_error_msg(
                "The active project could not be determined and it is required to execute this command"
            )
        except Exception as e:
            from jobflow_remote import SETTINGS

            if SETTINGS.cli_full_exc:
                raise  # Reraise exceptions to print the full stacktrace
            else:
                exit_with_error_msg(
                    f"An Error occurred during the command execution: {e.__class__.__name__} {getattr(e, 'message', str(e))}"
                )

    return wrapper
