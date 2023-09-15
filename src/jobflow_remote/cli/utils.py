from __future__ import annotations

import functools
import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
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


class ReprStr(str):
    """
    Helper class that overrides the standard __repr__ to return the string itself
    and not its repr().
    Used mainly to allow printing of strings with newlines instead of '\n' when
    repr is used in rich.
    """

    def __repr__(self):
        return self


def exit_with_error_msg(message: str, code: int = 1, **kwargs):
    kwargs.setdefault("style", "red")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def exit_with_warning_msg(message: str, code: int = 0, **kwargs):
    kwargs.setdefault("style", "gold1")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def print_success_msg(message: str = "operation completed", **kwargs):
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


def get_job_db_ids(job_db_id: str, job_index: int | None):
    try:
        db_id = int(job_db_id)
        job_id = None
    except ValueError:
        db_id = None
        job_id = job_db_id
        check_valid_uuid(job_id)

    if job_index and db_id is not None:
        out_console.print(
            "The index is defined even if an integer is passed as an ID. Will be ignored",
            style="yellow",
        )

    return db_id, job_id


def get_job_ids_indexes(job_ids: list[str] | None) -> list[tuple[str, int]] | None:
    if not job_ids:
        return None
    job_ids_indexes = []
    for j in job_ids:
        split = j.split(":")
        if len(split) != 2 or not split[1].isnumeric():
            raise typer.BadParameter(
                "The job id should be in the format UUID:INDEX "
                "(e.g. e1d66c4f-81db-4fff-bda2-2bf1d79d5961:2). "
                f"Wrong format for {j}"
            )
        check_valid_uuid(split[0])
        job_ids_indexes.append((split[0], int(split[1])))

    return job_ids_indexes


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


def check_valid_uuid(uuid_str):
    try:
        uuid_obj = uuid.UUID(uuid_str)
        if str(uuid_obj) == uuid_str:
            return
    except ValueError:
        pass

    raise typer.BadParameter(f"UUID {uuid_str} is in the wrong format.")


def convert_metadata(string_metadata: str | None) -> dict | None:
    if not string_metadata:
        return None

    try:
        metadata = json.loads(string_metadata)
    except json.JSONDecodeError:
        split = string_metadata.split("=")
        if len(split) != 2:
            raise typer.BadParameter(f"Wrong format for metadata {string_metadata}")

        metadata = {split[0]: split[1]}

    return metadata


def get_start_date(start_date: datetime | None, days: int | None, hours: int | None):

    if start_date and (start_date.year, start_date.month, start_date.day) == (
        1900,
        1,
        1,
    ):
        now = datetime.now()
        start_date = start_date.replace(year=now.year, month=now.month, day=now.day)
    elif days:
        start_date = datetime.now() - timedelta(days=days)
    elif hours:
        start_date = datetime.now() - timedelta(hours=hours)

    return start_date
