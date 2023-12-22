from __future__ import annotations

import functools
import json
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Callable

import typer
from click import ClickException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote import ConfigManager, JobController
from jobflow_remote.config.base import ProjectUndefined
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
from jobflow_remote.jobs.state import JobState

if TYPE_CHECKING:
    from cProfile import Profile

logger = logging.getLogger(__name__)


err_console = Console(stderr=True)
out_console = Console()


fmt_datetime = "%Y-%m-%d %H:%M"


# shared instances of the config manager and job controller, to avoid parsing
# the files multiple times. Needs to be initialized with the
# initialize_config_manager function.
_shared_config_manager: ConfigManager | None = None
_shared_job_controller: JobController | None = None

_profiler: Profile | None = None


def initialize_config_manager(*args, **kwargs):
    global _shared_config_manager
    _shared_config_manager = ConfigManager(*args, **kwargs)


def get_config_manager() -> ConfigManager:
    global _shared_config_manager
    if not _shared_config_manager:
        raise RuntimeError("The shared config manager needs to be initialized")
    return _shared_config_manager


def get_job_controller():
    global _shared_job_controller
    if _shared_job_controller is None:
        cm = get_config_manager()
        jc = JobController.from_project(cm.get_project())
        _shared_job_controller = jc

    return _shared_job_controller


def cleanup_job_controller():
    global _shared_job_controller
    if _shared_job_controller is not None:
        _shared_job_controller.close()


def start_profiling():
    global _profiler
    from cProfile import Profile

    _profiler = Profile()
    _profiler.enable()


def complete_profiling():
    global _profiler

    _profiler.disable()
    import pstats

    stats = pstats.Stats(_profiler).sort_stats("cumtime")
    stats.print_stats()


class SortOption(Enum):
    CREATED_ON = "created_on"
    UPDATED_ON = "updated_on"
    DB_ID = "db_id"


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
    if check_valid_uuid(job_db_id, raise_on_error=False):
        db_id = None
        job_id = job_db_id
    else:
        db_id = job_db_id
        job_id = None

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


def check_valid_uuid(uuid_str, raise_on_error: bool = True) -> bool:
    try:
        uuid_obj = uuid.UUID(uuid_str)
        if str(uuid_obj) == uuid_str:
            return True
    except ValueError:
        pass

    if raise_on_error:
        raise typer.BadParameter(f"UUID {uuid_str} is in the wrong format.")
    else:
        return False


def str_to_dict(string: str | None) -> dict | None:
    if not string:
        return None

    try:
        dictionary = json.loads(string)
    except json.JSONDecodeError:
        dictionary = {}
        for chunk in string.split(","):
            split = chunk.split("=")
            if len(split) != 2:
                raise typer.BadParameter(
                    f"Wrong format for dictionary-like field {string}"
                )

            dictionary[split[0]] = split[1]

    return dictionary


def get_start_date(start_date: datetime | None, days: int | None, hours: int | None):
    if start_date and (start_date.year, start_date.month, start_date.day) == (
        1900,
        1,
        1,
    ):
        now = datetime.now()
        start_date = start_date.replace(year=now.year, month=now.month, day=now.day)
        if start_date > now:
            start_date = start_date - timedelta(days=1)
    elif days:
        start_date = datetime.now() - timedelta(days=days)
    elif hours:
        start_date = datetime.now() - timedelta(hours=hours)

    return start_date


def execute_multi_jobs_cmd(
    single_cmd: Callable,
    multi_cmd: Callable,
    job_db_id: str | None = None,
    job_index: int | None = None,
    job_ids: list[str] | None = None,
    db_ids: str | list[str] | None = None,
    flow_ids: str | list[str] | None = None,
    state: JobState | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    name: str | None = None,
    metadata: str | None = None,
    days: int | None = None,
    hours: int | None = None,
    verbosity: int = 0,
    raise_on_error: bool = False,
    **kwargs,
):
    query_values = [
        job_ids,
        db_ids,
        flow_ids,
        state,
        start_date,
        end_date,
        name,
        metadata,
        days,
        hours,
    ]
    try:
        if job_db_id is not None:
            if any(query_values):
                msg = "If job_db_id is defined all the other query options should be disabled"
                exit_with_error_msg(msg)
            db_id, job_id = get_job_db_ids(job_db_id, job_index)
            with loading_spinner():
                modified_ids = single_cmd(
                    job_id=job_id, job_index=job_index, db_id=db_id, **kwargs
                )
                if not modified_ids:
                    exit_with_error_msg("Could not perform the requested operation")
        else:
            check_incompatible_opt(
                {"start_date": start_date, "days": days, "hours": hours}
            )
            check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
            metadata_dict = str_to_dict(metadata)

            job_ids_indexes = get_job_ids_indexes(job_ids)
            start_date = get_start_date(start_date, days, hours)

            if not any(
                (
                    job_ids_indexes,
                    db_ids,
                    flow_ids,
                    state,
                    start_date,
                    end_date,
                    name,
                    metadata,
                )
            ):
                text = Text.from_markup(
                    "[yellow]No filter has been set. This will apply the change to all "
                    "the jobs in the DB. Proceed anyway?[/yellow]"
                )

                confirmed = Confirm.ask(text, default=False)
                if not confirmed:
                    raise typer.Exit(0)

            with loading_spinner():
                modified_ids = multi_cmd(
                    job_ids=job_ids_indexes,
                    db_ids=db_ids,
                    flow_ids=flow_ids,
                    state=state,
                    start_date=start_date,
                    end_date=end_date,
                    name=name,
                    metadata=metadata_dict,
                    raise_on_error=raise_on_error,
                    **kwargs,
                )

        if verbosity:
            print_success_msg(f"Operation completed. Modified jobs: {modified_ids}")
        else:
            print_success_msg(f"Operation completed: {len(modified_ids)} jobs modified")
    except Exception:
        logger.error("Error executing the operation", exc_info=True)


def check_stopped_runner(error: bool = True):
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    try:
        with loading_spinner(False) as progress:
            progress.add_task(description="Checking the Daemon status...", total=None)
            current_status = dm.check_status()

    except DaemonError as e:
        exit_with_error_msg(
            f"Error while checking the status of the daemon: {getattr(e, 'message', str(e))}"
        )
    if current_status not in (DaemonStatus.STOPPED, DaemonStatus.SHUT_DOWN):
        if error:
            exit_with_error_msg(
                f"The status of the daemon is {current_status.value}. "
                "The daemon should not be running while resetting the database"
            )
        else:
            text = Text.from_markup(
                "[red]The Runner is active. This operation may lead to "
                "inconsistencies in this case. Proceed anyway?[/red]"
            )

            confirmed = Confirm.ask(text, default=False)
            if not confirmed:
                raise typer.Exit(0)
