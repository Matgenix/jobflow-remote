# ruff: noqa: PLW0602, PLW0603

from __future__ import annotations

import contextlib
import functools
import json
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Callable, NoReturn

import typer
from click import ClickException
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.text import Text, TextType
from rich.tree import Tree
from typer.core import TyperCommand, TyperGroup

from jobflow_remote import ConfigManager, JobController
from jobflow_remote.config.base import ProjectUndefinedError
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus

if TYPE_CHECKING:
    from cProfile import Profile

    from jobflow_remote.jobs.state import JobState

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


def initialize_config_manager(*args, **kwargs) -> None:
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


def cleanup_job_controller() -> None:
    global _shared_job_controller
    if _shared_job_controller is not None:
        _shared_job_controller.close()
        # set to None again, in case it needs to be used again in the same
        # execution (e.g., in tests)
        _shared_job_controller = None


def start_profiling() -> None:
    global _profiler
    from cProfile import Profile

    _profiler = Profile()
    _profiler.enable()


def complete_profiling() -> None:
    global _profiler

    _profiler.disable()
    import pstats

    stats = pstats.Stats(_profiler).sort_stats("cumtime")
    stats.print_stats()


class SortOption(str, Enum):
    CREATED_ON = "created_on"
    UPDATED_ON = "updated_on"
    DB_ID = "db_id"


class SerializeFileFormat(str, Enum):
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"


class IndexDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"

    @property
    def as_pymongo(self):
        import pymongo

        return {
            IndexDirection.ASC: pymongo.ASCENDING,
            IndexDirection.DESC: pymongo.DESCENDING,
        }[self]


class ReportInterval(str, Enum):
    HOURS = "hours"
    DAYS = "days"
    WEEKS = "weeks"
    MONTHS = "months"
    YEARS = "years"


class ReprStr(str):
    r"""
    Helper class that overrides the standard __repr__ to return the string itself
    and not its repr().
    Used mainly to allow printing of strings with newlines instead of '\n' when
    repr is used in rich.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return self


def exit_with_error_msg(message: TextType, code: int = 1, **kwargs) -> NoReturn:
    kwargs.setdefault("style", "red")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def exit_with_warning_msg(message: TextType, code: int = 0, **kwargs) -> NoReturn:
    kwargs.setdefault("style", "gold1")
    err_console.print(message, **kwargs)
    raise typer.Exit(code)


def print_success_msg(message: TextType = "operation completed", **kwargs) -> None:
    kwargs.setdefault("style", "green")
    out_console.print(message, **kwargs)


def check_incompatible_opt(d: dict) -> None:
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) > 1:
        options_list = ", ".join(not_none)
        exit_with_error_msg(f"Options {options_list} are incompatible")


def check_query_incompatibility(query, incompatible_options):
    if query and any(opt is not None for opt in incompatible_options):
        exit_with_error_msg(
            "The --query option is incompatible with all the other filtering options"
        )


def check_at_least_one_opt(d: dict) -> None:
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) > 1:
        options_list = ", ".join(d)
        exit_with_error_msg(
            f"At least one of the options {options_list} should be defined"
        )


def check_only_one_opt(d: dict) -> None:
    not_none = []
    for k, v in d.items():
        if v:
            not_none.append(k)

    if len(not_none) != 1:
        options_list = ", ".join(d)
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


@contextmanager
def hide_progress(progress: Progress):
    """
    Hide the progress bar or spinning icon if an input is required from the user.

    Adapted from a related github issue for rich:
    https://github.com/Textualize/rich/issues/1535#issuecomment-1745297594

    Parameters
    ----------
    progress
        The Progress object in use
    """
    transient = progress.live.transient  # save the old value
    progress.live.transient = True
    progress.stop()
    progress.live.transient = transient  # restore the old value
    try:
        yield
    finally:
        # make space for the progress to use so it doesn't overwrite any previous lines
        progress.start()


def get_job_db_ids(job_db_id: str, job_index: int | None):
    if check_valid_uuid(job_db_id, raise_on_error=False):
        db_id = None
        job_id = job_db_id
    else:
        db_id = job_db_id
        job_id = None

    if job_index and db_id is not None:
        out_console.print(
            "The index is defined while a db_id is passed as an ID. Will be ignored",
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
        except ProjectUndefinedError:
            exit_with_error_msg(
                "The active project could not be determined and it is required to execute this command"
            )
        except Exception as e:
            from jobflow_remote import SETTINGS

            if SETTINGS.cli_full_exc:
                raise  # Reraise exceptions to print the full stacktrace
            exit_with_error_msg(
                f"An Error occurred during the command execution: {type(e).__name__} {getattr(e, 'message', str(e))}"
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
    return False


def str_to_dict(string: str | None) -> dict | None:
    if not string:
        return None

    try:
        dictionary = json.loads(string)
    except json.JSONDecodeError as exc:
        dictionary = {}
        for chunk in string.split(","):
            split = chunk.split("=")
            if len(split) != 2:
                raise typer.BadParameter(
                    f"Wrong format for dictionary-like field {string}"
                ) from exc

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
    states: JobState | list[JobState] | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    name: str | None = None,
    metadata: dict | None = None,
    days: int | None = None,
    hours: int | None = None,
    workers: list[str] | None = None,
    custom_query: dict | None = None,
    verbosity: int = 0,
    raise_on_error: bool = False,
    interactive: bool = True,
    **kwargs,
) -> None:
    """
    Utility function to execute a command on a single job or on a set of jobs.
    Checks the query options, determine whether the command is single or multi
    and call the corresponding function.

    Parameters
    ----------
    single_cmd
        A function that takes the job_id and job_index as arguments and performs an
        action on a single job.
    multi_cmd
        A function that takes a set of standard arguments and performs an
        action on multiple jobs.
    job_db_id
        The job id or db_id of a job to be considered. If specified, all the other
        query options should be disabled.
    job_index
        The index of the job in the DB.
    job_ids
        A list of job ids to be considered.
    db_ids
        A list of db ids to be considered.
    flow_ids
        A list of flow ids to be considered.
    states
        The states of the jobs to be considered.
    start_date
        The start date of the jobs to be considered.
    end_date
        The end date of the jobs to be considered.
    name
        The name of the jobs to be considered.
    metadata
        The metadata of the jobs to be considered.
    days
        Set the start_date based on the number of past days.
    hours
        Set the start_date based on the number of past hours.
    workers
        Workers associated with the jobs to be considered.
    custom_query
        A custom query to be used to filter the jobs.
    verbosity
        The verbosity of the output.
    raise_on_error
        If True, an error will be raised if the operation fails.
    interactive
        If True, a spinner will be shown even if the operation is not interactive while
        the operation is being performed.
    **kwargs : dict
        Additional arguments to be passed to the single_cmd and multi_cmd functions.
    """
    query_values = [
        job_ids,
        db_ids,
        flow_ids,
        states,
        start_date,
        end_date,
        name,
        metadata,
        days,
        hours,
        workers,
        custom_query,
    ]
    try:
        if job_db_id is not None:
            if any(query_values):
                msg = "If job_db_id is defined all the other query options should be disabled"
                exit_with_error_msg(msg)
            db_id, job_id = get_job_db_ids(job_db_id, job_index)
            # if potentially interactive do not start the spinner.
            cm = get_config_manager()
            spinner_cm: contextlib.AbstractContextManager
            if interactive and cm.get_project().has_interactive_workers:
                spinner_cm = contextlib.nullcontext()
                out_console.print("Processing...")
            else:
                spinner_cm = loading_spinner()
            with spinner_cm:
                modified_ids = single_cmd(
                    job_id=job_id, job_index=job_index, db_id=db_id, **kwargs
                )
                if not isinstance(modified_ids, (list, tuple)):
                    modified_ids = [] if modified_ids is None else [modified_ids]
                if not modified_ids:
                    exit_with_error_msg("Could not perform the requested operation")
        else:
            check_incompatible_opt(
                {"start_date": start_date, "days": days, "hours": hours}
            )
            check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
            check_query_incompatibility(
                custom_query,
                [
                    job_ids,
                    db_ids,
                    flow_ids,
                    states,
                    start_date,
                    end_date,
                    name,
                    metadata,
                    days,
                    hours,
                    workers,
                ],
            )

            job_ids_indexes = get_job_ids_indexes(job_ids)
            start_date = get_start_date(start_date, days, hours)

            if not any(
                (
                    job_ids_indexes,
                    db_ids,
                    flow_ids,
                    states,
                    start_date,
                    end_date,
                    name,
                    metadata,
                    workers,
                    custom_query,
                )
            ):
                text = Text.from_markup(
                    "[yellow]No filter has been set. This will apply the change to all "
                    "the jobs in the DB. Proceed anyway?[/yellow]"
                )

                confirmed = Confirm.ask(text, default=False)
                if not confirmed:
                    raise typer.Exit(0)  # noqa: TRY301

            with loading_spinner():
                modified_ids = multi_cmd(
                    job_ids=job_ids_indexes,
                    db_ids=db_ids,
                    flow_ids=flow_ids,
                    states=states,
                    start_date=start_date,
                    end_date=end_date,
                    name=name,
                    metadata=metadata,
                    workers=workers,
                    custom_query=custom_query,
                    raise_on_error=raise_on_error,
                    **kwargs,
                )

        if verbosity:
            print_success_msg(f"Operation completed. Modified jobs: {modified_ids}")
        else:
            print_success_msg(f"Operation completed: {len(modified_ids)} jobs modified")
    except Exception:
        logger.exception("Error executing the operation")


def check_stopped_runner(error: bool = True) -> None:
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    try:
        with loading_spinner(processing=False) as progress:
            progress.add_task(description="Checking the Daemon status...", total=None)
            current_status = dm.check_status()

    except DaemonError as e:
        exit_with_error_msg(
            f"Error while checking the status of the daemon: {getattr(e, 'message', str(e))}"
        )

    # the current status is only local. Also check that the DB does not contain
    # a running_runner document that does not match the current machine.
    runner_stopped = False
    if current_status in (DaemonStatus.STOPPED, DaemonStatus.SHUT_DOWN):
        matching_error = dm.check_matching_runner(allow_missing=True)
        if matching_error:
            logger.warning(matching_error)
        else:
            runner_stopped = True
    if not runner_stopped:
        if error:
            exit_with_error_msg(
                f"The status of the daemon is {current_status.value}. "
                "The daemon should not be running while performing this operation"
            )
        else:
            text = Text.from_markup(
                "[red]The Runner is active. This operation may lead to "
                "inconsistencies in this case. Proceed anyway?[/red]"
            )

            confirmed = Confirm.ask(text, default=False)
            if not confirmed:
                raise typer.Exit(0)


def get_command_tree(
    app: TyperGroup,
    tree: Tree,
    show_options: bool,
    show_docs: bool,
    show_hidden: bool,
    max_depth: int | None,
    current_depth: int = 0,
    max_doc_lines: int | None = None,
) -> Tree:
    """
    Recursively build a tree representation of the command structure.

    Parameters
    ----------
    app
        The Typer app or command group to process.
    tree
        The Rich Tree object to add nodes to.
    show_options
        Whether to display command options.
    show_docs
        Whether to display command and option documentation.
    show_hidden
        Whether to display hidden commands.
    max_depth
        Maximum depth of the tree to display.
    current_depth
        Current depth in the tree (used for recursion).
    max_doc_lines
        If show_docs is True, the maximum number of lines showed from the
        documentation of each command/option.

    Returns
    -------
    Tree
        The populated Rich Tree object.
    """
    if max_depth is not None and current_depth >= max_depth:
        return tree

    for command_name, command in sorted(app.commands.items()):
        if not show_hidden and getattr(command, "hidden", False):
            continue

        command_str = f"[bold green]{command_name}[/bold green]"
        if (
            show_docs
            and isinstance(command, (TyperCommand, TyperGroup))
            and command.help
        ):
            command_str += f": {command.help}"
            if max_doc_lines:
                lines = command_str.splitlines()
                command_str = "\n".join(lines[:max_doc_lines])
                if len(lines) > max_doc_lines:
                    command_str += " ..."
        branch = tree.add(command_str)

        if isinstance(command, TyperGroup):
            get_command_tree(
                command,
                branch,
                show_options,
                show_docs,
                show_hidden,
                max_depth,
                current_depth + 1,
                max_doc_lines,
            )
        elif show_options:
            for param in command.params:
                param_str = f"[blue]{param.name}[/blue]"
                if param.default is not None:
                    default_value = param.default if param.default != "" else '""'
                    param_str += f" (default: {default_value})"
                if param.required:
                    param_str += " [red](required)[/red]"
                if show_docs and param.help:
                    param_str += f": {param.help}"
                if max_doc_lines:
                    lines = param_str.splitlines()
                    param_str = "\n".join(lines[:max_doc_lines])
                    if len(lines) > max_doc_lines:
                        param_str += " ..."
                branch.add(param_str)
    return tree


def tree_callback(
    ctx: typer.Context,
    value: bool,
):
    if value:
        main_app = ctx.command
        tree_title = f"[bold red]{ctx.command.name}[/bold red]"

        tree = Tree(tree_title)
        command_tree = get_command_tree(
            main_app,
            tree,
            show_options=False,
            show_docs=True,
            show_hidden=False,
            max_depth=None,
            max_doc_lines=1,
        )

        out_console.print(command_tree)
        raise typer.Exit(0)


def confirm_project_name(style: str | None = "red", exit_on_error: bool = True) -> bool:
    """
    Ask the user to insert the name of the current project to confirm that a specific operation
    should be performed.

    Parameters
    ----------
    style
        The style to use for the message. If None, no style is used.
    exit_on_error
        If True, exit the program with an error message if the user does not insert the correct
        project name. Otherwise, return False.

    Returns
    -------
    bool
        True if the user inserted the correct project name, False otherwise.
    """
    cm = get_config_manager()
    project = cm.get_project()
    msg = f"Insert the name of the project ({project.name}) to confirm that you want to proceed "
    if style:
        msg = f"[{style}]{msg}[/]"
    user_project_name = out_console.input(msg)
    if user_project_name.strip() != project.name:
        if exit_on_error:
            exit_with_error_msg("The input does not match the current project name")
        return False
    return True
