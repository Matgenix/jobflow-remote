from typing import Annotated

import typer

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import tree_opt
from jobflow_remote.cli.utils import (
    cleanup_job_controller,
    complete_profiling,
    initialize_config_manager,
    start_profiling,
)

app = JFRTyper(
    name="jf",
    add_completion=True,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog=None,  # to remove the default message in JFRTyper
)


ADDITIONAL_LOGGERS = []


def add_cli_logger_names(loggers: str | list[str]):
    # global ADDITIONAL_LOGGERS

    if isinstance(loggers, str):
        loggers = [loggers]

    ADDITIONAL_LOGGERS.extend(loggers)


def main_result_callback(*args, **kwargs) -> None:
    """
    Callback executed after the main command is completed successfully.
    Allowing to perform final actions. The cleanup of the shared resources is
    not done here, as this callback is not executed if the command fails.
    See the ``ctx.call_on_close`` registration in :func:`main`.
    """
    profile = kwargs.get("profile", False)
    if profile:
        complete_profiling()


@app.callback(result_callback=main_result_callback)
def main(
    ctx: typer.Context,
    project: Annotated[
        str,
        typer.Option(
            "--project",
            "-p",
            help="Select a project for the current execution",
            is_eager=True,
        ),
    ] = None,
    full_exc: Annotated[
        bool,
        typer.Option(
            "--full-exc",
            "-fe",
            help="Print the full stack trace of exception when enabled",
            is_eager=True,
        ),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(
            "--profile",
            "-prof",
            help="Profile the command execution and provide a report at the end. For developers",
            is_eager=True,
            hidden=True,
        ),
    ] = False,
    warn_project: Annotated[
        bool,
        typer.Option(
            "--warn-project",
            "-wp",
            help="Print warning messages regarding the parsing of the files in the projects folder.",
            is_eager=True,
        ),
    ] = False,
    print_tree: tree_opt = False,  # If selected will print the tree of the CLI and exit
) -> None:
    """The controller CLI for jobflow-remote."""
    from jobflow_remote import SETTINGS
    from jobflow_remote.utils.log import initialize_cli_logger

    # Close the shared JobController (if created) when the context is closed.
    # Unlike the result callback, this also happens if the command fails, so the
    # connections are always released and a controller is never reused by a
    # subsequent execution in the same process (e.g., in tests).
    ctx.call_on_close(cleanup_job_controller)

    if full_exc:
        SETTINGS.cli_full_exc = True

    if profile:
        start_profiling()

    initialize_cli_logger(
        level=SETTINGS.cli_log_level.to_logging(),
        full_exc_info=SETTINGS.cli_full_exc,
        loggers=ADDITIONAL_LOGGERS,
    )

    if project:
        SETTINGS.project = project

    # Only set the options of the shared ConfigManager. It will be created, and
    # the selected project printed, on first use. This avoids parsing the
    # configuration files for commands that do not need them (e.g. --help).
    initialize_config_manager(warn=warn_project)
