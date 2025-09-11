from typing import Annotated, Union

import typer
from rich.text import Text

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import tree_opt
from jobflow_remote.cli.utils import (
    cleanup_job_controller,
    complete_profiling,
    get_config_manager,
    initialize_config_manager,
    out_console,
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


def add_cli_logger_names(loggers: Union[str, list[str]]):
    # global ADDITIONAL_LOGGERS

    if isinstance(loggers, str):
        loggers = [loggers]

    ADDITIONAL_LOGGERS.extend(loggers)


def main_result_callback(*args, **kwargs) -> None:
    """
    Callback executed after the main command is completed.
    Allowing to make cleanup and other final actions.
    """
    cleanup_job_controller()
    profile = kwargs.get("profile", False)
    if profile:
        complete_profiling()


@app.callback(result_callback=main_result_callback)
def main(
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
    print_tree: tree_opt = False,  # If selected will print the tree of the CLI and exit
) -> None:
    """The controller CLI for jobflow-remote."""
    from jobflow_remote import SETTINGS
    from jobflow_remote.config import ConfigError
    from jobflow_remote.utils.log import initialize_cli_logger

    if full_exc:
        SETTINGS.cli_full_exc = True

    if profile:
        start_profiling()

    initialize_cli_logger(
        level=SETTINGS.cli_log_level.to_logging(),
        full_exc_info=SETTINGS.cli_full_exc,
        loggers=ADDITIONAL_LOGGERS,
    )

    # initialize the ConfigManager only once, to avoid parsing the configuration
    # files multiple times when the command is executed.
    initialize_config_manager()
    cm = get_config_manager()
    if project:
        SETTINGS.project = project

    try:
        project_data = cm.get_project_data()
        text = Text.from_markup(
            f"The selected project is [green]{project_data.project.name}[/green] "
            f"from config file [green]{project_data.filepath}[/green]"
        )
        out_console.print(text)
    except ConfigError:
        # no warning printed if not needed as this seems to be confusing for the user
        pass
