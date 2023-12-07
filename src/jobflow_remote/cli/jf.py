import typer
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import (
    cleanup_job_controller,
    exit_with_error_msg,
    get_config_manager,
    initialize_config_manager,
    out_console,
)
from jobflow_remote.config import ConfigError
from jobflow_remote.utils.log import initialize_cli_logger

app = JFRTyper(
    name="jf",
    add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog=None,  # to remove the default message in JFRTyper
)


@app.callback(result_callback=cleanup_job_controller)
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
):
    """
    The controller CLI for jobflow-remote
    """
    from jobflow_remote import SETTINGS

    if full_exc:
        SETTINGS.cli_full_exc = True

    if profile:
        from cProfile import Profile

        profiler = Profile()
        profiler.enable()

    initialize_cli_logger(
        level=SETTINGS.cli_log_level.to_logging(), full_exc_info=SETTINGS.cli_full_exc
    )

    # initialize the ConfigManager only once, to avoid parsing the configuration
    # files multiple times when the command is executed.
    initialize_config_manager()
    cm = get_config_manager()
    if project:
        if project not in cm.projects_data:
            exit_with_error_msg(
                f"Project {project} is not defined in {SETTINGS.projects_folder}"
            )

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

    if profile:
        profiler.disable()
        import pstats

        stats = pstats.Stats(profiler).sort_stats("cumtime")
        stats.print_stats()
