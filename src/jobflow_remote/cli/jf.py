import typer
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import exit_with_error_msg, out_console
from jobflow_remote.config import ConfigError, ConfigManager

app = JFRTyper(
    name="jf",
    add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


@app.callback()
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
):
    """
    The controller CLI for jobflow-remote
    """
    from jobflow_remote import SETTINGS

    cm = ConfigManager()
    if project:
        if project not in cm.projects_data:
            exit_with_error_msg(
                f"Project {project} is not defined in {SETTINGS.projects_folder}"
            )

        SETTINGS.project = project

    if full_exc:
        SETTINGS.cli_full_exc = True

    try:
        project_data = cm.get_project_data()
        text = Text.from_markup(
            f"The selected project is [green]{project_data.project.name}[/green] from config file [green]{project_data.filepath}[/green]"
        )
        out_console.print(text)
    except ConfigError as e:
        out_console.print(f"Current project could not be determined: {e}", style="red")
