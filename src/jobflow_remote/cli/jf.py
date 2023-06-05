import typer
from typing_extensions import Annotated

from jobflow_remote.cli.utils import exit_with_error_msg
from jobflow_remote.config import ConfigManager

app = typer.Typer(
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
    ] = None
):
    """
    The controller CLI for jobflow-remote
    """
    if project:
        from jobflow_remote import SETTINGS

        cm = ConfigManager()
        if project not in cm.projects_data:
            exit_with_error_msg(
                f"Project {project} is not defined in {SETTINGS.projects_folder}"
            )

        SETTINGS.project = project
