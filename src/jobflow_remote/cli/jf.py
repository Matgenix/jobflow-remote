import typer
from typing_extensions import Annotated

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import exit_with_error_msg
from jobflow_remote.config import ConfigManager

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

    if project:
        cm = ConfigManager()
        if project not in cm.projects_data:
            exit_with_error_msg(
                f"Project {project} is not defined in {SETTINGS.projects_folder}"
            )

        SETTINGS.project = project

    if full_exc:
        SETTINGS.cli_full_exc = True
