import typer
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.types import serialize_file_format_opt
from jobflow_remote.cli.utils import (
    SerializeFileFormat,
    exit_with_error_msg,
    exit_with_warning_msg,
    out_console,
    print_success_msg,
)
from jobflow_remote.config import ConfigError, ConfigManager
from jobflow_remote.config.helper import generate_dummy_project

app_config = typer.Typer(
    name="config",
    help="Commands concerning the configuration of jobflow remote execution",
    no_args_is_help=True,
)
app.add_typer(app_config)

app_project = typer.Typer(
    name="project",
    help="Commands concerning the project definition",
    no_args_is_help=True,
)
app_config.add_typer(app_project)


@app_project.command(name="list")
def list_projects():
    cm = ConfigManager()

    project_name = None
    try:
        project_data = cm.get_project_data()
        project_name = project_data.project.name
    except ConfigError:
        pass

    if not cm.projects_data:
        exit_with_warning_msg(f"No project available in {cm.projects_folder}")

    out_console.print(f"List of projects in {cm.projects_folder}")
    for pn in sorted(cm.projects_data.keys()):
        out_console.print(f" - {pn}", style="green" if pn == project_name else None)


@app_project.command(name="current")
def current_project():
    """
    Print the list of the project currently selected
    """
    cm = ConfigManager()

    try:
        project_data = cm.get_project_data()
        text = Text()
        text.append("The selected project is ")
        text.append(project_data.project.name, style="green")
        text.append(" from config file ")
        text.append(project_data.filepath, style="green")
        out_console.print(text)
    except ConfigError as e:
        exit_with_error_msg(f"Error loading the selected project: {e}")


@app_project.command()
def generate(
    name: Annotated[str, typer.Argument(help="Name of the project")],
    file_format: serialize_file_format_opt = SerializeFileFormat.YAML.value,
    full: Annotated[
        bool,
        typer.Option(
            "--full",
            help="Generate a configuration file with all the fields and more elements",
        ),
    ] = False,
):
    """
    Generate a project configuration file with dummy elements to be edited manually
    """

    cm = ConfigManager(exclude_unset=not full)
    if name in cm.projects_data:
        exit_with_error_msg(f"Project with name {name} already exists")

    filepath = cm.projects_folder / f"{name}.{file_format.value}"
    if filepath.exists():
        exit_with_error_msg(
            f"Project with name {name} does not exist, but file {str(filepath)} does and will not be overwritten"
        )

    project = generate_dummy_project(name=name, full=full)
    cm.create_project(project, ext=file_format.value)
    print_success_msg(
        f"Configuration file for project {name} created in {str(filepath)}"
    )
