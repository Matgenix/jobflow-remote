from __future__ import annotations

import typer
from rich.text import Text

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.utils import (
    exit_with_error_msg,
    exit_with_warning_msg,
    out_console,
)
from jobflow_remote.config import ConfigError, ConfigManager

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
