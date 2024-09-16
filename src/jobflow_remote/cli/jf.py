from typing import Annotated, Optional

import typer
from rich.text import Text
from rich.tree import Tree

from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import (
    cleanup_job_controller,
    complete_profiling,
    find_subcommand,
    get_command_tree,
    get_config_manager,
    initialize_config_manager,
    out_console,
    start_profiling,
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
) -> None:
    """The controller CLI for jobflow-remote."""
    from jobflow_remote import SETTINGS

    if full_exc:
        SETTINGS.cli_full_exc = True

    if profile:
        start_profiling()

    initialize_cli_logger(
        level=SETTINGS.cli_log_level.to_logging(), full_exc_info=SETTINGS.cli_full_exc
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


@app.command()
def tree(
    ctx: typer.Context,
    start_path: Annotated[
        Optional[list[str]],
        typer.Argument(help="Path to the starting command. e.g. 'jf tree job set'"),
    ] = None,
    show_options: Annotated[
        bool,
        typer.Option(
            "--options",
            "-o",
            help="Show command options in the tree",
        ),
    ] = False,
    show_docs: Annotated[
        bool,
        typer.Option(
            "--docs",
            "-D",
            help="Show hidden commands",
        ),
    ] = False,
    show_hidden: Annotated[
        bool,
        typer.Option(
            "--hidden",
            "-h",
            help="Show hidden commands",
        ),
    ] = False,
    max_depth: Annotated[
        Optional[int],
        typer.Option(
            "--max-depth",
            "-d",
            help="Maximum depth of the tree to display",
        ),
    ] = None,
):
    """
    Display a tree representation of the CLI command structure.

    This command shows the structure of the CLI application as a tree, with options to customize the output.

    Args:
        ctx (typer.Context): The Typer context object.
        start_path (List[str]): Path to the starting command for the tree.
        show_options (bool): If True, show command options in the tree.
        show_docs (bool): If True, show documentation for commands and options.
        show_hidden (bool): If True, show hidden commands.
        max_depth (Optional[int]): Maximum depth of the tree to display.
    """
    # Get the top-level app
    main_app = ctx.find_root().command

    if start_path:
        start_command = find_subcommand(main_app, start_path)
        if start_command is None:
            typer.echo(f"Error: Command '{' '.join(start_path)}' not found", err=True)
            raise typer.Exit(code=1)
        tree_title = f"[bold red]{' '.join(start_path)}[/bold red]"
    else:
        start_command = main_app
        tree_title = "[bold red]CLI App[/bold red]"

    tree = Tree(tree_title)
    command_tree = get_command_tree(
        start_command, tree, show_options, show_docs, show_hidden, max_depth
    )

    out_console.print(command_tree)
