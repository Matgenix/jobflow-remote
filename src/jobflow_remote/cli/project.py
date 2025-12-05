import difflib
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer
from rich.panel import Panel
from rich.prompt import Confirm
from rich.syntax import Syntax
from rich.text import Text

from jobflow_remote.cli.formatting import get_exec_config_table, get_worker_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    force_opt_deprecated,
    serialize_file_format_opt,
    tree_opt,
    verbosity_opt,
    yes_opt,
)
from jobflow_remote.cli.utils import (
    SerializeFileFormat,
    check_incompatible_opt,
    check_stopped_runner,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    hide_progress,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.config import ConfigError, ConfigManager, Project
from jobflow_remote.config.helper import (
    check_jobstore,
    check_queue_store,
    check_worker,
    generate_dummy_project,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

app_project = JFRTyper(
    name="project",
    help="Commands concerning the project definition",
    # no_args_is_help=True,
)
app.add_typer(app_project)


@app_project.command(name="list")
def list_projects(
    warn: Annotated[
        bool,
        typer.Option(
            "--warn",
            "-w",
            help="Print the warning for the files that could not be parsed",
        ),
    ] = False,
) -> None:
    """List of available projects."""
    cm = ConfigManager(warn=warn)

    project_name = None
    try:
        project_data = cm.get_project_data()
        project_name = project_data.project.name
    except ConfigError:
        pass

    full_project_list, erroneous_files = cm.project_names_from_files(
        suppress_warnings=True
    )

    if not full_project_list:
        exit_with_warning_msg(f"No project available in {cm.projects_folder}")

    out_console.print(f"List of projects in {cm.projects_folder}")
    for pn in sorted(full_project_list):
        out_console.print(f" - {pn}", style="green" if pn == project_name else None)

    not_parsed_projects = set(full_project_list).difference(cm.projects_data)
    if not_parsed_projects:
        out_console.print(
            "The following project names exist in files in the project folder, "
            "but could not properly parsed as projects: "
            f"{', '.join(not_parsed_projects)}.",
            style="yellow",
        )
    if erroneous_files:
        out_console.print(
            "The following files exist in the project folder, "
            "but could not properly parsed as projects: "
            f"{', '.join(erroneous_files)}.",
            style="yellow",
        )
    if (not_parsed_projects or erroneous_files) and not warn:
        from jobflow_remote import SETTINGS

        if SETTINGS.cli_suggestions:
            out_console.print(
                "Run the command with -w option to see the parsing errors",
                style="yellow",
            )


@app_project.callback(invoke_without_command=True)
def current_project(
    ctx: typer.Context,
    print_tree: tree_opt = False,  # If selected will print the tree of the CLI and exit
) -> None:
    """Print the list of the project currently selected."""
    # only run if no other subcommand is executed
    if ctx.invoked_subcommand is None:
        out_console.print("Run 'jf project -h' to get the list of available commands")


@app_project.command()
def generate(
    name: Annotated[str, typer.Argument(help="Name of the project")],
    file_format: serialize_file_format_opt = SerializeFileFormat.YAML,
    full: Annotated[
        bool,
        typer.Option(
            "--full",
            help="Generate a configuration file with all the fields and more elements",
        ),
    ] = False,
) -> None:
    """Generate a project configuration file with dummy elements to be edited manually."""
    cm = ConfigManager(exclude_unset=not full)
    if name in cm.projects_data:
        exit_with_error_msg(f"Project with name {name} already exists")

    filepath = cm.projects_folder / f"{name}.{file_format.value}"
    if filepath.exists():
        exit_with_error_msg(
            f"Project with name {name} does not exist, but file {filepath!s} does and will not be overwritten"
        )

    project = generate_dummy_project(name=name, full=full)
    cm.create_project(project, ext=file_format.value)
    print_success_msg(f"Configuration file for project {name} created in {filepath!s}")


@app_project.command()
def check(
    jobstore: Annotated[
        bool,
        typer.Option(
            "--jobstore",
            "-js",
            help="Only check the jobstore connection",
        ),
    ] = False,
    queue: Annotated[
        bool,
        typer.Option(
            "--queue",
            "-q",
            help="Only check the queue connection",
        ),
    ] = False,
    worker: Annotated[
        str,
        typer.Option(
            "--worker",
            "-w",
            help="Only check the connection for the selected worker",
        ),
    ] = None,
    print_errors: Annotated[
        bool,
        typer.Option(
            "--errors",
            "-e",
            help="Print the errors at the end of the checks",
        ),
    ] = False,
    full: Annotated[
        bool,
        typer.Option(
            "--full",
            "-f",
            help="Perform a full check",
        ),
    ] = False,
) -> None:
    """Check that the connection to the different elements of the projects are working."""
    check_incompatible_opt({"jobstore": jobstore, "queue": queue, "worker": worker})

    # Check environment variables starting with jfremote_ prefix
    import difflib
    import os

    from jobflow_remote import SETTINGS

    prefix = SETTINGS.model_config["env_prefix"]
    extra_vars = [
        k
        for k in os.environ
        if k.lower().startswith(prefix)
        and k[len(prefix) :].lower() not in SETTINGS.model_fields
    ]
    if extra_vars:
        out_console.print(
            "The following environment variables with the JFREMOTE_ prefix were found, "
            "but they don't match any recognized configuration variables and may be incorrect.:\n - "
        )
        out_console.print("\n - ".join(extra_vars))
        out_console.print(
            "\nCheck documentation of Jobflow-Remote for the available settings in "
            "https://matgenix.github.io/jobflow-remote/user/projectconf.html#general-settings-environment-variables\n"
        )
        suggestions = {}
        for ev in extra_vars:
            if close_matches := difflib.get_close_matches(
                ev[len(prefix) :].upper(),
                [f.upper() for f in SETTINGS.model_fields],
                n=1,
            ):
                suggestions[ev] = close_matches[0]
        if suggestions:
            out_console.print("Suggested environment variables:\n - ")
            out_console.print(
                "\n - ".join(
                    [
                        f"{ev} -> JFREMOTE_{suggestion}"
                        for ev, suggestion in suggestions.items()
                    ]
                )
            )

    cm = get_config_manager()
    project = cm.get_project()

    check_all = all(not v for v in (jobstore, worker, queue))

    workers_to_test: Iterable[str] = []
    if check_all:
        workers_to_test = project.workers
    elif worker:
        if worker not in project.workers:
            exit_with_error_msg(
                f"Worker {worker} does not exists in project {project.name}"
            )
        workers_to_test = [worker]

    # check that jobstore main Store and the queue Store do not share the same collection
    if (check_all or (jobstore and queue)) and (
        project.get_jobstore().docs_store == project.get_queue_store()
    ):
        msg_duplicated_stores = (
            "It seems that the main docs_store of the JobStore and the queue store point to the "
            "same database and collection. This will lead to errors. Choose different collection names."
        )

        out_console.print(msg_duplicated_stores, style="red bold")

    tick = "[bold green]✓[/] "
    tick_warn = "[bold yellow]✓[/] "
    cross = "[bold red]x[/] "
    errors = []
    with loading_spinner(processing=False) as progress:
        task_id = progress.add_task("Checking")
        for worker_name in workers_to_test:
            progress.update(task_id, description=f"Checking worker {worker_name}")
            worker_to_test = project.workers[worker_name]
            if worker_to_test.get_host().interactive_login:
                with hide_progress(progress):
                    err, worker_warn = check_worker(worker_to_test, full_check=full)
            else:
                err, worker_warn = check_worker(worker_to_test, full_check=full)
            header = tick
            # At the moment the check_worker should return either an error or a
            # warning. The code below also deals with the case where both are
            # returned in the future.
            if worker_warn:
                errors.append((f"Worker {worker_name} warning ", worker_warn))
                header = tick_warn
            if err:
                errors.append((f"Worker {worker_name} ", err))
                header = cross
            progress.print(Text.from_markup(header + f"Worker {worker_name}"))

        if check_all or jobstore:
            progress.update(task_id, description="Checking jobstore")
            err = check_jobstore(project.get_jobstore())
            header = tick
            if err:
                errors.append(("Jobstore", err))
                header = cross
            progress.print(Text.from_markup(header + "Jobstore"))

            if project.optional_jobstores:
                progress.update(task_id, description="Checking optional jobstores")
                for jobstore_name in project.optional_jobstores:
                    err = check_jobstore(project.get_jobstore(jobstore_name))
                    header = tick
                    if err:
                        errors.append((f"Jobstore {jobstore_name}", err))
                        header = cross
                    progress.print(
                        Text.from_markup(header + f"Optional jobstore {jobstore_name}")
                    )

        if check_all or queue:
            progress.update(task_id, description="Checking queue store")
            err = check_queue_store(project.get_queue_store())
            header = tick
            if err:
                errors.append(("Queue store", err))
                header = cross
            progress.print(Text.from_markup(header + "Queue store"))

    if print_errors and errors:
        out_console.print("Errors:", style="red bold")
        for e in errors:
            out_console.print(e[0], style="bold")
            out_console.print(e[1])


@app_project.command()
def remove(
    name: Annotated[str, typer.Argument(help="Name of the project")],
    keep_folders: Annotated[
        bool,
        typer.Option(
            "--keep-folders",
            "-k",
            help="Project related folders are not deleted",
        ),
    ] = False,
    yes_all: yes_opt = False,
    force_deprecated: force_opt_deprecated = False,
) -> None:
    """Remove a project from the projects' folder, including the related folders."""
    cm = get_config_manager()

    if name not in cm.projects_data:
        exit_with_warning_msg(f"Project {name} does not exist")

    p = cm.get_project(name)

    if not keep_folders and not yes_all:
        msg = f"This will delete also the folders:\n\t{p.base_dir}\n\t{p.log_dir}\n\t{p.tmp_dir}\n\t{p.daemon_dir}\nProceed anyway?"
        if not Confirm.ask(msg):
            raise typer.Exit(0)

    with loading_spinner(processing=False) as progress:
        progress.add_task("Deleting project")
        cm.remove_project(project_name=name, remove_folders=not keep_folders)


#####################################
# Exec config app
#####################################


app_exec_config = JFRTyper(
    name="exec_config",
    help="Commands concerning the Execution configurations",
    no_args_is_help=True,
)
app_project.add_typer(app_exec_config)


@app_exec_config.command(name="list")
def list_exec_config(
    verbosity: verbosity_opt = 0,
) -> None:
    """
    The list of defined Execution configurations
    """
    cm = get_config_manager()
    project = cm.get_project()
    table = get_exec_config_table(project.exec_config, verbosity)
    out_console.print(table)


#####################################
# Worker app
#####################################


app_worker = JFRTyper(
    name="worker",
    help="Commands concerning the workers",
    no_args_is_help=True,
)
app_project.add_typer(app_worker)


@app_worker.command(name="list")
def list_worker(
    verbosity: verbosity_opt = 0,
) -> None:
    """
    The list of defined workers
    """
    cm = get_config_manager()
    project = cm.get_project()
    table = get_worker_table(project.workers, verbosity)
    out_console.print(table)


#####################################
# Edit app
#####################################


app_edit = JFRTyper(
    name="edit",
    help="Edit the project files",
    no_args_is_help=True,
)
app_project.add_typer(app_edit)


@app_edit.command()
def replace(
    old_string: Annotated[str, typer.Argument(help="String to search for and replace")],
    new_string: Annotated[str, typer.Argument(help="String to replace with")],
    all_projects: Annotated[
        bool,
        typer.Option(
            "--all",
            "-a",
            help="Apply replacement to all project files",
        ),
    ] = False,
    yes_all: yes_opt = False,
    force_deprecated: force_opt_deprecated = False,
    no_backup: Annotated[
        bool,
        typer.Option(
            "--no-backup",
            "-nb",
            help="Avoid creating a backup copy of the project file",
        ),
    ] = False,
) -> None:
    """
    Replace a string in one or more project files.

    This command performs a text replacement in the YAML project files,
    replacing all instances of old_string with new_string.
    By default, creates a backup of the original file.
    """
    import json

    import tomlkit
    from ruamel.yaml import YAML

    cm = get_config_manager()

    # Determine which projects to process
    if all_projects:
        # sort to make it reproducible
        projects_to_process = sorted(cm.projects_data)

        if not projects_to_process:
            exit_with_error_msg("No valid project files found that can be parsed")

    else:
        project_data = cm.get_project_data()
        projects_to_process = [project_data.project.name]

        check_stopped_runner(error=True)

    modified_count = 0

    for project_name in projects_to_process:
        try:
            project_data = cm.get_project_data(project_name)
            filepath = Path(project_data.filepath)

            # Read the file as text
            original_content = filepath.read_text()

            modified_content = original_content.replace(old_string, new_string)

            if original_content != modified_content:
                # Show diff and ask for confirmation if not forced
                if not yes_all:
                    out_console.print(
                        f"\n[bold]File: {filepath.name} (Project: {project_name})[/bold]"
                    )
                    out_console.print(f"[dim]Path: {filepath}[/dim]\n")

                    diff_lines = list(
                        difflib.unified_diff(
                            original_content.splitlines(keepends=True),
                            modified_content.splitlines(keepends=True),
                            fromfile=f"{filepath.name} (original)",
                            tofile=f"{filepath.name} (modified)",
                        )
                    )

                    diff_text = "".join(diff_lines)
                    syntax = Syntax(
                        diff_text, "diff", theme="monokai", line_numbers=False
                    )
                    out_console.print(
                        Panel(
                            syntax,
                            title="[bold]Changes Preview[/bold]",
                            border_style="blue",
                        )
                    )

                    try:
                        if project_data.ext == "yaml":
                            model = YAML().load(modified_content)
                        elif project_data.ext == "json":
                            model = json.loads(modified_content)
                        elif project_data.ext == "toml":
                            model = tomlkit.parse(modified_content)
                        else:
                            out_console.print(
                                f"Unknown file format for project: {project_data.ext}"
                            )
                            continue
                        Project.model_validate(model)
                    except Exception as e:
                        out_console.print(
                            "[bold]WARNING: The modification to the project file will result in "
                            f"an invalid file/project [/bold]: {getattr(e, 'message', str(e))}",
                            style="red",
                        )

                    if not Confirm.ask(f"Apply these changes to {project_name}?"):
                        continue

                # create a backup of the project file before overwriting
                if not no_backup:
                    cm.backup_project(project_name)

                # Write back the modified content
                filepath.write_text(modified_content)
                modified_count += 1
                out_console.print(
                    f"  ✓ Modified: {project_name} ({filepath.name})", style="green"
                )
            else:
                out_console.print(
                    f"  - No changes: {project_name} ({filepath.name})", style="yellow"
                )

        except Exception as e:
            out_console.print(f"  ✗ Error processing {project_name}: {e}", style="red")

    if modified_count > 0:
        print_success_msg(
            f"Successfully modified {modified_count} project file(s). For the changes "
            "to be registered the runner of all the modified projects needs to be restarted"
        )
    if modified_count == 0:
        out_console.print("No replacements were made in any files", style="yellow")
