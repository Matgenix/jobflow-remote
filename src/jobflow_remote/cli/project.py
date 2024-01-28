from typing import Annotated

import typer
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import get_exec_config_table, get_worker_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import force_opt, serialize_file_format_opt, verbosity_opt
from jobflow_remote.cli.utils import (
    SerializeFileFormat,
    check_incompatible_opt,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.config import ConfigError, ConfigManager
from jobflow_remote.config.helper import (
    check_jobstore,
    check_queue_store,
    check_worker,
    generate_dummy_project,
)

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
):
    """
    List of available projects
    """
    cm = ConfigManager(warn=warn)

    project_name = None
    try:
        project_data = cm.get_project_data()
        project_name = project_data.project.name
    except ConfigError:
        pass

    full_project_list = cm.project_names_from_files()

    if not full_project_list:
        exit_with_warning_msg(f"No project available in {cm.projects_folder}")

    out_console.print(f"List of projects in {cm.projects_folder}")
    for pn in sorted(full_project_list):
        out_console.print(f" - {pn}", style="green" if pn == project_name else None)

    not_parsed_projects = set(full_project_list).difference(cm.projects_data.keys())
    if not_parsed_projects:
        out_console.print(
            "The following project names exist in files in the project folder, "
            "but could not properly parsed as projects: "
            f"{', '.join(not_parsed_projects)}.",
            style="yellow",
        )
        from jobflow_remote import SETTINGS

        if SETTINGS.cli_suggestions:
            out_console.print(
                "Run the command with -w option to see the parsing errors",
                style="yellow",
            )


@app_project.callback(invoke_without_command=True)
def current_project(ctx: typer.Context):
    """
    Print the list of the project currently selected
    """
    # only run if no other subcommand is executed
    if ctx.invoked_subcommand is None:
        out_console.print("Run 'jf project -h' to get the list of available commands")


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
):
    """
    Check that the connection to the different elements of the projects are working
    """
    check_incompatible_opt({"jobstore": jobstore, "queue": queue, "worker": worker})

    cm = ConfigManager(warn=True)
    project = cm.get_project()

    check_all = all(not v for v in (jobstore, worker, queue))

    workers_to_test = []
    if check_all:
        workers_to_test = project.workers.keys()
    elif worker:
        if worker not in project.workers:
            exit_with_error_msg(
                f"Worker {worker} does not exists in project {project.name}"
            )
        workers_to_test = [worker]

    tick = "[bold green]✓[/] "
    cross = "[bold red]x[/] "
    errors = []
    with loading_spinner(False) as progress:
        task_id = progress.add_task("Checking")
        for worker_name in workers_to_test:
            progress.update(task_id, description=f"Checking worker {worker_name}")
            worker = project.workers[worker_name]
            err = check_worker(worker)
            header = tick
            if err:
                errors.append((f"Worker {worker_name}", err))
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
    force: force_opt = False,
):
    """
    Remove a project from the projects' folder, including the related folders.
    """
    cm = get_config_manager()

    if name not in cm.projects_data:
        exit_with_warning_msg(f"Project {name} does not exist")

    p = cm.get_project(name)

    if not keep_folders and not force:
        msg = f"This will delete also the folders:\n\t{p.base_dir}\n\t{p.log_dir}\n\t{p.tmp_dir}\n\t{p.daemon_dir}\nProceed anyway?"
        if not Confirm.ask(msg):
            raise typer.Exit(0)

    with loading_spinner(False) as progress:
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
):
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
):
    cm = get_config_manager()
    project = cm.get_project()
    table = get_worker_table(project.workers, verbosity)
    out_console.print(table)
