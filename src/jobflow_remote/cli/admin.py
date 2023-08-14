import typer
from rich.prompt import Confirm
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    db_ids_opt,
    end_date_opt,
    force_opt,
    job_ids_opt,
    job_state_opt,
    remote_state_opt,
    start_date_opt,
)
from jobflow_remote.cli.utils import (
    check_incompatible_opt,
    exit_with_error_msg,
    loading_spinner,
    out_console,
)
from jobflow_remote.config import ConfigManager
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
from jobflow_remote.jobs.jobcontroller import JobController

app_admin = JFRTyper(
    name="admin", help="Commands for administering the database", no_args_is_help=True
)
app.add_typer(app_admin)


@app_admin.command()
def reset(
    reset_output: Annotated[
        bool,
        typer.Option(
            "--reset-output",
            "-o",
            help="Also delete all the documents in the current store",
        ),
    ] = False,
    max_limit: Annotated[
        int,
        typer.Option(
            "--max-limit",
            "-max",
            help=(
                "The database will be reset only if the number of Flows is lower than the specified limit. 0 means no limit"
            ),
        ),
    ] = 25,
    force: force_opt = False,
):
    """
    Reset the jobflow database.
    WARNING: deletes all the data. These could not be retrieved anymore.
    """
    from jobflow_remote import SETTINGS

    dm = DaemonManager()

    try:
        with loading_spinner(False) as progress:
            progress.add_task(description="Checking the Daemon status...", total=None)
            current_status = dm.check_status()

    except DaemonError as e:
        exit_with_error_msg(
            f"Error while checking the status of the daemon: {getattr(e, 'message', str(e))}"
        )

    if current_status not in (DaemonStatus.STOPPED, DaemonStatus.SHUT_DOWN):
        exit_with_error_msg(
            f"The status of the daemon is {current_status.value}. "
            "The daemon should not be running while resetting the database"
        )

    if not force:
        cm = ConfigManager()
        project_name = cm.get_project_data().project.name
        text = Text.from_markup(
            "[red]This operation will [bold]delete all the Jobs data[/bold] "
            f"for project [bold]{project_name}[/bold]. Proceed anyway?[/red]"
        )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)
    with loading_spinner(False) as progress:
        progress.add_task(description="Resetting the DB...", total=None)
        jc = JobController()
        done = jc.reset(reset_output=reset_output, max_limit=max_limit)
    not_text = "" if done else "[bold]NOT [/bold]"
    out_console.print(f"The database was {not_text}reset")
    if not done and SETTINGS.cli_suggestions:
        out_console.print(
            "Check the amount of Flows and change --max-limit if this is the correct project to reset",
            style="yellow",
        )


@app_admin.command()
def remove_lock(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    remote_state: remote_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    force: force_opt = False,
):
    """
    Forcibly removes the lock from the documents of the selected jobs.
    WARNING: can lead to inconsistencies if the processes is actually running
    """
    check_incompatible_opt({"state": state, "remote-state": remote_state})

    jc = JobController()
    if not force:
        with loading_spinner(False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            jobs_info = jc.get_jobs_info(
                job_ids=job_id,
                db_ids=db_id,
                state=state,
                remote_state=remote_state,
                start_date=start_date,
                locked=True,
                end_date=end_date,
            )

        text = Text(
            f"[red]This operation will [bold]remove the lock[/bold] for (roughly) [bold]{len(jobs_info)} Job(s)[/bold]. Proceed anyway?[/red]"
        )
        confirmed = Confirm.ask(text, default=False)

        if not confirmed:
            raise typer.Exit(0)

        with loading_spinner(False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            num_unlocked = jc.remove_lock(
                job_ids=job_id,
                db_ids=db_id,
                state=state,
                remote_state=remote_state,
                start_date=start_date,
                end_date=end_date,
            )

        out_console.print(f"{num_unlocked} jobs were unlocked")
