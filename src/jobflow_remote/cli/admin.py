from typing import Annotated

import typer
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    db_ids_opt,
    end_date_opt,
    force_opt,
    job_ids_indexes_opt,
    job_state_opt,
    start_date_opt,
)
from jobflow_remote.cli.utils import (
    check_stopped_runner,
    get_config_manager,
    get_job_controller,
    get_job_ids_indexes,
    loading_spinner,
    out_console,
)

app_admin = JFRTyper(
    name="admin", help="Commands for administering the database", no_args_is_help=True
)
app.add_typer(app_admin)


@app_admin.command()
def reset(
    reset_output: Annotated[
        bool,
        typer.Option(
            "--output",
            "-o",
            help="Also delete all the documents in the current store",
        ),
    ] = False,
    max_limit: Annotated[
        int,
        typer.Option(
            "--max",
            "-m",
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

    check_stopped_runner(error=True)

    if not force:
        cm = get_config_manager()
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
        jc = get_job_controller()
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
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    force: force_opt = False,
):
    """
    Forcibly removes the lock from the documents of the selected jobs.
    WARNING: can lead to inconsistencies if the processes is actually running
    """

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    if not force:
        with loading_spinner(False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            jobs_info = jc.get_jobs_info(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                state=state,
                start_date=start_date,
                locked=True,
                end_date=end_date,
            )

        text = Text.from_markup(
            f"[red]This operation will [bold]remove the lock[/bold] for (roughly) [bold]{len(jobs_info)} Job(s)[/bold]. Proceed anyway?[/red]"
        )
        confirmed = Confirm.ask(text, default=False)

        if not confirmed:
            raise typer.Exit(0)

        with loading_spinner(False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            num_unlocked = jc.remove_lock_job(
                job_ids=job_id,
                db_ids=db_id,
                state=state,
                start_date=start_date,
                end_date=end_date,
            )

        out_console.print(f"{num_unlocked} jobs were unlocked")
