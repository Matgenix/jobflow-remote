from typing import Annotated, Optional

import typer
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    db_ids_opt,
    end_date_opt,
    flow_ids_opt,
    flow_state_opt,
    force_opt,
    foreground_index_opt,
    index_direction_arg,
    index_key_arg,
    job_ids_indexes_opt,
    job_state_opt,
    start_date_opt,
)
from jobflow_remote.cli.utils import (
    IndexDirection,
    check_stopped_runner,
    exit_with_error_msg,
    get_config_manager,
    get_job_controller,
    get_job_ids_indexes,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.data import DbCollection

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
) -> None:
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
    with loading_spinner(processing=False) as progress:
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


@app_admin.command(hidden=True)
def remove_lock(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    force: force_opt = False,
) -> None:
    """
    DEPRECATED: use unlock instead
    Forcibly removes the lock from the documents of the selected jobs.
    WARNING: can lead to inconsistencies if the processes is actually running.
    """
    out_console.print(
        "remove-lock command has been DEPRECATED. Use unlock instead.",
        style="bold yellow",
    )

    unlock(
        job_id=job_id,
        db_id=db_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
        force=force,
    )


@app_admin.command()
def unlock(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    force: force_opt = False,
) -> None:
    """
    Forcibly removes the lock from the documents of the selected jobs.
    WARNING: can lead to inconsistencies if the processes is actually running.
    """
    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    if not force:
        with loading_spinner(processing=False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            jobs_info = jc.get_jobs_info(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                states=state,
                start_date=start_date,
                locked=True,
                end_date=end_date,
            )

        if not jobs_info:
            exit_with_error_msg("No data matching the request")

        text = Text.from_markup(
            f"[red]This operation will [bold]remove the lock[/bold] for (roughly) [bold]{len(jobs_info)} Job(s)[/bold]. Proceed anyway?[/red]"
        )
        confirmed = Confirm.ask(text, default=False)

        if not confirmed:
            raise typer.Exit(0)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Unlocking jobs...", total=None)

        num_unlocked = jc.unlock_jobs(
            job_ids=job_id,
            db_ids=db_id,
            states=state,
            start_date=start_date,
            end_date=end_date,
        )

    out_console.print(f"{num_unlocked} jobs were unlocked")


@app_admin.command()
def unlock_flow(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    force: force_opt = False,
) -> None:
    """
    Forcibly removes the lock from the documents of the selected jobs.
    WARNING: can lead to inconsistencies if the processes is actually running.
    """
    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    if not force:
        with loading_spinner(processing=False) as progress:
            progress.add_task(
                description="Checking the number of locked documents...", total=None
            )

            flows_info = jc.get_flows_info(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                flow_ids=flow_id,
                states=state,
                start_date=start_date,
                locked=True,
                end_date=end_date,
            )

        if not flows_info:
            exit_with_error_msg("No data matching the request")

        text = Text.from_markup(
            f"[red]This operation will [bold]remove the lock[/bold] for (roughly) [bold]{len(flows_info)} Flow(s)[/bold]. Proceed anyway?[/red]"
        )
        confirmed = Confirm.ask(text, default=False)

        if not confirmed:
            raise typer.Exit(0)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Unlocking flows...", total=None)

        num_unlocked = jc.unlock_flows(
            job_ids=job_id,
            db_ids=db_id,
            flow_ids=flow_id,
            states=state,
            start_date=start_date,
            end_date=end_date,
        )

    out_console.print(f"{num_unlocked} flows were unlocked")


app_index = JFRTyper(
    name="index",
    help="Commands for managing the indexes of the queue database",
    no_args_is_help=True,
)
app_admin.add_typer(app_index)


@app_index.command()
def rebuild(
    foreground: foreground_index_opt = False,
):
    """
    Rebuild all the standard indexes.
    Old indexes will be dropped, including the custom ones.
    """
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Resetting the indexes...", total=None)
        jc = get_job_controller()
        jc.build_indexes(background=not foreground, drop=True)

    if foreground:
        out_console.print("Indexes rebuilt")
    else:
        out_console.print("Indexes rebuild started in background")


@app_index.command()
def create(
    key: index_key_arg,
    direction: index_direction_arg = IndexDirection.ASC,
    foreground: foreground_index_opt = False,
    collection: Annotated[
        Optional[DbCollection],
        typer.Option(
            "--collection",
            "-c",
            help="The collection where the index should be added",
        ),
    ] = DbCollection.JOBS.value,  # type: ignore[assignment]
    unique: Annotated[
        bool,
        typer.Option(
            "--unique",
            "-u",
            help="The index will be unique",
        ),
    ] = False,
):
    """
    Add an index to one of the queue collections
    """
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Resetting the indexes...", total=None)
        jc = get_job_controller()
        jc.create_indexes(
            indexes=[[(key, direction.as_pymongo)]],
            unique=unique,
            collection=collection,
            background=not foreground,
        )
    if foreground:
        out_console.print("Index created")
    else:
        out_console.print("Index creation started in background")
