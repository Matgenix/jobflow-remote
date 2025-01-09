from typing import Annotated, Optional

import typer
from packaging.version import parse as parse_version
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import format_upgrade_actions
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
    confirm_project_name,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    get_job_ids_indexes,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.data import DbCollection
from jobflow_remote.jobs.upgrade import DatabaseUpgrader

app_admin = JFRTyper(
    name="admin", help="Commands for administering the database", no_args_is_help=True
)
app.add_typer(app_admin)


@app_admin.command()
def upgrade(
    no_dry_run: Annotated[
        bool,
        typer.Option(
            "--no-dry-run",
            "-ndr",
            help="Skip the dry-run step",
        ),
    ] = False,
    check_env: Annotated[
        bool,
        typer.Option(
            "--check-env",
            "-ce",
            help="Report the changes in the package installed in the python environment before "
            "proceeding with the upgrade",
        ),
    ] = False,
    target: Annotated[
        Optional[str],
        typer.Option(
            "--target",
            "-t",
            help="Explicitly sets the target version for upgrade. For testing purposes or development"
            " versions. Not for standard updates.",
        ),
    ] = None,
) -> None:
    """
    Upgrade the jobflow database.
    WARNING: can modify all the data. Previous version cannot be retrieved anymore.
    It preferable to perform a backup before upgrading.
    """
    check_stopped_runner(error=True)

    jc = get_job_controller()
    upgrader = DatabaseUpgrader(jc)
    target_version = parse_version(target) if target else upgrader.current_version
    if target_version.local or target_version.post:
        # this is likely a developer version installed from source.
        # get the available upgrades for version higher than the developer one
        base_version = parse_version(target_version.base_version)
        upgrades_available = upgrader.collect_upgrades(
            base_version, upgrader.registered_upgrades[-1]
        )
        msg = (
            f"Target version {target_version} is likely a development version. Explicitly "
            f"specify the target version with the --target option if this is the case."
        )
        if upgrades_available:
            msg += (
                f" Available upgrades larger than {base_version}: "
                f"{','.join(str(v) for v in upgrades_available)}"
            )
        out_console.print(msg, style="gold1")
    db_version = jc.get_current_db_version()
    if db_version >= target_version:
        exit_with_warning_msg(
            f"Current DB version: {db_version}. No upgrade required for target version {target_version}"
        )

    jobflow_check = jc.upgrade_check_jobflow()
    if jobflow_check:
        out_console.print(jobflow_check)

    if check_env:
        full_check = jc.upgrade_full_check()
        if jobflow_check or full_check:
            text = Text.from_markup(
                +full_check
                + "This does not necessarily pose a problem. Just check if there is any potential "
                "incompatible upgrade"
            )
            out_console.print(text)

    if not no_dry_run:
        actions = upgrader.dry_run(target_version=target)
        if not actions:
            out_console.print(
                f"No actions will be required for upgrading to version {target_version}"
            )
        else:
            out_console.print(
                f"In order to upgrade the DB to version {target_version} the following actions will be performed:"
            )
            actions_formatted = format_upgrade_actions(actions)
            out_console.print(actions_formatted)

    warn_text = Text.from_markup(
        "[bold red]Performing the upgrade will permanently modify data in the queue DB. "
        "It is advisable to perform a backup before proceeding.[/]"
    )
    out_console.print(warn_text)
    confirm_project_name(style="bold red", exit_on_error=True)
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Upgrading the DB...", total=None)

        done = upgrader.upgrade(target_version=target)
    not_text = "" if done else "[bold]NOT [/bold]"
    out_console.print(f"The database has {not_text}been upgraded")


@app_admin.command()
def reset(
    validation: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                "If the number of flows in the DB exceed 25 it will be required to pass "
                "today's date in the YYYY-MM-DD to proceed with the reset"
            ),
            metavar="DATE",
        ),
    ] = None,
    reset_output: Annotated[
        bool,
        typer.Option(
            "--output",
            "-o",
            help="Also delete all the documents in the current store",
        ),
    ] = False,
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
        done = jc.reset(reset_output=reset_output, max_limit=25, validation=validation)
    not_text = "" if done else "[bold]NOT [/bold]"
    out_console.print(f"The database was {not_text}reset")
    if not done and SETTINGS.cli_suggestions:
        out_console.print(
            "Check the amount of Flows and set the DATE argument if this is the correct project to reset",
            style="yellow",
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
    If no criteria is specified all the locked jobs will be selected.
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
    If no criteria is specified all the locked flows will be selected.
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


@app_admin.command()
def unlock_runner() -> None:
    """
    Forcibly removes the lock from the runner document.
    WARNING: can lead to inconsistencies if a runner daemon is actually running.
    """
    jc = get_job_controller()

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Unlocking runner document...", total=None)

        num_docs, num_unlocked = jc.unlock_runner()

    if num_docs == 0:
        out_console.print(
            "No runner document... "
            "Consider upgrading your database using 'jf admin upgrade'"
        )
    elif num_unlocked == 0:
        out_console.print("The runner document was not locked. Nothing changed.")
    else:
        out_console.print("The runner document has been unlocked.")


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
