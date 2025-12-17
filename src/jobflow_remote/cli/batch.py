from typing import Annotated

import typer
from pydantic import ValidationError
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import format_batch_info, get_batch_processes_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    batch_state_opt,
    db_ids_opt,
    job_ids_indexes_opt,
    max_results_opt,
    verbosity_opt,
    worker_name_opt,
    yes_opt,
)
from jobflow_remote.cli.utils import (
    check_valid_uuid,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    get_job_ids_indexes,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.jobs.state import BatchState

app_batch = JFRTyper(
    name="batch", help="Helper utils handling batch jobs", no_args_is_help=True
)
app.add_typer(app_batch)


def _check_exception_dev_version(exc: ValidationError):
    """
    Helper function to handle the change in the "jobs" type in the BatchDoc object.

    Initially defined as dict was switched to a list. Users may have this in their DB
    if using development version.

    Parameters
    ----------
    exc
        The pydantic ValidationError exception to be verified.
    """
    # TODO consider removing this function in the future
    try:
        # capture all possible to avoid that if some keys are not present it
        # fails with a confusing message.
        val_errors = exc.errors()
        if (
            exc.error_count() == 1
            and val_errors[0]["loc"] == ("jobs",)
            and isinstance(val_errors[0]["input"], dict)
        ):
            exit_with_error_msg(
                "It seems that you have used a development version of jobflow-remote. The internal format"
                "of the batch jobs have changed before the release. Please run 'jf batch fix-batch-doc-jobs-dict'"
                " to upgrade the database content (note that this command is hidden from the help as it is "
                "only needed for this error)."
            )
    except Exception:
        pass


@app_batch.command(name="list")
def processes_list(
    worker_name: worker_name_opt = None,
    max_results: max_results_opt = 20,
    batch_state: batch_state_opt = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    verbosity: verbosity_opt = 0,
) -> None:
    """
    Show the list of processes being executed on the batch workers.
    """

    jc = get_job_controller()

    cm = get_config_manager()
    project = cm.get_project()
    workers = project.workers

    job_ids_indexes = get_job_ids_indexes(job_id)

    try:
        with loading_spinner():
            batch_processes = jc.get_batches(
                worker=worker_name,
                batch_state=batch_state,
                job_ids=job_ids_indexes,
                db_ids=db_id,
                limit=max_results,
            )
    except ValidationError as exc:
        _check_exception_dev_version(exc=exc)
        raise

    if not batch_processes:
        exit_with_warning_msg("No batch processes")

    table = get_batch_processes_table(
        batch_processes=batch_processes,
        workers=workers,
        verbosity=verbosity,
        status=True,
        title="Batches info",
    )

    out_console.print(table)


@app_batch.command(name="info")
def process_info(
    selected_id: Annotated[
        str,
        typer.Argument(
            help="The ID of the batch process. Can be the Process id (i.e. the one coming from the worker) or batch UID",
            metavar="ID",
        ),
    ],
):
    """Detailed information on a specific batch process."""

    process_id = batch_uid = None
    if check_valid_uuid(selected_id, raise_on_error=False):
        batch_uid = selected_id
    else:
        process_id = selected_id

    jc = get_job_controller()

    cm = get_config_manager()
    project = cm.get_project()
    workers = project.workers

    with loading_spinner():
        batch_processes = jc.get_batches(
            process_id=process_id,
            batch_uid=batch_uid,
            limit=2,
        )

    if not batch_processes:
        exit_with_warning_msg("No batch process matching the request")

    if len(batch_processes) > 1:
        exit_with_error_msg(
            "More than one document matches the selection criteria. User 'jf batch list' to identify a"
            "unique criteria for the batch process that you want to visualize"
        )

    worker = workers[batch_processes[0].worker]
    out_console.print(
        format_batch_info(batch_processes[0], worker=worker), overflow="crop"
    )


@app_batch.command()
def delete(
    # batch_state defined like this to avoid typing issues
    batch_state: Annotated[
        list[BatchState],
        typer.Option(
            "--state",
            "-s",
            help="One or more of the batch states",
            default_factory=lambda: [BatchState.FINISHED.value],
            show_default=f"{BatchState.FINISHED.value}",
        ),
    ],
    process_id: Annotated[
        str | None,
        typer.Option(
            "--process-id",
            "-pid",
            help="One or more process ids",
        ),
    ] = None,
    batch_uid: Annotated[
        str | None,
        typer.Option(
            "--batch-uid",
            "-bid",
            help="One or more batch unique ids",
        ),
    ] = None,
    worker_name: worker_name_opt = None,
    yes_all: yes_opt = False,
):
    """Remove one or more batch processes from the database. No effect on the processes running on the worker."""

    jc = get_job_controller()

    try:
        with loading_spinner():
            to_delete = jc.get_batches(
                process_id=process_id,
                batch_uid=batch_uid,
                worker=worker_name,
                batch_state=batch_state,
            )
    except ValidationError as exc:
        _check_exception_dev_version(exc)
        raise

    if not to_delete:
        exit_with_warning_msg("No batch process matching the request")

    if not yes_all:
        if len(set(batch_state).difference([BatchState.FINISHED])) > 0 and any(
            b.batch_state != BatchState.FINISHED for b in to_delete
        ):
            text = Text.from_markup(
                "[red]This operation may remove batch processes in states other than 'FINISHED'. "
                "This could lead to [bold]inconsistencies or data loss[/bold]. "
                "Overall it will [bold]delete {len(to_delete)} batch processes[/bold]. Proceed anyway?[/red]"
            )
        else:
            text = Text.from_markup(
                f"[red]This operation will [bold]delete {len(to_delete)} batch processes[/bold]. Proceed anyway?[/red]"
            )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    with loading_spinner():
        n_deleted = jc.delete_batches(
            process_id=[td.process_id for td in to_delete],
        )

    print_success_msg(f"Operation completed. {n_deleted} batch processes deleted")


@app_batch.command(hidden=True)
def fix_batch_doc_jobs_dict():
    jc = get_job_controller()

    n_fixed = jc.fix_batch_doc_jobs()

    out_console.print(f"{n_fixed} batch documents modified")
