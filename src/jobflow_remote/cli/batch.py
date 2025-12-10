from typing import Annotated

import typer
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import format_batch_info, get_batch_processes_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    batch_state_opt,
    max_results_opt,
    verbosity_opt,
    worker_name_opt,
    yes_opt,
)
from jobflow_remote.cli.utils import (
    check_valid_uuid,
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.jobs.state import BatchState

app_batch = JFRTyper(
    name="batch", help="Helper utils handling batch jobs", no_args_is_help=True
)
app.add_typer(app_batch)


@app_batch.command(name="list")
def processes_list(
    worker_name: worker_name_opt = None,
    max_results: max_results_opt = 20,
    batch_state: batch_state_opt = None,
    verbosity: verbosity_opt = 0,
) -> None:
    """
    Show the list of processes being executed on the batch workers.
    """

    jc = get_job_controller()

    cm = get_config_manager()
    project = cm.get_project()
    workers = project.workers

    with loading_spinner():
        batch_processes = jc.get_batches(
            worker=worker_name,
            batch_state=batch_state,
            limit=max_results,
        )
    if not batch_processes:
        exit_with_warning_msg("No batch processes")

    batches_jobs = []
    if verbosity > 0:
        for batch in batch_processes:
            batch_jobs = [
                (jid, str(jidx))
                for jid, jid_dict in batch.jobs.items()
                for jidx in jid_dict
            ]
            batches_jobs.append(batch_jobs)

    table = get_batch_processes_table(
        batch_processes=batch_processes,
        workers=workers,
        batches_jobs=batches_jobs,
        verbosity=verbosity,
        status=True,
        title="Batches info",
        job_ids_column_name="Job ids (Index)",
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
            limit=1,
        )

    if not batch_processes:
        exit_with_warning_msg("No batch process matching the request")

    worker = workers[batch_processes[0].worker]
    out_console.print(
        format_batch_info(batch_processes[0], worker=worker), overflow="crop"
    )


@app_batch.command()
def delete(
    # batch_state defined like this to avoid typing issues
    batch_state: Annotated[
        list[BatchState] | None,
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
            "-uid",
            help="One or more process ids",
        ),
    ] = None,
    worker_name: worker_name_opt = None,
    yes_all: yes_opt = False,
):
    """Remove one or more batch processes from the database. No effect on the processes running on the worker."""

    print(batch_state)
    if len(set(batch_state).difference([BatchState.FINISHED])) > 0 and not yes_all:
        text = Text.from_markup(
            "[red]This operation may remove batch processes in states other than 'FINISHED'. "
            "This could lead to [bold]inconsistencies or data loss[/bold]. Proceed anyway?[/red]"
        )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    jc = get_job_controller()

    with loading_spinner():
        n_batch_processes = jc.count_batches(
            process_id=process_id,
            batch_uid=batch_uid,
            worker=worker_name,
            batch_state=batch_state,
        )

    if not n_batch_processes:
        exit_with_warning_msg("No batch process matching the request")

    if not yes_all:
        text = Text.from_markup(
            f"[red]This operation will [bold]delete {n_batch_processes} batch processes[/bold]. Proceed anyway?[/red]"
        )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    with loading_spinner():
        n_deleted = jc.delete_batches(
            process_id=process_id,
            batch_uid=batch_uid,
            worker=worker_name,
            batch_state=batch_state,
        )

    print_success_msg(f"Operation completed. {n_deleted} batch processes deleted")
