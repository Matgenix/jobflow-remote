from itertools import islice
from typing import Annotated, Optional

import typer

from jobflow_remote.cli.formatting import get_batch_processes_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    max_batches_per_worker_opt,
    show_all_batches_opt,
    verbosity_opt,
)
from jobflow_remote.cli.utils import (
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    out_console,
)
from jobflow_remote.jobs.batch import RemoteBatchManager

app_batch = JFRTyper(
    name="batch", help="Helper utils handling batch jobs", no_args_is_help=True
)
app.add_typer(app_batch)


@app_batch.command(name="list")
def processes_list(
    worker: Annotated[
        Optional[str],
        typer.Option(
            "--worker",
            "-w",
            help="Select the worker.",
        ),
    ] = None,
    show_all: show_all_batches_opt = False,
    max_batches: max_batches_per_worker_opt = 10,
    verbosity: verbosity_opt = 0,
) -> None:
    """
    Show the list of processes being executed on the batch workers.
    Increasing verbosity will require connecting to the host.
    """

    jc = get_job_controller()

    cm = get_config_manager()
    project = cm.get_project()
    workers = project.workers

    batch_processes = jc.get_batch_processes(worker)
    if not batch_processes or not any(wbc for wbc in batch_processes.values()):
        if not show_all:
            exit_with_warning_msg("No batch processes running")
        out_console.print("No batch processes running", style="italic")
    else:
        running_jobs = {}
        if verbosity > 0:
            for worker_name in batch_processes:
                worker_config = workers[worker_name]
                host = worker_config.get_host()
                host.connect()
                remote_batch_manager = RemoteBatchManager(
                    host, worker_config.batch.jobs_handle_dir
                )
                running_jobs[worker_name] = remote_batch_manager.get_running()

        table = get_batch_processes_table(
            batch_processes=batch_processes,
            workers=workers,
            running_jobs=running_jobs,
            verbosity=verbosity,
        )

        out_console.print(table)

    if show_all:
        archived_batches = jc.get_archived_batch_processes(worker)
        if not archived_batches or not any(wb for wb in archived_batches.values()):
            out_console.print("No archived batch processes", style="italic")
            raise typer.Exit(0)
        if verbosity > 0:
            # TODO: Implement the gathering of the ids of the jobs that were run in this batch
            #  Questions ... some jobs may have started to run with a batch then go into remote
            #  error ... should it be mentioned there ? Or only those that completed ?
            verbosity = 0
        if max_batches:
            archived_batches = {
                wname: dict(islice(batches_data.items(), max_batches))
                for wname, batches_data in archived_batches.items()
            }
        table = get_batch_processes_table(
            batch_processes=archived_batches,
            workers=workers,
            running_jobs={},
            verbosity=verbosity,
            title="Archived batches info",
        )
        out_console.print(table)
