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
from jobflow_remote.jobs.state import BatchState

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

    if not show_all:
        batch_processes = jc.get_batch_processes(worker)
        if not batch_processes or not any(wbc for wbc in batch_processes.values()):
            exit_with_warning_msg("No batch processes running")
        else:
            worker_running_jobs = {}
            if verbosity > 0:
                for worker_name in batch_processes:
                    worker_config = workers[worker_name]
                    host = worker_config.get_host()
                    host.connect()
                    remote_batch_manager = RemoteBatchManager(
                        host, worker_config.batch.jobs_handle_dir
                    )
                    worker_running_jobs[worker_name] = (
                        remote_batch_manager.get_running()
                    )
            batch_processes = [
                {
                    "batch_uid": batch_uid,
                    "process_id": process_id,
                    "worker": worker,
                }
                for worker, worker_batches in batch_processes.items()
                for process_id, batch_uid in worker_batches.items()
            ]
            running_jobs = []
            if verbosity > 0:
                running_jobs = [
                    [
                        (str(jid), str(jidx))
                        for jid, jidx, batch_uid in worker_running_jobs[batch["worker"]]
                        if batch_uid == batch["batch_uid"]
                    ]
                    for batch in batch_processes
                ]
            table = get_batch_processes_table(
                batch_processes=batch_processes,
                workers=workers,
                running_jobs=running_jobs,
                verbosity=verbosity,
            )

            out_console.print(table)
    else:
        if jc.batches is None:
            exit_with_warning_msg(
                "No batches collection defined for your project. "
                'You can get running batches without the "--all" option.'
            )
        batch_processes = jc.get_all_batches(
            batch_state=[BatchState.SUBMITTED, BatchState.RUNNING]
        )

        finished_batch_processes = jc.get_all_batches(
            batch_state=BatchState.FINISHED,
            max_batches_per_worker=max_batches,
            sort={"finished_on": -1},
        )

        batch_processes.extend(finished_batch_processes)
        if not batch_processes:
            exit_with_warning_msg("No batch processes")

        batches_jobs = []
        if verbosity > 0:
            for batch in batch_processes:
                batch_jobs = [
                    (jid, str(jidx))
                    for jid, jid_dict in batch.get("jobs", {}).items()
                    for jidx in jid_dict
                ]
                batches_jobs.append(batch_jobs)

        table = get_batch_processes_table(
            batch_processes=batch_processes,
            workers=workers,
            running_jobs=batches_jobs,
            verbosity=verbosity,
            status=True,
            title="Batches info",
        )

        out_console.print(table)
