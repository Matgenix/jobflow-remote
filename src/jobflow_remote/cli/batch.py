from jobflow_remote.cli.formatting import get_batch_processes_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    batch_state_opt,
    max_results_opt,
    verbosity_opt,
    worker_name_opt,
)
from jobflow_remote.cli.utils import (
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    out_console,
)

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
    Increasing verbosity will require connecting to the host.
    """

    jc = get_job_controller()

    cm = get_config_manager()
    project = cm.get_project()
    workers = project.workers

    batch_processes = jc.get_all_batches(
        worker=worker_name,
        batch_state=batch_state,
        max_results=max_results,
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
