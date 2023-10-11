import io
from pathlib import Path

import typer
from typing_extensions import Annotated

from jobflow_remote import SETTINGS
from jobflow_remote.cli.formatting import format_job_info, get_job_info_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    days_opt,
    db_ids_opt,
    end_date_opt,
    flow_ids_opt,
    hours_opt,
    job_db_id_arg,
    job_ids_indexes_opt,
    job_index_arg,
    job_state_opt,
    locked_opt,
    max_results_opt,
    metadata_opt,
    name_opt,
    query_opt,
    remote_state_arg,
    remote_state_opt,
    reverse_sort_flag_opt,
    sort_opt,
    start_date_opt,
    verbosity_opt,
)
from jobflow_remote.cli.utils import (
    SortOption,
    check_incompatible_opt,
    convert_metadata,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_job_db_ids,
    get_job_ids_indexes,
    get_start_date,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.config import ConfigManager
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.state import RemoteState
from jobflow_remote.remote.queue import ERR_FNAME, OUT_FNAME

app_job = JFRTyper(
    name="job", help="Commands for managing the jobs", no_args_is_help=True
)
app.add_typer(app_job)


@app_job.command(name="list")
def jobs_list(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    remote_state: remote_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    max_results: max_results_opt = 100,
    sort: sort_opt = SortOption.UPDATED_ON.value,
    reverse_sort: reverse_sort_flag_opt = False,
    locked: locked_opt = False,
    custom_query: query_opt = None,
):
    """
    Get the list of Jobs in the database
    """
    check_incompatible_opt({"state": state, "remote-state": remote_state})
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
    metadata_dict = convert_metadata(metadata)

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = JobController()

    start_date = get_start_date(start_date, days, hours)

    sort = [(sort.query_field, 1 if reverse_sort else -1)]

    with loading_spinner():
        if custom_query:
            jobs_info = jc.get_jobs_info_query(
                query=custom_query,
                limit=max_results,
                sort=sort,
            )
        else:
            jobs_info = jc.get_jobs_info(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                flow_ids=flow_id,
                state=state,
                remote_state=remote_state,
                start_date=start_date,
                locked=locked,
                end_date=end_date,
                name=name,
                metadata=metadata_dict,
                limit=max_results,
                sort=sort,
            )

        table = get_job_info_table(jobs_info, verbosity=verbosity)

    out_console.print(table)
    if SETTINGS.cli_suggestions:
        if max_results and len(jobs_info) == max_results:
            out_console.print(
                f"The number of Jobs printed may be limited by the maximum selected: {max_results}",
                style="yellow",
            )
        if any(ji.retry_time_limit is not None for ji in jobs_info):
            text = (
                "Some jobs (remote state in red) have failed while interacting with"
                " the worker, but will be retried again.\nGet more information about"
                " the error with 'jf job info -err JOB_ID'"
            )
            out_console.print(text, style="yellow")


@app_job.command(name="info")
def job_info(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
    with_error: Annotated[
        bool,
        typer.Option(
            "--with-error",
            "-err",
            help="Also fetch and display information about errors",
        ),
    ] = False,
    show_none: Annotated[
        bool,
        typer.Option(
            "--show-none",
            "-n",
            help="Show the data whose values are None. Usually hidden",
        ),
    ] = False,
):
    """
    Detail information on a specific job
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = JobController()

        job_info = jc.get_job_info(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
            full=with_error,
        )
    if not job_info:
        exit_with_error_msg("No data matching the request")

    out_console.print(format_job_info(job_info, show_none=show_none))


@app_job.command()
def reset_failed(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    For a job with a FAILED remote state reset it to the previous state
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = JobController()

        succeeded = jc.reset_failed_state(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset failed state")

    print_success_msg()


@app_job.command()
def reset_remote_attempts(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    Resets the number of attempts to perform a remote action and eliminates
    the delay in retrying. This will not restore a Job from its failed state.
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = JobController()

        succeeded = jc.reset_remote_attempts(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset the remote attempts")

    print_success_msg()


@app_job.command()
def set_remote_state(
    state: remote_state_arg,
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    Sets the remote state to an arbitrary value.
    WARNING: this can lead to inconsistencies in the DB. Use with care
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = JobController()

        succeeded = jc.set_remote_state(
            state=state,
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset the remote attempts")

    print_success_msg()


@app_job.command()
def rerun(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    remote_state: remote_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
):
    """
    Rerun Jobs
    """
    check_incompatible_opt({"state": state, "remote-state": remote_state})

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = JobController()

    with loading_spinner():
        fw_ids = jc.rerun_jobs(
            job_ids=job_ids_indexes,
            db_ids=db_id,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
        )

    out_console.print(f"{len(fw_ids)} Jobs were rerun: {fw_ids}")


@app_job.command()
def queue_out(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    Print the content of the output files produced by the queue manager.
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving info...", total=None)
        jc = JobController()

        job_info = jc.get_job_info(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not job_info:
        exit_with_error_msg("No data matching the request")

    if job_info.remote_state not in (
        RemoteState.RUNNING,
        RemoteState.TERMINATED,
        RemoteState.DOWNLOADED,
        RemoteState.COMPLETED,
        RemoteState.FAILED,
    ):
        remote_state_str = (
            f"[{job_info.remote_state.value}]" if job_info.remote_state else ""
        )
        exit_with_warning_msg(
            f"The Job is in state {job_info.state.value}{remote_state_str} and the queue output will not be present"
        )

    remote_dir = job_info.run_dir

    out_path = Path(remote_dir, OUT_FNAME)
    err_path = Path(remote_dir, ERR_FNAME)
    out = None
    err = None
    out_error = None
    err_error = None
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving files...", total=None)
        cm = ConfigManager()
        worker = cm.get_worker(job_info.worker)
        host = worker.get_host()

        try:
            host.connect()
            try:
                out_bytes = io.BytesIO()
                host.get(out_path, out_bytes)
                out = out_bytes.getvalue().decode("utf-8")
            except Exception as e:
                out_error = getattr(e, "message", str(e))
            try:
                err_bytes = io.BytesIO()
                host.get(err_path, err_bytes)
                err = err_bytes.getvalue().decode("utf-8")
            except Exception as e:
                err_error = getattr(e, "message", str(e))
        finally:
            try:
                host.close()
            except Exception:
                pass

    if out_error:
        out_console.print(
            f"Error while fetching queue output from {str(out_path)}: {out_error}",
            style="red",
        )
    else:
        out_console.print(f"Queue output from {str(out_path)}:\n")
        out_console.print(out)

    if err_error:
        out_console.print(
            f"Error while fetching queue error from {str(err_path)}: {err_error}",
            style="red",
        )
    else:
        out_console.print(f"Queue error from {str(err_path)}:\n")
        out_console.print(err)
