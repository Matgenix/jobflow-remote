import io
from datetime import datetime, timedelta
from pathlib import Path

import typer
from typing_extensions import Annotated

from jobflow_remote.cli.formatting import format_job_info, get_job_info_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.types import (
    days_opt,
    db_id_flag_opt,
    db_ids_opt,
    end_date_opt,
    job_id_arg,
    job_ids_opt,
    job_state_opt,
    locked_opt,
    max_results_opt,
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
    exit_with_error_msg,
    exit_with_warning_msg,
    get_job_db_ids,
    loading_spinner,
    out_console,
    print_success_msg,
)
from jobflow_remote.config import ConfigManager
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.state import RemoteState
from jobflow_remote.remote.queue import ERR_FNAME, OUT_FNAME

app_job = typer.Typer(
    name="job", help="Commands for managing the jobs", no_args_is_help=True
)
app.add_typer(app_job)


@app_job.command(name="list")
def jobs_list(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    state: job_state_opt = None,
    remote_state: remote_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    days: days_opt = None,
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
    check_incompatible_opt({"start_date": start_date, "days": days})
    check_incompatible_opt({"end_date": end_date, "days": days})

    jc = JobController()

    if days:
        start_date = datetime.now() - timedelta(days=days)

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
                job_ids=job_id,
                db_ids=db_id,
                state=state,
                remote_state=remote_state,
                start_date=start_date,
                locked=locked,
                end_date=end_date,
                limit=max_results,
                sort=sort,
            )

        table = get_job_info_table(jobs_info, verbosity=verbosity)

    if max_results and len(jobs_info) == max_results:
        out_console.print(
            f"The number of Jobs printed is limited by the maximum selected: {max_results}",
            style="yellow",
        )

    out_console.print(table)


@app_job.command(name="info")
def job_info(
    job_id: job_id_arg,
    db_id: db_id_flag_opt = False,
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

    with loading_spinner():

        jc = JobController()

        db_id_value, job_id_value = get_job_db_ids(db_id, job_id)

        job_info = jc.get_job_info(
            job_id=job_id_value,
            db_id=db_id_value,
            full=with_error,
        )
    if not job_info:
        exit_with_error_msg("No data matching the request")

    out_console.print(format_job_info(job_info, show_none=show_none))


@app_job.command()
def reset_failed(
    job_id: job_id_arg,
    db_id: db_id_flag_opt = False,
):
    """
    For a job with a FAILED remote state reset it to the previous state
    """
    with loading_spinner():
        jc = JobController()

        db_id_value, job_id_value = get_job_db_ids(db_id, job_id)

        succeeded = jc.reset_failed_state(
            job_id=job_id_value,
            db_id=db_id_value,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset failed state")

    print_success_msg()


@app_job.command()
def reset_remote_attempts(
    job_id: job_id_arg,
    db_id: db_id_flag_opt = False,
):
    """
    Resets the number of attempts to perform a remote action and eliminates
    the delay in retrying. This will not restore a Jon from its failed state.
    """
    with loading_spinner():
        jc = JobController()

        db_id_value, job_id_value = get_job_db_ids(db_id, job_id)

        succeeded = jc.reset_remote_attempts(
            job_id=job_id_value,
            db_id=db_id_value,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset the remote attempts")

    print_success_msg()


@app_job.command()
def set_remote_state(
    job_id: job_id_arg,
    state: remote_state_arg,
    db_id: db_id_flag_opt = False,
):
    """
    Sets the remote state to an arbitrary value.
    WARNING: this can lead to inconsistencies in the DB. Use with care
    """
    with loading_spinner():
        jc = JobController()

        db_id_value, job_id_value = get_job_db_ids(db_id, job_id)

        succeeded = jc.set_remote_state(
            state=state,
            job_id=job_id_value,
            db_id=db_id_value,
        )

    if not succeeded:
        exit_with_error_msg("Could not reset the remote attempts")

    print_success_msg()


@app_job.command()
def rerun(
    job_id: job_ids_opt = None,
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

    jc = JobController()

    with loading_spinner():
        fw_ids = jc.rerun_jobs(
            job_ids=job_id,
            db_ids=db_id,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
        )

    out_console.print(f"{len(fw_ids)} Jobs were rerun: {fw_ids}")


@app_job.command()
def queue_out(
    job_id: job_id_arg,
    db_id: db_id_flag_opt = False,
):
    with loading_spinner() as progress:
        progress.add_task(description="Retrieving info...", total=None)
        jc = JobController()

        db_id_value, job_id_value = get_job_db_ids(db_id, job_id)

        job_data_list = jc.get_jobs_data(
            job_ids=job_id_value,
            db_ids=db_id_value,
        )

    if not job_data_list:
        exit_with_error_msg("No data matching the request")

    job_data = job_data_list[0]
    info = job_data.info
    if info.remote_state not in (
        RemoteState.RUNNING,
        RemoteState.TERMINATED,
        RemoteState.DOWNLOADED,
        RemoteState.COMPLETED,
        RemoteState.FAILED,
    ):
        remote_state_str = f"[{info.remote_state.value}]" if info.remote_state else ""
        exit_with_warning_msg(
            f"The Job is in state {info.state.value}{remote_state_str} and the queue output will not be present"
        )

    remote_dir = info.run_dir

    out_path = Path(remote_dir, OUT_FNAME)
    err_path = Path(remote_dir, ERR_FNAME)
    out = None
    err = None
    out_error = None
    err_error = None
    with loading_spinner() as progress:
        progress.add_task(description="Retrieving files...", total=None)
        cm = ConfigManager()
        worker = cm.load_worker(info.worker)
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
            host.close()

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
