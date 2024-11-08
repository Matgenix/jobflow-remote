import io
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer
from dateutil.tz import tzlocal
from monty.json import jsanitize
from monty.serialization import dumpfn
from qtoolkit.core.data_objects import QResources
from rich.pretty import pprint

from jobflow_remote import SETTINGS
from jobflow_remote.cli.formatting import (
    format_job_info,
    get_job_info_table,
    get_job_report_components,
)
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    OptionalStr,
    break_lock_opt,
    days_opt,
    db_ids_opt,
    delete_all_opt,
    delete_files_opt,
    delete_output_opt,
    end_date_opt,
    flow_ids_opt,
    hours_opt,
    job_db_id_arg,
    job_ids_indexes_opt,
    job_index_arg,
    job_index_opt,
    job_state_arg,
    job_state_opt,
    locked_opt,
    max_results_opt,
    metadata_opt,
    name_opt,
    query_opt,
    raise_on_error_opt,
    reverse_sort_flag_opt,
    sort_opt,
    start_date_opt,
    verbosity_opt,
    wait_lock_opt,
    worker_name_opt,
)
from jobflow_remote.cli.utils import (
    ReportInterval,
    SortOption,
    check_incompatible_opt,
    check_query_incompatibility,
    check_stopped_runner,
    execute_multi_jobs_cmd,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    get_job_controller,
    get_job_db_ids,
    get_job_ids_indexes,
    get_start_date,
    hide_progress,
    loading_spinner,
    out_console,
    print_success_msg,
    str_to_dict,
)
from jobflow_remote.jobs.report import JobsReport
from jobflow_remote.jobs.state import JobState
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
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    worker_name: worker_name_opt = None,
    verbosity: verbosity_opt = 0,
    max_results: max_results_opt = 100,
    sort: sort_opt = SortOption.UPDATED_ON,
    reverse_sort: reverse_sort_flag_opt = False,
    locked: locked_opt = False,
    custom_query: query_opt = None,
    error: Annotated[
        bool,
        typer.Option(
            "--error",
            "-e",
            help="Select the jobs in FAILED and REMOTE_ERROR state. Incompatible with the --state option",
        ),
    ] = False,
):
    """
    Get the list of Jobs in the database
    """
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
    check_incompatible_opt({"state": state, "error": error})
    check_query_incompatibility(
        custom_query,
        [
            job_id,
            db_id,
            flow_id,
            state,
            start_date,
            end_date,
            name,
            metadata,
            days,
            hours,
            worker_name,
        ],
    )

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    db_sort: list[tuple[str, int]] = [(sort.value, 1 if reverse_sort else -1)]

    if error:
        state = [JobState.REMOTE_ERROR, JobState.FAILED]

    with loading_spinner():
        if custom_query:
            jobs_info = jc.get_jobs_info_query(
                query=custom_query,
                limit=max_results,
                sort=db_sort,
            )
        else:
            jobs_info = jc.get_jobs_info(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                flow_ids=flow_id,
                states=state,
                start_date=start_date,
                locked=locked,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=worker_name,
                limit=max_results,
                sort=db_sort,
            )

        table = get_job_info_table(jobs_info, verbosity=verbosity)

    out_console.print(table)
    if SETTINGS.cli_suggestions:
        if max_results and len(jobs_info) == max_results:
            out_console.print(
                f"The number of Jobs printed may be limited by the maximum selected: {max_results}",
                style="yellow",
            )
        remote_errors = False
        if any(
            ji.remote.retry_time_limit is not None and ji.state != JobState.REMOTE_ERROR
            for ji in jobs_info
        ):
            text = (
                "Some jobs (state in orange) have failed while interacting with"
                " the worker, but will be retried."
            )
            out_console.print(text, style="yellow")
            remote_errors = True
        if remote_errors or any(
            ji.state in (JobState.REMOTE_ERROR, JobState.FAILED) for ji in jobs_info
        ):
            text = "Get more information about the errors with 'jf job info JOB_ID'"
            out_console.print(text, style="yellow")


@app_job.command(name="info")
def job_info(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    pid: Annotated[
        str,
        typer.Option(
            "--pid",
            help="The process ID of the job in the queue system (e.g. Slurm job ID)"
            "The -v flag will be ignored if --pid is used.",
        ),
    ] = None,
    show_none: Annotated[
        bool,
        typer.Option(
            "--show-none",
            "-n",
            help="Show the data whose values are None. Usually hidden",
        ),
    ] = False,
    verbosity: verbosity_opt = 0,
) -> None:
    """Detailed information on a specific job."""
    if pid is not None and job_db_id is not None:
        raise typer.BadParameter("Cannot specify both job ID/index and process ID")

    with loading_spinner():
        jc = get_job_controller()

        if job_db_id is not None:
            db_id, job_id = get_job_db_ids(job_db_id, job_index)

            if verbosity > 0:
                job_data = jc.get_job_doc(
                    job_id=job_id,
                    job_index=job_index,
                    db_id=db_id,
                )
            else:
                job_data = jc.get_job_info(
                    job_id=job_id,
                    job_index=job_index,
                    db_id=db_id,
                )
        elif pid is not None:
            job_data = jc.get_job_info_by_pid(pid)
        else:
            raise typer.BadParameter("Must specify either job ID/index or process ID")

    if not job_data:
        exit_with_error_msg("No data matching the request")

    out_console.print(format_job_info(job_data, verbosity, show_none=show_none))


@app_job.command()
def set_state(
    state: job_state_arg,
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
) -> None:
    """
    Sets the state of a Job to an arbitrary value.
    WARNING: No checks. This can lead to inconsistencies in the DB. Use with care.
    """
    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = get_job_controller()

        succeeded = jc.set_job_state(
            state=state,
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not succeeded:
        exit_with_error_msg("Could not change the job state")

    print_success_msg()


@app_job.command()
def rerun(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help=(
                "Force the rerun even if some conditions would normally prevent it (e.g. "
                "state usually not allowed or children already executed). Can lead to "
                "inconsistencies. Advanced users."
            ),
        ),
    ] = False,
    raise_on_error: raise_on_error_opt = False,
    no_delete: Annotated[
        bool,
        typer.Option(
            "--no-delete",
            "-nd",
            help=("Skip the delete of the files on the worker."),
        ),
    ] = False,
) -> None:
    """
    Rerun a Job. By default, this is limited to jobs that failed and children did
    not start or jobs that are running. The rerun Job is set to READY and children
    Jobs to WAITING. If possible, the associated job submitted to the remote queue
    will be cancelled. Most of the limitations can be overridden by the 'force'
    option. This could lead to inconsistencies in the overall state of the Jobs of
    the Flow.
    All the folders of the Jobs whose state are modified will also be deleted on
    the worker.
    """
    if force or break_lock:
        check_stopped_runner(error=False)

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.rerun_job,
        multi_cmd=jc.rerun_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        workers=worker_name,
        custom_query=custom_query,
        days=days,
        hours=hours,
        verbosity=verbosity,
        wait=wait,
        break_lock=break_lock,
        force=force,
        raise_on_error=raise_on_error,
        interactive=True,
        delete_files=not no_delete,
    )


@app_job.command()
def retry(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
    raise_on_error: raise_on_error_opt = False,
) -> None:
    """
    Retry to perform the operation that failed for a job in a REMOTE_ERROR state
    or reset the number of attempts at remote action, in order to allow the
    runner to try it again immediately.
    """
    if break_lock:
        check_stopped_runner(error=False)

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.retry_job,
        multi_cmd=jc.retry_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        wait=wait,
        break_lock=break_lock,
        raise_on_error=raise_on_error,
    )


@app_job.command()
def pause(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    raise_on_error: raise_on_error_opt = False,
) -> None:
    """Pause a Job. Only READY and WAITING Jobs can be paused. The operation is reversible."""
    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.pause_job,
        multi_cmd=jc.pause_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        wait=wait,
        raise_on_error=raise_on_error,
    )


@app_job.command()
def play(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    raise_on_error: raise_on_error_opt = False,
) -> None:
    """Resume a Job that was previously PAUSED."""
    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.play_job,
        multi_cmd=jc.play_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        wait=wait,
        raise_on_error=raise_on_error,
    )


@app_job.command()
def stop(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
    raise_on_error: raise_on_error_opt = False,
) -> None:
    """
    Stop a Job. Only Jobs that did not complete or had an error can be stopped.
    The operation is irreversible.
    If possible, the associated job submitted to the remote queue will be cancelled.
    """
    if break_lock:
        check_stopped_runner(error=False)

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.stop_job,
        multi_cmd=jc.stop_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        wait=wait,
        break_lock=break_lock,
        raise_on_error=raise_on_error,
    )


@app_job.command()
def delete(
    job_db_id: job_db_id_arg = None,
    job_index: job_index_arg = None,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    raise_on_error: raise_on_error_opt = False,
    delete_output: delete_output_opt = False,
    delete_files: delete_files_opt = False,
    delete_all: delete_all_opt = False,
) -> None:
    """
    Delete Jobs individually. The Flow document will be updated accordingly but
    no consistency check is performed. The Flow may be left in an inconsistent state.
    For advanced users only.
    """

    if delete_all:
        delete_files = delete_output = True

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.delete_job,
        multi_cmd=jc.delete_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        wait=wait,
        raise_on_error=raise_on_error,
        interactive=True,
        delete_output=delete_output,
        delete_files=delete_files,
    )


@app_job.command()
def queue_out(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
) -> None:
    """Print the content of the output files produced by the queue manager."""
    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    cm = get_config_manager()

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving info...", total=None)
        jc = get_job_controller()

        job_info = jc.get_job_info(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not job_info:
        exit_with_error_msg("No data matching the request")

    remote_dir = job_info.run_dir

    if not remote_dir:
        exit_with_warning_msg("The remote folder has not been created yet")

    out_path = Path(remote_dir, OUT_FNAME)
    err_path = Path(remote_dir, ERR_FNAME)
    out = None
    err = None
    out_error = None
    err_error = None
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving files...", total=None)
        worker = cm.get_worker(job_info.worker)
        host = worker.get_host()

        try:
            if worker.get_host().interactive_login:
                with hide_progress(progress):
                    host.connect()
            else:
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
            f"Error while fetching queue output from {out_path!s}: {out_error}",
            style="red",
        )
    else:
        out_console.print(f"Queue output from {out_path!s}:\n")
        out_console.print(out)

    if err_error:
        out_console.print(
            f"Error while fetching queue error from {err_path!s}: {err_error}",
            style="red",
        )
    else:
        out_console.print(f"Queue error from {err_path!s}:\n")
        out_console.print(err)


@app_job.command()
def report(
    interval: Annotated[
        ReportInterval,
        typer.Argument(
            help="The interval of the trends for the report",
            metavar="INTERVAL",
        ),
    ] = ReportInterval.DAYS,
    num_intervals: Annotated[
        Optional[int],
        typer.Argument(
            help="The number of intervals to consider. Default depends on the interval type",
            metavar="NUM_INTERVALS",
        ),
    ] = None,
):
    """
    Generate a report about the Jobs in the database.
    """
    jc = get_job_controller()

    timezone = datetime.now(tzlocal()).tzname()

    jobs_report = JobsReport.generate_report(
        job_controller=jc,
        interval=interval.value,
        num_intervals=num_intervals,
        timezone=timezone,
    )
    out_console.print(*get_job_report_components(jobs_report))


app_job_set = JFRTyper(
    name="set", help="Commands for setting properties for jobs", no_args_is_help=True
)
app_job.add_typer(app_job_set)


@app_job_set.command()
def worker(
    worker_name: Annotated[
        str,
        typer.Argument(
            help="The name of the worker",
            metavar="WORKER",
        ),
    ],
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    select_worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the worker for the selected Jobs.
    Only Jobs not in an evolving state (e.g. CHECKED_OUT, UPLOADED, ...).
    """
    jc = get_job_controller()
    execute_multi_jobs_cmd(
        single_cmd=jc.set_job_run_properties,
        multi_cmd=jc.set_job_run_properties,
        job_db_id=None,
        job_index=None,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=select_worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        worker=worker_name,
    )


@app_job_set.command()
def exec_config(
    exec_config_value: Annotated[
        str,
        typer.Argument(
            help="The name of the exec_config",
            metavar="EXEC_CONFIG",
        ),
    ],
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the exec_config for the selected Jobs.
    Only Jobs not in an evolving state (e.g. CHECKED_OUT, UPLOADED, ...).
    """
    jc = get_job_controller()
    execute_multi_jobs_cmd(
        single_cmd=jc.set_job_run_properties,
        multi_cmd=jc.set_job_run_properties,
        job_db_id=None,
        job_index=None,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        exec_config=exec_config_value,
    )


@app_job_set.command()
def resources(
    resources_value: Annotated[
        str,
        typer.Argument(
            help="The resources to be specified. Can be either a list of"
            "comma separated key=value pairs or a string with the JSON "
            "representation of a dictionary "
            '(e.g \'{"key1.key2": 1, "key3": "test"}\')',
            metavar="RESOURCES",
        ),
    ],
    replace: Annotated[
        bool,
        typer.Option(
            "--replace",
            "-r",
            help="If present the value will replace entirely those present "
            "instead of updating the DB, otherwise only the selected keys "
            "will be updated",
        ),
    ] = False,
    qresources: Annotated[
        bool,
        typer.Option(
            "--qresources",
            "-qr",
            help="If present the values in `resources_value` will be interpreted as arguments for a QResources object",
        ),
    ] = False,
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the resources for the selected Jobs.
    Only Jobs not in an evolving state (e.g. CHECKED_OUT, UPLOADED, ...)
    """
    resources = str_to_dict(resources_value)

    if qresources:
        resources = QResources(**resources)

    jc = get_job_controller()
    execute_multi_jobs_cmd(
        single_cmd=jc.set_job_run_properties,
        multi_cmd=jc.set_job_run_properties,
        job_db_id=None,
        job_index=None,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        resources=resources,
        update=not replace,
    )


@app_job_set.command()
def priority(
    priority_value: Annotated[
        int,
        typer.Argument(
            help="The priority of the Job",
            metavar="PRIORITY",
        ),
    ],
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the priority for the selected Jobs.
    Only Jobs not in an evolving state (e.g. CHECKED_OUT, UPLOADED, ...).
    """
    jc = get_job_controller()
    execute_multi_jobs_cmd(
        single_cmd=jc.set_job_run_properties,
        multi_cmd=jc.set_job_run_properties,
        job_db_id=None,
        job_index=None,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        states=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        workers=worker_name,
        custom_query=custom_query,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        priority=priority_value,
    )


@app_job.command(name="dump", hidden=True)
def job_dump(
    job_id: job_ids_indexes_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: job_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    metadata: metadata_opt = None,
    worker_name: worker_name_opt = None,
    custom_query: query_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    file_path: Annotated[
        str,
        typer.Option(
            "--path",
            "-p",
            help="Path to where the file should be dumped",
        ),
    ] = "jobs_dump.json",
) -> None:
    """Dump to json the documents of the selected Jobs from the DB. For debugging."""
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
    check_query_incompatibility(
        custom_query,
        [
            job_id,
            db_id,
            flow_id,
            state,
            start_date,
            end_date,
            name,
            metadata,
            days,
            hours,
            worker_name,
        ],
    )

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    with loading_spinner():
        if custom_query:
            jobs_doc = jc.get_jobs_doc_query(
                query=custom_query,
            )
        else:
            jobs_doc = jc.get_jobs_doc(
                job_ids=job_ids_indexes,
                db_ids=db_id,
                flow_ids=flow_id,
                states=state,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=worker_name,
            )
        if jobs_doc:
            dumpfn(jsanitize(jobs_doc, strict=True, enum_values=True), file_path)

    if not jobs_doc:
        exit_with_error_msg("No data matching the request")


@app_job.command()
def output(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
    file_path: Annotated[
        OptionalStr,
        typer.Option(
            "--path",
            "-p",
            help="If defined, the output will be dumped to this file based on the extension (json or yaml)",
        ),
    ] = None,
    load: Annotated[
        bool,
        typer.Option(
            "--load",
            "-l",
            help="If enabled all the data from additional stores are also loaded ",
        ),
    ] = False,
) -> None:
    """Fetch the output of a Job from the output Store."""
    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    with loading_spinner():
        jc = get_job_controller()

        if db_id:
            job_info = jc.get_job_info(
                job_id=job_id,
                job_index=job_index,
                db_id=db_id,
            )
            if job_info:
                job_id = job_info.uuid
                job_index = job_info.index

        job_output = None
        if job_id:
            job_output = jc.jobstore.get_output(job_id, job_index or "last", load=load)

    if not job_output:
        exit_with_error_msg("No data matching the request")

    if file_path:
        dumpfn(job_output, file_path)
    else:
        pprint(job_output)


app_job_files = JFRTyper(
    name="files",
    help="Commands for managing the files associated to a job",
    no_args_is_help=True,
)
app_job.add_typer(app_job_files)


@app_job_files.command(name="ls")
def files_list(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
) -> None:
    """List of files in the run_dir of the selected Job."""
    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    cm = get_config_manager()

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving info...", total=None)
        jc = get_job_controller()

        job_info = jc.get_job_info(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not job_info:
        exit_with_error_msg("No data matching the request")

    remote_dir = job_info.run_dir

    if not remote_dir:
        exit_with_warning_msg("The remote folder has not been created yet")

    remote_files = None
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving information...", total=None)
        worker = cm.get_worker(job_info.worker)
        host = worker.get_host()

        try:
            host.connect()
            try:
                remote_files = host.listdir(remote_dir)
            except Exception as e:
                raise RuntimeError(
                    f"Error while fetching the list of files from {remote_dir!s}: {getattr(e, 'message', str(e))}"
                ) from e
        finally:
            try:
                host.close()
            except Exception:
                pass

    out_console.print(f"List of files in {remote_dir}:")
    for fn in remote_files:
        out_console.print(fn)


@app_job_files.command(name="get")
def files_get(
    job_db_id: job_db_id_arg,
    filenames: Annotated[
        list[str],
        typer.Argument(
            help="A list of file names to be retrieved from the job run dir",
            metavar="FILE_NAMES",
        ),
    ],
    job_index: job_index_opt = None,
    path: Annotated[
        OptionalStr,
        typer.Option(
            "--path",
            "-p",
            help="If defined, the files will be copied to this path. Otherwise in the local folder.",
        ),
    ] = None,
) -> None:
    """
    Retrieve files from the Job's execution folder.
    """
    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    cm = get_config_manager()

    if not path:
        path = "."
    save_path = Path(path)
    if not save_path.is_dir():
        raise typer.BadParameter("The path is not an existing directory")

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Retrieving info...", total=None)
        jc = get_job_controller()

        job_info = jc.get_job_info(
            job_id=job_id,
            job_index=job_index,
            db_id=db_id,
        )

    if not job_info:
        exit_with_error_msg("No data matching the request")

    remote_dir = job_info.run_dir

    if not remote_dir:
        exit_with_warning_msg("The remote folder has not been created yet")

    with loading_spinner(processing=False) as progress:
        task_id = progress.add_task(description="Retrieving files...", total=None)
        worker = cm.get_worker(job_info.worker)
        host = worker.get_host()

        file_name: str
        try:
            host.connect()
            for file_name in filenames:
                progress.update(task_id, description=f"Retrieving {file_name}")
                host.get(str(Path(remote_dir) / file_name), str(save_path / file_name))
        except Exception as exc:
            raise RuntimeError(
                f"Error while fetching file {file_name} from {remote_dir!s}: {getattr(exc, 'message', str(exc))}"
            ) from exc
        finally:
            try:
                host.close()
            except Exception:
                pass
