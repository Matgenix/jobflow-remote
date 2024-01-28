import io
from pathlib import Path
from typing import Annotated, Optional

import typer
from monty.json import jsanitize
from monty.serialization import dumpfn
from qtoolkit.core.data_objects import QResources
from rich.pretty import pprint

from jobflow_remote import SETTINGS
from jobflow_remote.cli.formatting import format_job_info, get_job_info_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    break_lock_opt,
    days_opt,
    db_ids_opt,
    end_date_opt,
    flow_ids_opt,
    hours_opt,
    job_db_id_arg,
    job_ids_indexes_opt,
    job_index_arg,
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
)
from jobflow_remote.cli.utils import (
    SortOption,
    check_incompatible_opt,
    check_stopped_runner,
    execute_multi_jobs_cmd,
    exit_with_error_msg,
    get_config_manager,
    get_job_controller,
    get_job_db_ids,
    get_job_ids_indexes,
    get_start_date,
    loading_spinner,
    out_console,
    print_success_msg,
    str_to_dict,
)
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
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
    metadata_dict = str_to_dict(metadata)

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    sort = [(sort.value, 1 if reverse_sort else -1)]

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
        if any(ji.remote.retry_time_limit is not None for ji in jobs_info):
            text = (
                "Some jobs (state in red) have failed while interacting with"
                " the worker, but will be retried again.\nGet more information about"
                " the error with 'jf job info JOB_ID'"
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
            help="DEPRECATED: not needed anymore to fetch errors",
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
    verbosity: verbosity_opt = 0,
):
    """
    Detail information on a specific job
    """

    db_id, job_id = get_job_db_ids(job_db_id, job_index)

    if with_error:
        out_console.print(
            "The --with-error option is deprecated and not needed anymore to show error messages",
            style="yellow",
        )

    with loading_spinner():
        jc = get_job_controller()

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

    if not job_data:
        exit_with_error_msg("No data matching the request")

    out_console.print(format_job_info(job_data, verbosity, show_none=show_none))


@app_job.command()
def set_state(
    state: job_state_arg,
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    Sets the state of a Job to an arbitrary value.
    WARNING: No checks. This can lead to inconsistencies in the DB. Use with care
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
        exit_with_error_msg("Could not reset the remote attempts")

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
):
    """
    Rerun a Job. By default, this is limited to jobs that failed and children did
    not start or jobs that are running. The rerun Job is set to READY and children
    Jobs to WAITING. If possible, the associated job submitted to the remote queue
    will be cancelled. Most of the limitations can be overridden by the 'force'
    option. This could lead to inconsistencies in the overall state of the Jobs of
    the Flow.
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
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        verbosity=verbosity,
        wait=wait,
        break_lock=break_lock,
        force=force,
        raise_on_error=raise_on_error,
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
    raise_on_error: raise_on_error_opt = False,
):
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
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Pause a Job. Only READY and WAITING Jobs can be paused. The operation is reversible.
    """

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.pause_job,
        multi_cmd=jc.pause_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Resume a Job that was previously PAUSED.
    """

    jc = get_job_controller()

    execute_multi_jobs_cmd(
        single_cmd=jc.play_job,
        multi_cmd=jc.play_jobs,
        job_db_id=job_db_id,
        job_index=job_index,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
    raise_on_error: raise_on_error_opt = False,
):
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
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        verbosity=verbosity,
        wait=wait,
        break_lock=break_lock,
        raise_on_error=raise_on_error,
    )


@app_job.command()
def queue_out(
    job_db_id: job_db_id_arg,
    job_index: job_index_arg = None,
):
    """
    Print the content of the output files produced by the queue manager.
    """

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


app_job_set = JFRTyper(
    name="set", help="Commands for managing the jobs", no_args_is_help=True
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the worker for the selected Jobs. Only READY or WAITING Jobs.
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
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the exec_config for the selected Jobs. Only READY or WAITING Jobs.
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
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        exec_config_value=exec_config_value,
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
            metavar="EXEC_CONFIG",
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
            help="If present the values will be interpreted as arguments for a QResources object",
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
    days: days_opt = None,
    hours: hours_opt = None,
    verbosity: verbosity_opt = 0,
    raise_on_error: raise_on_error_opt = False,
):
    """
    Set the resources for the selected Jobs. Only READY or WAITING Jobs.
    """

    resources_value = str_to_dict(resources_value)

    if qresources:
        resources_value = QResources(**resources_value)

    jc = get_job_controller()
    execute_multi_jobs_cmd(
        single_cmd=jc.set_job_run_properties,
        multi_cmd=jc.set_job_run_properties,
        job_db_id=None,
        job_index=None,
        job_ids=job_id,
        db_ids=db_id,
        flow_ids=flow_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
        name=name,
        metadata=metadata,
        days=days,
        hours=hours,
        verbosity=verbosity,
        raise_on_error=raise_on_error,
        resources=resources_value,
        update=not replace,
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
):
    """
    Dump to json the documents of the selected Jobs from the DB. For debugging.
    """

    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})
    metadata_dict = str_to_dict(metadata)

    job_ids_indexes = get_job_ids_indexes(job_id)

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    with loading_spinner():
        jobs_doc = jc.get_jobs_doc(
            job_ids=job_ids_indexes,
            db_ids=db_id,
            flow_ids=flow_id,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata_dict,
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
        Optional[str],
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
            "-",
            help="If enabled all the data from additional stores are also loaded ",
        ),
    ] = False,
):
    """
    Detail information on a specific job
    """

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
