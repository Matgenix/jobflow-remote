import contextlib
from datetime import datetime
from typing import Annotated

import typer
from dateutil.tz import tzlocal
from jobflow.utils.graph import draw_graph
from rich.prompt import Confirm
from rich.text import Text

from jobflow_remote.cli.formatting import (
    format_flow_info,
    get_flow_info_table,
    get_flow_report_components,
    get_single_flow_report_components,
    header_name_data_getter_map,
)
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    break_lock_opt,
    cli_output_keys_opt,
    count_opt,
    days_opt,
    db_ids_opt,
    delete_all_opt,
    delete_files_opt,
    delete_output_opt,
    end_date_opt,
    flow_db_id_arg,
    flow_ids_opt,
    flow_state_opt,
    force_opt_deprecated,
    hours_opt,
    job_flow_id_flag_opt,
    job_ids_opt,
    locked_flow_opt,
    max_results_opt,
    metadata_opt,
    name_opt,
    reverse_sort_flag_opt,
    sort_opt,
    start_date_opt,
    stored_data_keys_opt,
    verbosity_opt,
    wait_lock_opt,
    yes_opt,
)
from jobflow_remote.cli.utils import (
    ReportInterval,
    SortOption,
    check_incompatible_opt,
    check_output_stored_data_keys,
    check_stopped_runner,
    exit_with_error_msg,
    exit_with_warning_msg,
    get_job_controller,
    get_job_db_ids,
    get_start_date,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.graph import get_graph, plot_dash
from jobflow_remote.jobs.report import FlowsReport
from jobflow_remote.jobs.state import JobState

app_flow = JFRTyper(
    name="flow", help="Commands for managing the flows", no_args_is_help=True
)
app.add_typer(app_flow)


@app_flow.command(name="list")
def flows_list(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    metadata: metadata_opt = None,
    locked: locked_flow_opt = False,
    verbosity: verbosity_opt = 0,
    count: count_opt = False,
    max_results: max_results_opt = 100,
    sort: sort_opt = SortOption.UPDATED_ON,
    reverse_sort: reverse_sort_flag_opt = False,
) -> None:
    """Get the list of Flows in the database."""
    from jobflow_remote import SETTINGS

    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    db_sort: list[tuple[str, int]] = [(sort.value, 1 if reverse_sort else -1)]

    if count:
        with loading_spinner():
            n_flows = jc.count_flows(
                job_ids=job_id,
                db_ids=db_id,
                flow_ids=flow_id,
                states=state,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                locked=locked,
            )
        out_console.print(f"Number of Flows: {n_flows}")
    else:
        with loading_spinner():
            flows_info = jc.get_flows_info(
                job_ids=job_id,
                db_ids=db_id,
                flow_ids=flow_id,
                states=state,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                locked=locked,
                limit=max_results,
                sort=db_sort,
                with_jobs_info=verbosity > 0,
            )

            table = get_flow_info_table(flows_info, verbosity=verbosity)

        if SETTINGS.cli_suggestions and max_results and len(flows_info) == max_results:
            out_console.print(
                f"The number of Flows printed is limited by the maximum selected: {max_results}",
                style="yellow",
            )

        out_console.print(table)


@app_flow.command()
def delete(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    yes_all: yes_opt = False,
    force_deprecated: force_opt_deprecated = False,
    max_limit: Annotated[
        int,
        typer.Option(
            "--max",
            "-m",
            help=(
                "The Flows will be deleted only if the total number is lower than the specified limit. 0 means no limit"
            ),
        ),
    ] = 10,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            help="Print the list of Flows to be deleted when asking for confirmation. "
            "Multiple -v options increase the details on the flow. (e.g. -vvv)",
            count=True,
        ),
    ] = False,
    delete_output: delete_output_opt = False,
    delete_files: delete_files_opt = False,
    delete_all: delete_all_opt = False,
    keep_processes: Annotated[
        bool,
        typer.Option(
            "--keep-processes",
            "-kp",
            help="Do not attempt to cancel SUBMITTED and RUNNING processes from "
            "the worker associated with the deleted Flows",
        ),
    ] = False,
    wait: wait_lock_opt = None,
    break_lock: break_lock_opt = False,
) -> None:
    """Permanently delete Flows from the database"""
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})

    if delete_all:
        delete_files = delete_output = True

    start_date = get_start_date(start_date, days, hours)

    jc = get_job_controller()

    # At variance with flows_list, for the amount of details to be printed,
    # the verbosity value will be decreased by one: the first is to enable
    # initial print

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Fetching data...", total=None)
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_ids=flow_id,
            states=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            with_jobs_info=verbosity > 1,
        )

    if not flows_info:
        exit_with_warning_msg("No flows matching criteria")

    if flows_info and not yes_all:
        if verbosity:
            preamble = Text.from_markup(
                f"[red]This operation will [bold]delete the following {len(flows_info)} Flow(s)[/bold][/red]"
            )
            out_console.print(preamble)
            table = get_flow_info_table(flows_info, verbosity=verbosity - 1)
            out_console.print(table)
            text = Text.from_markup("[red]Proceed anyway?[/red]")
        else:
            text = Text.from_markup(
                f"[red]This operation will [bold]delete {len(flows_info)} Flow(s)[/bold]. Proceed anyway?[/red]"
            )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    to_delete = [fi.flow_id for fi in flows_info]

    # if potentially interactive do not start the spinner.
    spinner_cm: contextlib.AbstractContextManager
    if delete_files and jc.project.has_interactive_workers:
        spinner_cm = contextlib.nullcontext()
        out_console.print("Deleting flows...")
    else:
        spinner_cm = loading_spinner(processing=False)
    with spinner_cm as progress:
        if progress:
            progress.add_task(description="Deleting flows...", total=None)

        deleted = jc.delete_flows(
            flow_ids=to_delete,
            delete_output=delete_output,
            delete_files=delete_files,
            cancel_processes=not keep_processes,
            max_limit=max_limit,
            wait=wait,
            break_lock=break_lock,
        )

    if not_deleted := set(to_delete) - set(deleted):
        out_console.print(
            f"Some of the selected Flows were not deleted: {', '.join(i for i in not_deleted)}"
        )

    if deleted:
        out_console.print(
            f"Deleted Flow(s) with id: {', '.join(str(i) for i in deleted)}"
        )


@app_flow.command(name="info")
def flow_info(
    flow_db_id: flow_db_id_arg,
    job_id_flag: job_flow_id_flag_opt = False,
    verbosity: verbosity_opt = 0,
    stored_data_keys: stored_data_keys_opt = None,
    cli_output_keys: cli_output_keys_opt = None,
    sort: sort_opt = SortOption.UPDATED_ON,
    jobs_sort: Annotated[
        SortOption,
        typer.Option(
            "--jobs-sort",
            help="The field on which the jobs will be sorted. In descending order",
        ),
    ] = None,
    reverse_sort: reverse_sort_flag_opt = False,
    reverse_jobs_sort: Annotated[
        bool,
        typer.Option(
            "--reverse-jobs-sort",
            "-jrevs",
            help="Reverse the sorting order of the jobs",
        ),
    ] = False,
    print_report: Annotated[
        bool,
        typer.Option(
            "--report",
            "-rep",
            help="Show a summary report of the Flow status",
        ),
    ] = False,
) -> None:
    """Provide detailed information on a Flow."""

    output_keys = check_output_stored_data_keys(
        cli_output_keys, stored_data_keys, verbosity, header_name_data_getter_map
    )

    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    db_ids = job_ids = flow_ids = None
    if db_id is not None:
        db_ids = [db_id]
    elif job_id_flag:
        job_ids = [jf_id]
    else:
        flow_ids = [jf_id]

    db_sort: list[tuple[str, int]] = [(sort.value, 1 if reverse_sort else -1)]
    db_jobs_sort: list[tuple[str, int]] | None = None
    if jobs_sort:
        db_jobs_sort = [(jobs_sort.value, 1 if reverse_jobs_sort else -1)]

    with loading_spinner():
        jc = get_job_controller()
        with_jobs_info: bool | list[str] = True
        if report:
            with_jobs_info = ["start_time", "end_time"]

        flows_info = jc.get_flows_info(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            sort=db_sort,
            jobs_sort=db_jobs_sort,
            limit=1,
            with_jobs_info=with_jobs_info,
        )
    if not flows_info:
        exit_with_error_msg("No data matching the request")

    if print_report:
        out_console.print(*get_single_flow_report_components(flows_info[0]))
    else:
        out_console.print(
            format_flow_info(
                flows_info[0],
                verbosity=verbosity,
                output_keys=output_keys,
                stored_data_keys=stored_data_keys,
            )
        )


@app_flow.command()
def graph(
    flow_db_id: flow_db_id_arg,
    job_id_flag: job_flow_id_flag_opt = False,
    label: Annotated[
        str | None,
        typer.Option(
            "--label",
            "-l",
            help="The label used to identify the nodes",
        ),
    ] = "name",
    file_path: Annotated[
        str | None,
        typer.Option(
            "--path",
            "-p",
            help="If defined, the graph will be dumped to a file",
        ),
    ] = None,
    dash_plot: Annotated[
        bool,
        typer.Option(
            "--dash",
            "-d",
            help="Show the graph in a dash app",
        ),
    ] = False,
    print_mermaid: Annotated[
        bool,
        typer.Option(
            "--mermaid",
            "-m",
            help="Print the mermaid graph",
        ),
    ] = False,
) -> None:
    """Provide detailed information on a Flow."""
    check_incompatible_opt({"dash": dash_plot, "mermaid": print_mermaid})
    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    db_ids = job_ids = flow_ids = None
    if db_id is not None:
        db_ids = [db_id]
    elif job_id_flag:
        job_ids = [jf_id]
    else:
        flow_ids = [jf_id]

    with loading_spinner():
        jc = get_job_controller()

        flows_info = jc.get_flows_info(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            limit=1,
            with_jobs_info=True,
        )
    if not flows_info:
        exit_with_error_msg("No data matching the request")

    if print_mermaid:
        from jobflow_remote.jobs.graph import get_mermaid

        print(get_mermaid(flows_info[0]))

    elif dash_plot:
        plot_dash(flows_info[0])
    else:
        plt = draw_graph(get_graph(flows_info[0], label=label))
        if file_path:
            plt.savefig(file_path)
        else:
            plt.show()


@app_flow.command()
def report(
    interval: Annotated[
        ReportInterval,
        typer.Argument(
            help="The interval of the trends for the report",
            metavar="INTERVAL",
        ),
    ] = ReportInterval.DAYS,
    num_intervals: Annotated[
        int | None,
        typer.Argument(
            help="The number of intervals to consider. Default depends on the interval type",
            metavar="NUM_INTERVALS",
        ),
    ] = None,
):
    """
    Generate a report about the Flows in the database.
    """
    jc = get_job_controller()

    timezone = datetime.now(tzlocal()).tzname()

    jobs_report = FlowsReport.generate_report(
        job_controller=jc,
        interval=interval.value,
        num_intervals=num_intervals,
        timezone=timezone,
    )
    out_console.print(*get_flow_report_components(jobs_report))


@app_flow.command()
def resume(
    flow_db_id: flow_db_id_arg,
    job_id_flag: job_flow_id_flag_opt = False,
) -> None:
    """Resume a STOPPED or PAUSED Flow."""
    job_id = flow_id = None
    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    if db_id is None:
        if job_id_flag:
            job_id = jf_id
        else:
            flow_id = jf_id

    with loading_spinner():
        jc = get_job_controller()

        n_jobs = jc.resume_flow(
            job_id=job_id,
            db_id=db_id,
            flow_id=flow_id,
        )

    out_console.print(f"{n_jobs} Job(s) resumed")


@app_flow.command()
def clean(
    job_id: job_ids_opt = None,
    db_id: db_ids_opt = None,
    flow_id: flow_ids_opt = None,
    state: flow_state_opt = None,
    start_date: start_date_opt = None,
    end_date: end_date_opt = None,
    name: name_opt = None,
    days: days_opt = None,
    hours: hours_opt = None,
    metadata: metadata_opt = None,
    locked: locked_flow_opt = False,
    verbosity: verbosity_opt = 0,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Do not check if runner is active",
        ),
    ] = False,
    yes_all: yes_opt = False,
    all_states: Annotated[
        bool,
        typer.Option(
            "--all-states",
            "-as",
            help="Delete files for Jobs in any state",
        ),
    ] = False,
):
    """
    Remove the files of the executed Jobs.
    """
    check_incompatible_opt({"start_date": start_date, "days": days, "hours": hours})
    check_incompatible_opt({"end_date": end_date, "days": days, "hours": hours})

    if all_states and not force:
        check_stopped_runner(error=True)

    jc = get_job_controller()

    start_date = get_start_date(start_date, days, hours)

    with loading_spinner():
        with_jobs_info = ["run_dir"]
        flows_info = jc.get_flows_info(
            job_ids=job_id,
            db_ids=db_id,
            flow_ids=flow_id,
            states=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            locked=locked,
            with_jobs_info=with_jobs_info,
        )

    if not flows_info:
        exit_with_warning_msg("No flows matching criteria")

    if not yes_all:
        if verbosity:
            preamble = Text.from_markup(
                f"[red]This operation will [bold]delete the files of the following {len(flows_info)} Flow(s)[/bold][/red]"
            )
            out_console.print(preamble)
            table = get_flow_info_table(flows_info, verbosity=verbosity - 1)
            out_console.print(table)
            text = Text.from_markup("[red]Proceed anyway?[/red]")
        else:
            text = Text.from_markup(
                f"[red]This operation will [bold]delete the files of {len(flows_info)} Flow(s)[/bold]. Proceed anyway?[/red]"
            )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    # if potentially interactive do not start the spinner.
    spinner_cm: contextlib.AbstractContextManager
    if jc.project.has_interactive_workers:
        spinner_cm = contextlib.nullcontext()
        out_console.print("Deleting files...")
    else:
        spinner_cm = loading_spinner(processing=False)

    skipped_jobs = []
    cleanable_states = (
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.REMOTE_ERROR,
    )
    with spinner_cm as progress:
        if progress:
            progress.add_task(description="Deleting files...", total=None)
        jobs_info = {}
        for fi in flows_info:
            for ji in fi.jobs_info:
                if ji.run_dir:
                    if not all_states and ji.state not in cleanable_states:
                        skipped_jobs.append(ji)
                    else:
                        jobs_info[ji.db_id] = ji

        deleted = jc.safe_delete_files(list(jobs_info.values()))

    deleted_dict = {ji.db_id: ji for ji in deleted}
    not_deleted = set(jobs_info) - set(deleted_dict)
    if not_deleted:
        out_console.print("Folder was not deleted for the following jobs:")
        for db_id in not_deleted:
            out_console.print(f" - {db_id}: {jobs_info[db_id].run_dir}")
    if skipped_jobs:
        out_console.print(
            f"{len(skipped_jobs)} were not cleaned-up due to their state:"
        )
        if len(skipped_jobs) < 10:
            for ji in skipped_jobs:
                out_console.print(f" - {ji.db_id} - {ji.state}")
        else:
            confirmed = False
            if not yes_all:
                text = (
                    "The number of skipped jobs is too large to be printed, the list can be "
                    "dumped to the `skipped_cleanup.dat` file. Do you want to create the file?"
                )
                confirmed = Confirm.ask(text, default=False)
            if yes_all or confirmed:
                with open("skipped_cleanup.dat", "w") as f:
                    for ji in skipped_jobs:
                        f.writelines(f" - {ji.db_id} - {ji.state}")

    out_console.print(f"Deleted execution folders of {len(deleted)} Jobs")


app_flow_set = JFRTyper(
    name="set", help="Commands for setting properties for flows", no_args_is_help=True
)
app_flow.add_typer(app_flow_set)


@app_flow_set.command()
def store(
    flow_db_id: flow_db_id_arg,
    store: Annotated[
        str | None,
        typer.Argument(
            help="The name of the Store to be set. If empty will set the default JobStore",
            metavar="STORE",
        ),
    ] = None,
    job_id_flag: job_flow_id_flag_opt = False,
) -> None:
    """Provide detailed information on a Flow."""
    db_id = job_id = flow_id = None
    db_id, jf_id = get_job_db_ids(flow_db_id, None)
    if db_id is None:
        if job_id_flag:
            job_id = jf_id
        else:
            flow_id = jf_id

    with loading_spinner():
        jc = get_job_controller()

        jc.set_flow_store(store=store, flow_id=flow_id, db_id=db_id, job_id=job_id)
    out_console.print("Flow has been updated")
