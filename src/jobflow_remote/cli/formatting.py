from __future__ import annotations

import copy
import datetime
import io
import time
from itertools import cycle
from typing import TYPE_CHECKING

from monty.json import jsanitize
from rich.console import Group
from rich.markdown import Markdown
from rich.panel import Panel
from rich.pretty import Pretty
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.utils import ReprStr, fmt_datetime, render_scope_jfr
from jobflow_remote.jobs.state import FlowState, JobState
from jobflow_remote.remote.data import get_job_path
from jobflow_remote.utils.data import convert_utc_time, get_h_m_s

if TYPE_CHECKING:
    from packaging.version import Version
    from rich.console import ConsoleRenderable, RenderableType

    from jobflow_remote.config.base import ExecutionConfig, WorkerBase
    from jobflow_remote.jobs.data import BatchDoc, FlowInfo, JobDoc, JobInfo
    from jobflow_remote.jobs.report import FlowsReport, JobsReport
    from jobflow_remote.jobs.upgrade import UpgradeAction

colors_list = [
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "bright_red",
    "bright_green",
    "bright_yellow",
    "bright_blue",
    "bright_magenta",
    "bright_cyan",
    "dark_blue",
    "salmon1",
    "dodger_blue1",
    "spring_green4",
    "dark_green",
    "cyan1",
    "purple4",
    "royal_blue1",
    "dark_red",
    "orange4",
    "green3",
    "deep_sky_blue1",
    "deep_pink1",
    "orange1",
    "deep_sky_blue4",
    "yellow3",
    "violet",
    "deep_pink3",
    "spring_green1",
    "steel_blue",
    "green_yellow",
    "blue3",
]


def format_state(ji: JobInfo) -> Text:
    state = ji.state.name
    if ji.state in (JobState.REMOTE_ERROR, JobState.FAILED):
        state = f"[bold red]{state}[/]"
    elif ji.remote.retry_time_limit is not None:
        state = f"[bold orange3]{state}[/]"
    return Text.from_markup(state)


def format_run_time(ji: JobInfo) -> str:
    prefix = ""
    if ji.state == JobState.RUNNING:
        run_time = ji.estimated_run_time
        prefix = "~"
    else:
        run_time = ji.run_time
    if not run_time:
        return ""
    m, s = divmod(run_time, 60)
    h, m = divmod(m, 60)
    h, m, s = get_h_m_s(run_time)
    return prefix + f"{h:g}:{m:02g}"


time_zone_str = f" [{time.tzname[0]}]"
header_name_data_getter_map = {
    "db_id": ("DB id", lambda ji: str(ji.db_id)),
    "name": ("Name", lambda ji: ji.name),
    "state": ("State", format_state),
    "job_id": ("Job id (Index)", lambda ji: f"{ji.uuid}  ({ji.index})"),
    "worker": ("Worker", lambda ji: ji.worker),
    "last_updated": (
        "Last updated" + time_zone_str,
        lambda ji: convert_utc_time(ji.updated_on).strftime(fmt_datetime),
    ),
    "queue_id": ("Queue id", lambda ji: ji.remote.process_id),
    "run_time": (Text("Run time [h:mm]", no_wrap=True), format_run_time),
    "retry_time": (
        "Retry time" + time_zone_str,
        lambda ji: convert_utc_time(ji.remote.retry_time_limit).strftime(fmt_datetime)
        if ji.remote.retry_time_limit
        else None,
    ),
    "prev_state": (
        "Prev state",
        lambda ji: ji.previous_state.name if ji.previous_state else None,
    ),
    "locked": ("Locked", lambda ji: "*" if ji.lock_id is not None else None),
    "lock_id": ("Lock id", lambda ji: str(ji.lock_id)),
    "lock_time": (
        "Lock time" + time_zone_str,
        lambda ji: convert_utc_time(ji.lock_time).strftime(fmt_datetime)
        if ji.lock_time
        else None,
    ),
    "metadata": (
        "Metadata",
        lambda ji: Pretty(ji.metadata, max_length=10, max_string=500, max_depth=3),
    ),
}


def get_job_info_table(
    jobs_info: list[JobInfo],
    verbosity: int,
    output_keys: list[str] | None = None,
    stored_data_keys: list[str] | None = None,
    color: bool = False,
) -> Table:
    stored_data_keys = stored_data_keys or []
    if not output_keys or verbosity > 0:
        all_output_keys = list(header_name_data_getter_map)
        output_keys = all_output_keys[:6]
        if verbosity >= 1:
            output_keys += all_output_keys[6:10]
        if verbosity == 1:
            output_keys.append(all_output_keys[10])
        if verbosity >= 2:
            output_keys += all_output_keys[11:14]
    all_display_keys = output_keys + stored_data_keys

    # Use a dictionary to determine how to extract the value to print from each
    # JobInfo object. Main reference is header_name_data_getter_map and gets
    # updated with additional required values.
    sdk_map = {
        k: (k, lambda x, k=k: x.stored_data.get(k) if x.stored_data else None)
        for k in stored_data_keys
    }
    full_map = header_name_data_getter_map | sdk_map

    table = Table(title="Jobs info")
    for key in all_display_keys:
        table.add_column(full_map[key][0])

    if color and "name" in all_display_keys:
        # color the name of the Job by replacing the function that gets the
        # value of the cell in header_name_data_getter_map.
        # make a copy to avoid modifying the original object.
        main_hosts = list(dict.fromkeys(ji.hosts[-1] for ji in jobs_info if ji.hosts))
        hosts_color_map = dict(zip(main_hosts, cycle(colors_list)))

        def get_colored_name(ji):
            if ji.hosts:
                return Text(ji.name, style=hosts_color_map.get(ji.hosts[-1], None))
            # this should likely never happen, but leave to avoid hard failures
            return ji.name

        full_map = copy.deepcopy(full_map)
        full_map["name"] = (full_map["name"][0], get_colored_name)

    for ji in jobs_info:
        table.add_row(*(full_map[key][1](ji) for key in all_display_keys))

    return table


def get_flow_info_table(flows_info: list[FlowInfo], verbosity: int) -> Table:
    time_zone_str = f" [{time.tzname[0]}]"

    table = Table(title="Flows info")
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Flow id")
    table.add_column("Num Jobs")
    table.add_column("Last updated" + time_zone_str)

    if verbosity >= 1:
        table.add_column("Workers")

        table.add_column("Job states")
        table.add_column("Flow metadata")

    for fi in flows_info:
        # show the smallest Job db_id as db_id
        db_id = min(fi.db_ids)

        row = [
            str(db_id),
            fi.name,
            fi.state.name,
            fi.flow_id,
            str(len(fi.job_ids)),
            convert_utc_time(fi.updated_on).strftime(fmt_datetime),
        ]

        if verbosity >= 1:
            workers = set(fi.workers)
            row.append(", ".join(workers))
            job_states = "-".join(js.short_value for js in fi.job_states)
            row.append(job_states)
            row.append(
                Pretty(fi.flow_metadata, max_length=10, max_string=500, max_depth=3)
            )

        table.add_row(*row)

    return table


JOB_INFO_ORDER = [
    "db_id",
    "uuid",
    "index",
    "name",
    "state",
    "error",
    "remote",
    "previous_state",
    "job",
    "created_on",
    "updated_on",
    "start_time",
    "end_time",
    "metadata",
    "run_dir",
    "parents",
    "priority",
    "worker",
    "resources",
    "exec_config",
    "lock_id",
    "lock_time",
    "stored_data",
    "hosts",
]


def format_job_info(
    job_info: JobInfo | JobDoc, verbosity: int, show_none: bool = False
):
    d = job_info.dict(exclude_none=not show_none)
    if verbosity == 1:
        d.pop("job", None)

    # convert dates at the first level and for the remote error
    for k, v in d.items():
        if isinstance(v, datetime.datetime):
            d[k] = convert_utc_time(v).strftime(fmt_datetime)

    if d["remote"].get("retry_time_limit"):
        d["remote"]["retry_time_limit"] = convert_utc_time(
            d["remote"]["retry_time_limit"]
        ).strftime(fmt_datetime)

    d = jsanitize(d, allow_bson=True, enum_values=True, strict=True)
    error = d.get("error")
    if error:
        d["error"] = ReprStr(error)

    remote_error = d["remote"].get("error")
    if remote_error:
        d["remote"]["error"] = ReprStr(remote_error)

    if verbosity == 0:
        d["remote"].pop("queue_out", None)
        d["remote"].pop("queue_err", None)
    else:
        queue_out = d["remote"].get("queue_out")
        if queue_out:
            d["remote"]["queue_out"] = ReprStr(queue_out)
        queue_err = d["remote"].get("queue_err")
        if queue_err:
            d["remote"]["queue_err"] = ReprStr(queue_err)

    if verbosity < 2 and d.get("parents") and len(d.get("parents", [])) > 5:
        d["parents"] = d["parents"][:2] + ["..."] + d["parents"][-2:]

    if verbosity < 2 and d.get("hosts") and len(d.get("hosts", [])) > 5:
        d["hosts"] = d["hosts"][:2] + ["..."] + d["hosts"][-2:]

    # reorder the keys
    # Do not check here that all the keys in JobInfo are in JOB_INFO_ORDER. Check in the tests
    sorted_d = {}
    for k in JOB_INFO_ORDER:
        if k in d:
            sorted_d[k] = d[k]

    return render_scope_jfr(sorted_d, sort_keys=False, overflow="fold")


def format_flow_info(
    flow_info: FlowInfo, verbosity=0, output_keys=None, stored_data_keys=None
) -> Table:
    title = Text(
        f"Flow: {flow_info.name} - {flow_info.flow_id} - {flow_info.state.name}"
    )
    if verbosity > 0:
        title = Group(title, Text("Metadata:"), Pretty(flow_info.flow_metadata))
    table = get_job_info_table(
        flow_info.jobs_info or [],
        verbosity=verbosity,
        output_keys=output_keys,
        stored_data_keys=stored_data_keys,
    )
    table.title = title
    return table


def get_exec_config_table(
    exec_config: dict[str, ExecutionConfig], verbosity: int = 0
) -> Table:
    table = Table(title="Execution config", show_lines=verbosity > 0)
    table.add_column("Name")
    if verbosity > 0:
        table.add_column("modules")
        table.add_column("export")
        table.add_column("pre_run")
        table.add_column("post_run")
    for name in sorted(exec_config):
        row = [Text(name, style="bold")]
        if verbosity > 0:
            ec = exec_config[name]
            from ruamel.yaml import YAML

            yaml = YAML()
            # The following should already be the case but we keep it to be sure
            yaml.default_flow_style = False
            if ec.modules:
                ec_modules_strio = io.StringIO()
                yaml.dump(ec.modules, ec_modules_strio)
                row.append(ec_modules_strio.getvalue())
            else:
                row.append("")
            if ec.export:
                ec_export_strio = io.StringIO()
                yaml.dump(ec.export, ec_export_strio)
                row.append(ec_export_strio.getvalue())
            else:
                row.append("")
            if ec.post_run:
                row.append(ec.post_run)
            else:
                row.append("")

        table.add_row(*row)

    return table


def get_worker_table(workers: dict[str, WorkerBase], verbosity: int = 0) -> Table:
    table = Table(title="Workers", show_lines=verbosity > 1)
    table.add_column("Name")
    if verbosity > 0:
        table.add_column("type")
    if verbosity == 1:
        table.add_column("info")
    elif verbosity > 1:
        table.add_column("details")

    for name in sorted(workers):
        row: list[Text | str | ConsoleRenderable] = [Text(name, style="bold")]
        worker = workers[name]
        if verbosity > 0:
            row.append(worker.type)
        if verbosity == 1:
            row.append(render_scope(worker.cli_info))
        elif verbosity > 1:
            d = worker.dict(exclude_none=True)
            d = jsanitize(d, allow_bson=False, enum_values=True)
            row.append(render_scope(d))

        table.add_row(*row)

    return table


def create_bar(count, max_count, size=30, color="white"):
    """Creates a text-based bar for a histogram with fixed color per state."""
    bar_filled = "█" * int(size * count / max_count)
    bar_empty = " " * (size - len(bar_filled))
    return f"[{color}]{bar_filled}[white]{bar_empty}"


def get_job_report_components(report: JobsReport) -> list[RenderableType]:
    components = []

    # Summary of Key Metrics
    summary_table = Table(title="Job Summary", title_style="bold green")
    summary_table.add_column("Metric", style="cyan", justify="right")
    summary_table.add_column("Count", style="green", justify="center")

    summary_table.add_row("Completed Jobs", str(report.completed))
    summary_table.add_row("Running Jobs", str(report.running))
    summary_table.add_row("Error Jobs", str(report.error))
    summary_table.add_row("Active Jobs", str(report.active))

    components.append(summary_table)

    # Job State Distribution
    components.append(
        Panel("[bold green]Job State Distribution[/bold green]", expand=False)
    )

    # Remove COMPLETED, as this will likely account for most of the jobs present in the DB
    state_counts = dict(report.state_counts)
    state_counts.pop(JobState.COMPLETED)

    # Find the max count to normalize the histograms
    max_count = max(*state_counts.values(), 1)

    total_count = sum(state_counts.values()) or 1

    # Display job states in a histogram
    state_colors = {
        JobState.WAITING: "grey39",
        JobState.READY: "cyan",
        JobState.CHECKED_OUT: "bright_cyan",
        JobState.UPLOADED: "deep_sky_blue1",
        JobState.SUBMITTED: "blue",
        JobState.RUNNING: "green",
        JobState.RUN_FINISHED: "red",
        JobState.DOWNLOADED: "blue_violet",
        JobState.REMOTE_ERROR: "yellow",
        JobState.COMPLETED: "green",
        JobState.FAILED: "red",
        JobState.PAUSED: "magenta",
        JobState.STOPPED: "dark_orange",
        JobState.USER_STOPPED: "orange4",
        JobState.BATCH_SUBMITTED: "light_slate_blue",
        JobState.BATCH_RUNNING: "chartreuse3",
    }

    newline = ""
    for state, color in state_colors.items():
        if state not in state_counts:
            continue
        count = state_counts[state]
        percentage = round((count / total_count) * 100)
        bar = create_bar(count, max_count, color=color)
        components.extend(
            [f"{newline}{state.name:15} [{count:>3}] ({percentage:>3}%):", bar]
        )
        newline = "\n"

    # Longest Running Jobs
    if report.longest_running:
        longest_running_table = get_job_info_table(report.longest_running, verbosity=1)
        longest_running_table.title = "Longest running jobs"
        longest_running_table.title_style = "bold green"
        components.append(longest_running_table)

    # Worker Utilization
    if report.worker_utilization:
        worker_table = Table(title="Worker Jobs Distribution", title_style="bold green")
        worker_table.add_column("Worker", style="cyan", justify="center")
        worker_table.add_column("Job Count", style="green", justify="center")

        for worker, count in report.worker_utilization.items():
            worker_table.add_row(worker, str(count))

        components.append(worker_table)

    # Job Trends
    if report.trends:
        trends = report.trends
        trends_table = Table(
            title=f"Job Trends ({trends.num_intervals} {trends.interval}) [{trends.timezone}]",
            title_style="bold green",
        )
        trends_table.add_column("Date", justify="center", style="cyan", no_wrap=True)
        trends_table.add_column("Completed", justify="center", style="green")
        trends_table.add_column("Failed", justify="center", style="red")
        trends_table.add_column("Remote Error", justify="center", style="yellow")

        for i in range(trends.num_intervals):
            trends_table.add_row(
                trends.dates[i],
                f"{trends.completed[i]}",
                f"{trends.failed[i]}",
                f"{trends.remote_error[i]}",
            )

        components.append(trends_table)

    return components


def get_flow_report_components(report: FlowsReport) -> list[RenderableType]:
    components = []

    # Summary of Key Metrics
    summary_table = Table(title="Flow Summary", title_style="bold green")
    summary_table.add_column("Metric", style="cyan", justify="right")
    summary_table.add_column("Count", style="green", justify="center")

    summary_table.add_row("Completed Flows", str(report.completed))
    summary_table.add_row("Running Flows", str(report.running))
    summary_table.add_row("Error Flows", str(report.error))

    components.append(summary_table)

    # Job State Distribution
    components.append(
        Panel("[bold green]Flow State Distribution[/bold green]", expand=False)
    )

    # Remove COMPLETED, as this will likely account for most of the jobs present in the DB
    state_counts = dict(report.state_counts)
    state_counts.pop(FlowState.COMPLETED)

    # Find the max count to normalize the histograms
    max_count = max(*state_counts.values(), 1)

    total_count = sum(state_counts.values()) or 1

    # Display job states in a histogram
    state_colors = {
        FlowState.WAITING: "grey39",
        FlowState.READY: "cyan",
        FlowState.RUNNING: "green",
        FlowState.COMPLETED: "green",
        FlowState.FAILED: "red",
        FlowState.PAUSED: "magenta",
        FlowState.STOPPED: "dark_orange",
    }

    newline = ""
    for state, color in state_colors.items():
        if state not in state_counts:
            continue
        count = state_counts[state]
        percentage = round((count / total_count) * 100)
        bar = create_bar(count, max_count, color=color)
        components.extend(
            [f"{newline}{state.name:15} [{count:>3}] ({percentage:>3}%):", bar]
        )
        newline = "\n"

    # Job Trends
    if report.trends:
        trends = report.trends
        trends_table = Table(
            title=f"Flow Trends ({trends.num_intervals} {trends.interval}) [{trends.timezone}]",
            title_style="bold green",
        )
        trends_table.add_column("Date", justify="center", style="cyan", no_wrap=True)
        trends_table.add_column("Completed", justify="center", style="green")
        trends_table.add_column("Failed", justify="center", style="red")

        for i in range(trends.num_intervals):
            trends_table.add_row(
                trends.dates[i],
                f"{trends.completed[i]}",
                f"{trends.failed[i]}",
            )

        components.append(trends_table)

    return components


def get_single_flow_report_components(flow_info: FlowInfo) -> list[RenderableType]:
    """
    Generate report rich components for a single Flow.

    Parameters
    ----------
    flow_info
        The FlowInfo object containing information about the Flow.

    Returns
    -------
    list[RenderableType]
        List of Rich components for display.
    """
    components = []

    # Flow Header with basic information
    header_table = Table(
        title=f"Flow Report: {flow_info.name}",
        title_style="bold green",
        show_header=False,
    )
    header_table.add_column("", style="cyan", justify="right")
    header_table.add_column("", style="white", justify="left")

    header_table.add_row("Flow ID", flow_info.flow_id)
    header_table.add_row("Flow State", flow_info.state.name)
    header_table.add_row("Number of Jobs", str(len(flow_info.job_ids)))

    # Calculate timing information
    start_time = flow_info.created_on
    header_table.add_row(
        "Start Time",
        convert_utc_time(start_time).strftime(fmt_datetime) + f" [{time.tzname[0]}]",
    )

    # If flow is completed, calculate total time
    if flow_info.state == FlowState.COMPLETED:
        end_time = flow_info.updated_on
        header_table.add_row(
            "Complete Time",
            convert_utc_time(end_time).strftime(fmt_datetime) + f" [{time.tzname[0]}]",
        )
        total_time = (end_time - start_time).total_seconds()
        hours, minutes, seconds = get_h_m_s(total_time)
        header_table.add_row(
            "Total Time", f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        )
    else:
        header_table.add_row(
            "Last Updated",
            convert_utc_time(flow_info.updated_on).strftime(fmt_datetime)
            + f" [{time.tzname[0]}]",
        )
        # Show elapsed time for non-completed flows
        elapsed_time = (
            datetime.datetime.now(datetime.timezone.utc)
            - start_time.replace(tzinfo=datetime.timezone.utc)
        ).total_seconds()
        hours, minutes, seconds = get_h_m_s(elapsed_time)
        header_table.add_row(
            "Elapsed Time", f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
        )

    # Calculate sum of job execution times
    if flow_info.jobs_info:
        total_job_run_time = sum(job.run_time or 0 for job in flow_info.jobs_info)
        if total_job_run_time > 0:
            hours, minutes, seconds = get_h_m_s(total_job_run_time)
            header_table.add_row(
                "Total Job Run Time",
                f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}",
            )

    components.append(header_table)

    # Job States Distribution
    if flow_info.jobs_info:
        # Count job states
        state_counts: dict[JobState, int] = {}
        for job in flow_info.jobs_info:
            state = job.state
            state_counts[state] = state_counts.get(state, 0) + 1

        # Create job states table
        states_table = Table(title="Job States Distribution", title_style="bold green")
        states_table.add_column("State", style="cyan", justify="left")
        states_table.add_column("Count", style="white", justify="center")
        states_table.add_column("Percentage", style="white", justify="right")

        total_jobs = len(flow_info.jobs_info)

        for state in JobState:
            if state in state_counts:
                count = state_counts[state]
                percentage = (count / total_jobs) * 100

                # Color code the state name based on state
                if state in (JobState.FAILED, JobState.REMOTE_ERROR):
                    state_str = f"[red]{state.name}[/red]"
                elif state == JobState.COMPLETED:
                    state_str = f"[green]{state.name}[/green]"
                elif state == JobState.RUNNING:
                    state_str = f"[cyan]{state.name}[/cyan]"
                elif state in (
                    JobState.PAUSED,
                    JobState.STOPPED,
                    JobState.USER_STOPPED,
                ):
                    state_str = f"[yellow]{state.name}[/yellow]"
                else:
                    state_str = state.name

                states_table.add_row(
                    Text.from_markup(state_str), str(count), f"{percentage:.1f}%"
                )

        components.append(states_table)

        # Flow advancement
        # Consider all the states after the Job has started as "running"
        running_states = (
            JobState.RUNNING,
            JobState.BATCH_RUNNING,
            JobState.RUN_FINISHED,
            JobState.DOWNLOADED,
        )
        completed_count = sum(
            1 for j in flow_info.jobs_info if j.state == JobState.COMPLETED
        )
        failed_count = sum(
            1
            for j in flow_info.jobs_info
            if j.state in (JobState.FAILED, JobState.REMOTE_ERROR)
        )
        running_count = sum(1 for j in flow_info.jobs_info if j.state in running_states)
        queued_count = sum(
            1
            for j in flow_info.jobs_info
            if j.state in (JobState.SUBMITTED, JobState.BATCH_SUBMITTED)
        )
        pending_count = (
            total_jobs - completed_count - failed_count - running_count - queued_count
        )

        progress_panel = Panel.fit(
            Text.from_markup(
                f"[green]Completed: {completed_count}[/green] | "
                f"[cyan]Running: {running_count}[/cyan] | "
                f"[blue]Queued: {queued_count}[/blue] | "
                f"[yellow]Pending: {pending_count}[/yellow] | "
                f"[red]Failed: {failed_count}[/red]\n"
                f"Progress: {completed_count}/{total_jobs} ({(completed_count/total_jobs)*100:.1f}%)"
            ),
            title="Flow Progress",
            title_align="left",
            border_style="green",
        )
        components.append(progress_panel)

    return components


def format_upgrade_actions(actions: list[UpgradeAction]):
    msg = ""
    for action in actions:
        msg += f"* {action.description}\n"

    return Markdown(msg)


def format_failed_conditions(failed_conditions: list[tuple[Version, dict]]):
    msg = ""
    for fc_version, fc_dict in failed_conditions:
        msg += f"* {fc_dict['condition'].description}: {fc_dict['message']} (condition to upgrade to {fc_version!s})\n"

    return Markdown(msg)


def get_batch_processes_table(
    batch_processes: list,
    workers: dict[str, WorkerBase],
    verbosity: int = 0,
    title: str = "Running batches info",
    status: bool = False,
):
    table = Table(title=title)
    table.add_column("Process ID")
    table.add_column("Batch UID")
    table.add_column("Worker")
    table.add_column("Process folder")
    if status:
        table.add_column("Status")
    if verbosity > 0:
        table.add_column("DB id - Running Job ids (Index)")
        table.add_column(header_name_data_getter_map["last_updated"][0])

    for batch_data in batch_processes:
        worker = workers[batch_data.worker]
        row = [
            batch_data.process_id,
            batch_data.batch_uid,
            batch_data.worker,
            get_job_path(batch_data.batch_uid, None, worker.batch.work_dir),
        ]
        if status:
            row.append(batch_data.batch_state.value)

        if verbosity > 0:
            row.append(
                "\n".join([f"{jb[0]} {jb[1]} ({jb[2]})" for jb in batch_data.jobs])
            )
            row.append(header_name_data_getter_map["last_updated"][1](batch_data))

        table.add_row(*row)

    return table


def get_runner_pings_table(runner_pings: list[dict]) -> Table:
    ping_keys = {
        "daemon_id": lambda x: str(x),
        "runner_id": lambda x: str(x),
        "time": lambda x: convert_utc_time(x).strftime(fmt_datetime),
        "hostname": lambda x: str(x),
        "run_options": lambda x: str(x),
        "project_name": lambda x: str(x),
        "user": lambda x: str(x),
    }
    table = Table(title="Runner pings")
    table.add_column("Daemon ID")
    table.add_column("Runner ID")
    table.add_column(f"Ping time{time_zone_str}")
    table.add_column("Hostname")
    table.add_column("Run options")
    table.add_column("Project")
    table.add_column("User")

    for rp in reversed(runner_pings):
        table.add_row(*[f(rp.get(k)) for k, f in ping_keys.items()])

    return table


BATCH_INFO_ORDER = [
    "process_id",
    "batch_uid",
    "batch_state",
    "worker",
    "process_folder",
    "updated_on",
    "last_ping_time",
    "created_on",
    "start_time",
    "end_time",
    "jobs",
]


def format_batch_info(batch_doc: BatchDoc, worker):
    d = batch_doc.dict()

    # convert dates at the first level and for the remote error
    for k, v in d.items():
        if isinstance(v, datetime.datetime):
            d[k] = convert_utc_time(v).strftime(fmt_datetime)

    d = jsanitize(d, allow_bson=True, enum_values=True, strict=True)

    d["process_folder"] = get_job_path(batch_doc.batch_uid, None, worker.batch.work_dir)

    # reorder the keys
    sorted_d = {}
    for k in BATCH_INFO_ORDER:
        if k in d:
            sorted_d[k] = d[k]

    return render_scope_jfr(sorted_d, sort_keys=False, overflow="fold")
