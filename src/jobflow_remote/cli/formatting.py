from __future__ import annotations

import datetime
import io
import time
from typing import TYPE_CHECKING

from monty.json import jsanitize
from rich.panel import Panel
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.utils import ReprStr, fmt_datetime
from jobflow_remote.jobs.state import FlowState, JobState
from jobflow_remote.utils.data import convert_utc_time

if TYPE_CHECKING:
    from rich.console import RenderableType

    from jobflow_remote.config.base import ExecutionConfig, WorkerBase
    from jobflow_remote.jobs.data import FlowInfo, JobDoc, JobInfo
    from jobflow_remote.jobs.report import FlowsReport, JobsReport


def get_job_info_table(jobs_info: list[JobInfo], verbosity: int):
    time_zone_str = f" [{time.tzname[0]}]"

    table = Table(title="Jobs info")
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Job id  (Index)")

    table.add_column("Worker")
    table.add_column("Last updated" + time_zone_str)

    if verbosity >= 1:
        table.add_column("Queue id")
        table.add_column("Run time")
        table.add_column("Retry time" + time_zone_str)
        table.add_column("Prev state")
        if verbosity < 2:
            table.add_column("Locked")

    if verbosity >= 2:
        table.add_column("Lock id")
        table.add_column("Lock time" + time_zone_str)

    for ji in jobs_info:
        state = ji.state.name

        if ji.state in (JobState.REMOTE_ERROR, JobState.FAILED):
            state = f"[bold red]{state}[/]"
        elif ji.remote.retry_time_limit is not None:
            state = f"[bold orange3]{state}[/]"

        row = [
            str(ji.db_id),
            ji.name,
            Text.from_markup(state),
            f"{ji.uuid}  ({ji.index})",
            ji.worker,
            convert_utc_time(ji.updated_on).strftime(fmt_datetime),
        ]

        if verbosity >= 1:
            row.append(ji.remote.process_id)
            prefix = ""
            if ji.state == JobState.RUNNING:
                run_time = ji.estimated_run_time
                prefix = "~"
            else:
                run_time = ji.run_time
            if run_time:
                m, s = divmod(run_time, 60)
                h, m = divmod(m, 60)
                row.append(prefix + f"{h:g}:{m:02g}")
            else:
                row.append("")
            row.append(
                convert_utc_time(ji.remote.retry_time_limit).strftime(fmt_datetime)
                if ji.remote.retry_time_limit
                else None
            )
            row.append(ji.previous_state.name if ji.previous_state else None)
            if verbosity < 2:
                row.append("*" if ji.lock_id is not None else None)

        if verbosity >= 2:
            row.append(str(ji.lock_id))
            row.append(
                convert_utc_time(ji.lock_time).strftime(fmt_datetime)
                if ji.lock_time
                else None
            )

        table.add_row(*row)

    return table


def get_flow_info_table(flows_info: list[FlowInfo], verbosity: int):
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

    # reorder the keys
    # Do not check here that all the keys in JobInfo are in JOB_INFO_ORDER. Check in the tests
    sorted_d = {}
    for k in JOB_INFO_ORDER:
        if k in d:
            sorted_d[k] = d[k]

    return render_scope(sorted_d, sort_keys=False)


def format_flow_info(flow_info: FlowInfo):
    title = f"Flow: {flow_info.name} - {flow_info.flow_id} - {flow_info.state.name}"
    table = Table(title=title)
    table.title_style = "bold"
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Job id  (Index)")
    table.add_column("Worker")

    for i, job_id in enumerate(flow_info.job_ids):
        state = flow_info.job_states[i].name

        row = [
            str(flow_info.db_ids[i]),
            flow_info.job_names[i],
            state,
            f"{job_id}  ({flow_info.job_indexes[i]})",
            flow_info.workers[i],
        ]

        table.add_row(*row)

    return table


def get_exec_config_table(exec_config: dict[str, ExecutionConfig], verbosity: int = 0):
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


def get_worker_table(workers: dict[str, WorkerBase], verbosity: int = 0):
    table = Table(title="Workers", show_lines=verbosity > 1)
    table.add_column("Name")
    if verbosity > 0:
        table.add_column("type")
    if verbosity == 1:
        table.add_column("info")
    elif verbosity > 1:
        table.add_column("details")

    for name in sorted(workers):
        row = [Text(name, style="bold")]
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

    total_count = sum(state_counts.values())

    # Display job states in a histogram
    state_colors = {
        JobState.WAITING: "grey39",
        JobState.READY: "cyan",
        JobState.CHECKED_OUT: "bright_cyan",
        JobState.UPLOADED: "deep_sky_blue1",
        JobState.SUBMITTED: "blue",
        JobState.RUNNING: "green",
        JobState.TERMINATED: "red",
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

    total_count = sum(state_counts.values())

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
