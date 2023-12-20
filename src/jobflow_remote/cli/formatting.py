from __future__ import annotations

import datetime
import time

from monty.json import jsanitize
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.utils import ReprStr, fmt_datetime
from jobflow_remote.config.base import ExecutionConfig, WorkerBase
from jobflow_remote.jobs.data import FlowInfo, JobDoc, JobInfo
from jobflow_remote.jobs.state import JobState
from jobflow_remote.utils.data import convert_utc_time


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

        if ji.remote.retry_time_limit is not None:
            state = f"[bold red]{state}[/]"

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

    return render_scope(d)


def format_flow_info(flow_info: FlowInfo):
    title = f"Flow: {flow_info.name} - {flow_info.flow_id} - {flow_info.state.name}"
    table = Table(title=title)
    table.title_style = "bold"
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State [Remote]")
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
    for name in sorted(exec_config.keys()):
        row = [Text(name, style="bold")]
        if verbosity > 0:
            ec = exec_config[name]
            from ruamel import yaml

            if ec.modules:
                row.append(yaml.dump(ec.modules, default_flow_style=False))
            else:
                row.append("")
            if ec.export:
                row.append(yaml.dump(ec.export, default_flow_style=False))
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

    for name in sorted(workers.keys()):
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
