from __future__ import annotations

from dataclasses import asdict

from monty.json import jsanitize
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.utils import ReprStr, fmt_datetime
from jobflow_remote.config.base import ExecutionConfig, WorkerBase
from jobflow_remote.jobs.data import FlowInfo, JobInfo
from jobflow_remote.jobs.state import JobState, RemoteState
from jobflow_remote.utils.data import remove_none


def get_job_info_table(jobs_info: list[JobInfo], verbosity: int):
    table = Table(title="Jobs info")
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State [Remote]")
    table.add_column("Job id  (Index)")

    table.add_column("Worker")
    table.add_column("Last updated")

    if verbosity >= 1:
        table.add_column("Queue id")
        table.add_column("Run time")
        table.add_column("Retry time")
        table.add_column("Prev state")
        if verbosity < 2:
            table.add_column("Locked")

    if verbosity >= 2:
        table.add_column("Lock id")
        table.add_column("Lock time")

    excluded_states = (JobState.COMPLETED, JobState.PAUSED)
    for ji in jobs_info:
        state = ji.state.name
        if ji.remote_state is not None and ji.state not in excluded_states:

            if ji.retry_time_limit is not None:
                state += f" [[bold red]{ji.remote_state.name}[/]]"
            else:
                state += f" [{ji.remote_state.name}]"

        row = [
            str(ji.db_id),
            ji.name,
            Text.from_markup(state),
            f"{ji.job_id}  ({ji.job_index})",
            ji.worker,
            ji.last_updated.strftime(fmt_datetime),
        ]

        if verbosity >= 1:
            row.append(ji.queue_job_id)
            prefix = ""
            if ji.remote_state == RemoteState.RUNNING:
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
                ji.retry_time_limit.strftime(fmt_datetime)
                if ji.retry_time_limit
                else None
            )
            row.append(
                ji.remote_previous_state.name if ji.remote_previous_state else None
            )
            if verbosity < 2:
                row.append("*" if ji.lock_id is not None else None)

        if verbosity >= 2:
            row.append(str(ji.lock_id))
            row.append(ji.lock_time.strftime(fmt_datetime) if ji.lock_time else None)

        table.add_row(*row)

    return table


def get_flow_info_table(flows_info: list[FlowInfo], verbosity: int):
    table = Table(title="Flows info")
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State")
    table.add_column("Flow id")
    table.add_column("Num Jobs")
    table.add_column("Last updated")

    if verbosity >= 1:
        table.add_column("Workers")

        table.add_column("Job states")

    for fi in flows_info:
        # show the smallest fw_id as db_id
        db_id = min(fi.db_ids)

        row = [
            str(db_id),
            fi.name,
            fi.state.name,
            fi.flow_id,
            str(len(fi.job_ids)),
            fi.last_updated.strftime(fmt_datetime),
        ]

        if verbosity >= 1:
            workers = set(fi.workers)
            row.append(", ".join(workers))
            job_states = "-".join(js.short_value for js in fi.job_states)
            row.append(job_states)

        table.add_row(*row)

    return table


def format_job_info(job_info: JobInfo, show_none: bool = False):
    d = asdict(job_info)
    if not show_none:
        d = remove_none(d)

    d = jsanitize(d, allow_bson=False, enum_values=True)
    error_remote = d.get("error_remote")
    if error_remote:
        d["error_remote"] = ReprStr(error_remote)
    return render_scope(d)


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
            d = worker.dict()
            d = remove_none(d)
            d = jsanitize(d, allow_bson=False, enum_values=True)
            row.append(render_scope(d))

        table.add_row(*row)

    return table
