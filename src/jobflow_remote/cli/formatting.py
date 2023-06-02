from __future__ import annotations

from dataclasses import asdict

from monty.json import jsanitize
from rich.scope import render_scope
from rich.table import Table

from jobflow_remote.cli.utils import fmt_datetime
from jobflow_remote.jobs.data import JobInfo
from jobflow_remote.jobs.state import JobState
from jobflow_remote.utils.data import remove_none


def get_job_info_table(jobs_info: list[JobInfo], verbosity: int):
    table = Table(title="Jobs info")
    table.add_column("DB id")
    table.add_column("Name")
    table.add_column("State [Remote]")
    table.add_column("Job id")

    table.add_column("Machine")
    table.add_column("Last updated")

    if verbosity >= 1:
        table.add_column("Queue id")
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

            state += f" [{ji.remote_state.name}]"
        row = [
            str(ji.db_id),
            ji.name,
            state,
            ji.job_id,
            ji.machine,
            ji.last_updated.strftime(fmt_datetime),
        ]

        if verbosity >= 1:
            row.append(ji.queue_job_id)
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
            row.append(ji.lock_id)
            row.append(ji.lock_time.strftime(fmt_datetime) if ji.lock_time else None)

        table.add_row(*row)

    return table


def format_job_info(job_info: JobInfo, show_none: bool = False):
    d = asdict(job_info)
    if not show_none:
        d = remove_none(d)

    d = jsanitize(d, allow_bson=False, enum_values=True)
    return render_scope(d)
