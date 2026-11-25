from __future__ import annotations

import copy
import re
from typing import TYPE_CHECKING, Any

from jobflow_remote.remote.queue import BaseQueueManager

try:
    from qtoolkit.core.data_objects import QCloudResources
except ImportError:  # pragma: no cover - older qtoolkit releases
    QCloudResources = None

if TYPE_CHECKING:
    from collections.abc import Sequence

    from qtoolkit.core.data_objects import CancelResult, QJob, QState, SubmissionResult
    from qtoolkit.io.api.base import BaseAPISchedulerIO


def sanitize_cloud_job_name(name: str, max_len: int = 128) -> str:
    """Sanitize and truncate a job name for cloud schedulers (starts alphanumeric; only ``A-Za-z0-9_-``)."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", name.strip())
    if not cleaned or not cleaned[0].isalnum():
        cleaned = f"jf_{cleaned}"
    return cleaned[:max_len]


class CloudQueueManager(BaseQueueManager):
    """Queue manager delegating directly to a :class:`~qtoolkit.io.api.base.BaseAPISchedulerIO`."""

    def __init__(self, scheduler_io: BaseAPISchedulerIO) -> None:
        self.scheduler_io = scheduler_io

    def build_command_lines(
        self,
        commands: str | list[str],
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
    ) -> list[str]:
        """Build ordered shell command blocks for container execution."""
        lines: list[str] = []
        if pre_str := self.get_pre_run(pre_run):
            lines.append(pre_str)
        if export_str := self.get_export(export):
            lines.append(export_str)
        if modules_str := self.get_modules(modules):
            lines.append(modules_str)
        lines.append(self.get_run_commands(commands))
        if post_str := self.get_post_run(post_run):
            # Preserve the main command's exit status across post_run cleanup.
            lines.append("_jf_rc=$?")
            lines.append(post_str)
            lines.append("exit $_jf_rc")
        return lines

    def prepare_options(
        self,
        options: dict | Any | None = None,
        job_name: str | None = None,
    ) -> Any:
        """Copy ``options`` (``dict``, ``QCloudResources``, or ``None``) and set ``job_name`` if missing."""
        if QCloudResources is None:
            raise ImportError(
                "CloudQueueManager requires a newer qtoolkit release with QCloudResources."
            )

        prepared_options: Any
        if options is None or (isinstance(options, dict) and not options):
            prepared_options = QCloudResources()
        elif isinstance(options, QCloudResources | dict):
            prepared_options = copy.deepcopy(options)
        else:
            raise TypeError("options must be a dict, QCloudResources, or None.")

        if job_name:
            safe_name = sanitize_cloud_job_name(job_name)
            if isinstance(prepared_options, QCloudResources):
                if not prepared_options.job_name:
                    prepared_options.job_name = safe_name
            elif isinstance(prepared_options, dict) and not prepared_options.get(
                "jobName"
            ):
                prepared_options["jobName"] = safe_name

        return prepared_options

    def submit(
        self,
        commands: str | list[str],
        options: dict | Any | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
        job_name: str | None = None,
    ) -> SubmissionResult:
        """Submit a job directly through the cloud scheduler API."""
        cmd_lines = self.build_command_lines(
            commands=commands,
            pre_run=pre_run,
            post_run=post_run,
            export=export,
            modules=modules,
        )
        prepared_options = self.prepare_options(options=options, job_name=job_name)
        return self.scheduler_io.submit(commands=cmd_lines, options=prepared_options)

    def cancel(
        self,
        job: QJob | int | str,
        reason: str | None = None,
    ) -> CancelResult:
        """Cancel or terminate a job through the cloud scheduler API."""
        return self.scheduler_io.cancel(job=job, reason=reason)

    def get_jobs_list(
        self,
        jobs: Sequence[QJob | int | str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
    ) -> list[QJob]:
        """Retrieve jobs from the cloud scheduler API."""
        jobs_arg: Any = (
            jobs if (jobs is None or isinstance(jobs, str | bytes)) else list(jobs)
        )
        return self.scheduler_io.get_jobs_list(
            jobs=jobs_arg,
            user=user,
            queue=queue,
            states=states,
        )
