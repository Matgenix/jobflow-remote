from __future__ import annotations

import copy
import re
from typing import TYPE_CHECKING, Any

try:
    from qtoolkit.core.data_objects import QCloudResources
except ImportError:  # pragma: no cover - older qtoolkit releases
    QCloudResources = None

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from qtoolkit.core.data_objects import CancelResult, QJob, QState, SubmissionResult
    from qtoolkit.io.api.base import BaseAPISchedulerIO

    from jobflow_remote.remote.storage.base import BaseCloudStorage


def sanitize_cloud_job_name(name: str, max_len: int = 128) -> str:
    """Sanitize a job name for cloud batch schedulers (alphanumeric, ``-``, ``_``)."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", name.strip())
    if not cleaned or not cleaned[0].isalnum():
        cleaned = f"jf_{cleaned}"
    return cleaned[:max_len]


class CloudQueueManager:
    """Queue manager for API-based cloud schedulers backed by cloud object storage.

    Parallel to :class:`~jobflow_remote.remote.queue.QueueManager`, but calls a
    :class:`~qtoolkit.io.api.base.BaseAPISchedulerIO` directly instead of
    generating submission scripts and invoking CLI commands over a host shell.
    """

    def __init__(
        self,
        scheduler_io: BaseAPISchedulerIO,
        storage: BaseCloudStorage,
        container_work_dir: str = "/tmp/jf_run",  # noqa: S108
    ) -> None:
        self.scheduler_io = scheduler_io
        self.storage = storage
        self.container_work_dir = container_work_dir

    def get_run_commands(
        self,
        commands: str | list[str],
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
    ) -> list[str]:
        """Build the list of shell lines to run inside the cloud container."""
        lines: list[str] = []
        if pre_run:
            if isinstance(pre_run, (list, tuple)):
                lines.extend(pre_run)
            else:
                lines.append(pre_run)
        if export:
            for k, v in export.items():
                lines.append(f"export {k}={v}")
        if modules:
            lines.extend(f"module load {m}" for m in modules)

        if isinstance(commands, str):
            lines.append(commands)
        elif isinstance(commands, list):
            lines.extend(commands)
        else:
            raise TypeError("commands should be a str or a list of str.")

        if post_run:
            lines.append("_jf_rc=$?")
            if isinstance(post_run, (list, tuple)):
                lines.extend(post_run)
            else:
                lines.append(post_run)
            lines.append("exit $_jf_rc")
        return lines

    def prepare_options(
        self,
        options: dict | Any | None = None,
        job_name: str | None = None,
    ) -> Any:
        """Validate and prepare scheduler options for cloud job submission."""
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
        work_dir: str | Path | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
        job_name: str | None = None,
        **_kwargs,
    ) -> SubmissionResult:
        """Submit a job directly through the cloud scheduler API."""
        cmd_lines = self.get_run_commands(
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
        timeout: int | None = None,
    ) -> CancelResult:
        """Cancel or terminate a job through the cloud scheduler API."""
        return self.scheduler_io.cancel(job=job, reason=reason)

    def get_job(self, job: QJob | int | str, timeout: int | None = None) -> QJob | None:
        """Retrieve a single job from the cloud scheduler API."""
        return self.scheduler_io.get_job(job=job)

    def get_jobs_list(
        self,
        jobs: Sequence[QJob | int | str] | None = None,
        user: str | None = None,
        queue: str | None = None,
        states: list[QState] | None = None,
        timeout: int | None = None,
    ) -> list[QJob]:
        """Retrieve jobs from the cloud scheduler API."""
        if isinstance(jobs, str | bytes):
            raise TypeError(
                "jobs must be a sequence of job IDs or QJob objects, not a string or bytes."
            )
        jobs_list = list(jobs) if jobs is not None else None
        return self.scheduler_io.get_jobs_list(
            jobs=jobs_list,
            user=user,
            queue=queue,
            states=states,
        )
