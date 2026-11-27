from __future__ import annotations

import copy
from typing import TYPE_CHECKING

from qtoolkit.core.data_objects import QResources

from jobflow_remote.remote.queue import BaseQueueManager

if TYPE_CHECKING:
    from qtoolkit.core.data_objects import (
        BaseQResources,
        CancelResult,
        QJob,
        SubmissionResult,
    )
    from qtoolkit.io.api.base import BaseAPISchedulerIO


class CloudQueueManager(BaseQueueManager):
    """Queue manager for cloud workers, built on a qtoolkit BaseAPISchedulerIO."""

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
        """Return the shell lines that run the job in the container."""
        lines: list[str] = []
        if pre_str := self.get_pre_run(pre_run):
            lines.append(pre_str)
        if export_str := self.get_export(export):
            lines.append(export_str)
        if modules_str := self.get_modules(modules):
            lines.append(modules_str)
        lines.append(self.get_run_commands(commands))
        if post_str := self.get_post_run(post_run):
            # Exit with the status of the job, not of post_run.
            lines.append("_jf_rc=$?")
            lines.append(post_str)
            lines.append("exit $_jf_rc")
        return lines

    def prepare_options(
        self,
        options: dict | BaseQResources | None = None,
        job_name: str | None = None,
    ) -> BaseQResources:
        """Return ``options`` as a new resources object of the scheduler IO.

        A dict holds scheduler options and goes into ``scheduler_kwargs``. An
        empty QResources, which is how a job stores ``resources={}``, means no
        options. ``job_name`` is set if the options do not set it.
        """
        resources_cls = self.scheduler_io.supported_resources_cls
        if options == QResources():
            options = None
        if options is None or isinstance(options, dict):
            prepared = resources_cls[0](scheduler_kwargs=copy.deepcopy(options or {}))
        elif isinstance(options, resources_cls):
            prepared = copy.deepcopy(options)
        else:
            names = " or ".join(cls.__name__ for cls in resources_cls)
            raise TypeError(
                f"{type(self.scheduler_io).__name__} does not support "
                f"{type(options).__name__} resources. Use {names} or a dict of "
                "scheduler options."
            )

        if job_name and not prepared.job_name:
            prepared.job_name = job_name
        return prepared

    def submit(
        self,
        commands: str | list[str],
        options: dict | BaseQResources | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
        job_name: str | None = None,
    ) -> SubmissionResult:
        """Submit a job to the cloud scheduler."""
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
        """Cancel a job on the cloud scheduler."""
        return self.scheduler_io.cancel(job=job, reason=reason)

    def get_jobs_list(
        self,
        jobs: list[QJob | int | str] | None = None,
        user: str | None = None,
    ) -> list[QJob]:
        """Return the jobs from the cloud scheduler."""
        return self.scheduler_io.get_jobs_list(jobs=jobs, user=user)
