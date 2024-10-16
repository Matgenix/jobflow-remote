"""Scheduling tools based on the schedule module."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from schedule import Scheduler

if TYPE_CHECKING:
    from jobflow import Job

logger = logging.getLogger(__name__)


# TODO consider making this with an exponential backoff strategy
# with a failure at the end
class SafeScheduler(Scheduler):
    """
    An implementation of Scheduler that catches jobs that fail, logs their
    exception tracebacks as errors, optionally reschedules the jobs for their
    next run time, and keeps going.

    Adapted from https://gist.github.com/mplewis/8483f1c24f2d6259aef6
    """

    def __init__(
        self, reschedule_on_failure: bool = True, seconds_after_failure: int = 0
    ) -> None:
        """
        If reschedule_on_failure is True, jobs will be rescheduled for their
        next run as if they had completed successfully. If False, they'll run
        on the next run_pending() tick.
        """
        self.reschedule_on_failure = reschedule_on_failure
        self.seconds_after_failure = seconds_after_failure
        super().__init__()

    def _run_job(self, job: Job) -> None:
        try:
            super()._run_job(job)
        except Exception:
            task_name = job.job_func.__name__
            logger.exception(f"Error while running task {task_name}")
            if self.reschedule_on_failure:
                if secs := self.seconds_after_failure:
                    logger.warning(f"Task {task_name} rescheduled in {secs} seconds")
                    job.last_run = None
                    job.next_run = datetime.now() + timedelta(seconds=secs)
                else:
                    logger.warning(f"Task {task_name} rescheduled")
                    job.last_run = datetime.now()
                    job._schedule_next_run()
            else:
                logger.warning(f"Task {task_name} canceled.")
                self.cancel_job(job)
