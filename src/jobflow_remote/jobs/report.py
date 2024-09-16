from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from jobflow_remote.jobs.data import JobInfo, projection_job_info
from jobflow_remote.jobs.state import FlowState, JobState

if TYPE_CHECKING:
    from jobflow_remote import JobController


@dataclass
class JobTrends:
    """
    Trends of job states over time.
    """

    interval: str
    dates: list[str]
    completed: list[int]
    failed: list[int]
    remote_error: list[int]
    timezone: str

    @property
    def num_intervals(self) -> int:
        return len(self.dates)


@dataclass
class JobsReport:
    """
    A report of the job states.
    """

    state_counts: dict[JobState, int] = field(default_factory=dict)
    trends: JobTrends | None = None
    longest_running: list[JobInfo] = field(default_factory=list)
    worker_utilization: dict[str, int] = field(default_factory=dict)

    @property
    def running(self) -> int:
        """Returns the count of running jobs."""
        return self.state_counts.get(JobState.RUNNING, 0)

    @property
    def completed(self) -> int:
        """Returns the count of completed jobs."""
        return self.state_counts.get(JobState.COMPLETED, 0)

    @property
    def error(self) -> int:
        """Returns the sum of failed, remote error, and paused jobs (i.e., error states)."""
        return sum(
            self.state_counts.get(state, 0)
            for state in [JobState.FAILED, JobState.REMOTE_ERROR]
        )

    @property
    def active(self) -> int:
        """Returns the sum of failed, remote error, and paused jobs (i.e., error states)."""
        return sum(
            self.state_counts.get(state, 0)
            for state in [
                JobState.CHECKED_OUT,
                JobState.UPLOADED,
                JobState.UPLOADED,
                JobState.SUBMITTED,
                JobState.RUNNING,
                JobState.TERMINATED,
                JobState.DOWNLOADED,
                JobState.BATCH_SUBMITTED,
                JobState.BATCH_RUNNING,
            ]
        )

    @classmethod
    def generate_report(
        cls,
        job_controller: JobController,
        interval: str = "days",
        num_intervals: int | None = None,
        timezone: str = "UTC",
    ) -> JobsReport:
        """
        Generates a report of the job states.

        Parameters
        ----------
        job_controller
            The JobController instance to generate the report from.
        interval
            The interval of the trends for the report.
        num_intervals
            The number of intervals to consider.
        timezone
            The timezone to use for the report.

        Returns
        -------
        JobsReport
            A report of the job states.
        """
        now = datetime.utcnow()

        state_counts = job_controller.count_jobs_states(list(JobState))

        # Job trends over time
        states_trends = [JobState.COMPLETED, JobState.FAILED, JobState.REMOTE_ERROR]
        trends_dict = job_controller.get_trends(
            states=states_trends,
            interval=interval,
            num_intervals=num_intervals,
            interval_timezone=timezone,
        )
        trends_dates = sorted(trends_dict)
        trends = JobTrends(
            interval=interval,
            dates=trends_dates,
            completed=[trends_dict[d][JobState.COMPLETED] for d in trends_dates],
            failed=[trends_dict[d][JobState.FAILED] for d in trends_dates],
            remote_error=[trends_dict[d][JobState.REMOTE_ERROR] for d in trends_dates],
            timezone=timezone,
        )

        # Longest running jobs (Top 5)
        projection_longest: dict = {k: 1 for k in projection_job_info}
        projection_longest["duration"] = {"$subtract": [now, "$start_time"]}
        pipeline_longest = [
            {"$match": {"state": "RUNNING"}},
            {"$project": projection_longest},
            {"$sort": {"duration": -1}},
            {"$limit": 5},
        ]
        longest_running_result = job_controller.jobs.aggregate(pipeline_longest)
        longest_running = [
            JobInfo.from_query_output(doc) for doc in longest_running_result
        ]

        # Worker utilization (number of jobs assigned to each worker)
        pipeline_worker_utilization = [
            {"$group": {"_id": "$worker", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        worker_utilization_result = job_controller.jobs.aggregate(
            pipeline_worker_utilization
        )
        worker_utilization = {
            doc["_id"]: doc["count"] for doc in worker_utilization_result
        }

        return cls(
            state_counts=state_counts,
            trends=trends,
            longest_running=longest_running,
            worker_utilization=worker_utilization,
        )


@dataclass
class FlowTrends:
    """
    Trends of flow states over time.
    """

    interval: str
    dates: list[str]
    completed: list[int]
    failed: list[int]
    timezone: str

    @property
    def num_intervals(self) -> int:
        return len(self.dates)


@dataclass
class FlowsReport:
    """
    A report of the flow states.
    """

    state_counts: dict[FlowState, int]
    trends: FlowTrends

    @property
    def running(self) -> int:
        """Returns the count of running flows."""
        return self.state_counts.get(FlowState.RUNNING, 0)

    @property
    def completed(self) -> int:
        """Returns the count of completed flows."""
        return self.state_counts.get(FlowState.COMPLETED, 0)

    @property
    def error(self) -> int:
        """Returns the count of failed flows."""
        return self.state_counts.get(FlowState.FAILED, 0)

    @classmethod
    def generate_report(
        cls,
        job_controller: JobController,
        interval: str = "days",
        num_intervals: int = None,
        timezone: str = "UTC",
    ):
        """
        Generates a report of the flow states.

        Parameters
        ----------
        job_controller
            The JobController instance to generate the report from.
        interval
            The interval of the trends for the report.
        num_intervals
            The number of intervals to consider.
        timezone
            The timezone to use for the report.

        Returns
        -------
        FlowsReport
            A report of the flow states.
        """
        state_counts = job_controller.count_flows_states(list(FlowState))

        trends_dict = job_controller.get_trends(
            states=[FlowState.COMPLETED, FlowState.FAILED],
            interval=interval,
            num_intervals=num_intervals,
            interval_timezone=timezone,
        )
        trends_dates = sorted(trends_dict)
        trends = FlowTrends(
            interval=interval,
            dates=trends_dates,
            completed=[trends_dict[d][FlowState.COMPLETED] for d in trends_dates],
            failed=[trends_dict[d][FlowState.FAILED] for d in trends_dates],
            timezone=timezone,
        )

        # Create report instance
        return cls(state_counts=state_counts, trends=trends)
