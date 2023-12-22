from __future__ import annotations

from enum import Enum


class JobState(Enum):
    """
    States of a Job
    """

    WAITING = "WAITING"
    READY = "READY"
    CHECKED_OUT = "CHECKED_OUT"  # TODO should it be RESERVED?
    UPLOADED = "UPLOADED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    DOWNLOADED = "DOWNLOADED"
    REMOTE_ERROR = "REMOTE_ERROR"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    USER_STOPPED = "USER_STOPPED"
    BATCH_SUBMITTED = "BATCH_SUBMITTED"
    BATCH_RUNNING = "BATCH_RUNNING"

    @property
    def short_value(self) -> str:
        return short_state_mapping[self]


short_state_mapping = {
    JobState.WAITING: "W",
    JobState.READY: "R",
    JobState.CHECKED_OUT: "CE",
    JobState.UPLOADED: "U",
    JobState.SUBMITTED: "SU",
    JobState.RUNNING: "RU",
    JobState.TERMINATED: "T",
    JobState.DOWNLOADED: "D",
    JobState.REMOTE_ERROR: "RERR",
    JobState.COMPLETED: "C",
    JobState.FAILED: "F",
    JobState.PAUSED: "P",
    JobState.STOPPED: "ST",
    JobState.USER_STOPPED: "CA",
    JobState.BATCH_SUBMITTED: "BS",
    JobState.BATCH_RUNNING: "BR",
}


PAUSABLE_STATES = [
    JobState.READY,
    JobState.WAITING,
]

PAUSABLE_STATES_V = [s.value for s in PAUSABLE_STATES]

RUNNING_STATES = [
    JobState.CHECKED_OUT,
    JobState.UPLOADED,
    JobState.SUBMITTED,
    JobState.RUNNING,
    JobState.TERMINATED,
    JobState.DOWNLOADED,
]

RUNNING_STATES_V = [s.value for s in RUNNING_STATES]

RESETTABLE_STATES = RUNNING_STATES

RESETTABLE_STATES_V = RUNNING_STATES_V


class FlowState(Enum):
    """
    States of a Flow.
    """

    WAITING = "WAITING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"

    @classmethod
    def from_jobs_states(
        cls, jobs_states: list[JobState], leaf_states: list[JobState]
    ) -> FlowState:
        """
        Generate the state of the Flow based on the states of the Jobs
        composing it, and in particular the states of the leaf Jobs.

        Parameters
        ----------
        jobs_states
            List of JobStates of all the Jobs in the Flow.
        leaf_states
            List of JobStates of the leaf Jobs in the Flow.

        Returns
        -------
        FlowState
            The state of the Flow.
        """
        if all(js == JobState.WAITING for js in jobs_states):
            return cls.WAITING
        elif all(js in (JobState.WAITING, JobState.READY) for js in jobs_states):
            return cls.READY
        # only need to check the leaf states to determine if it is completed,
        # in case some intermediate Job failed but children allow missing
        # references.
        elif all(js == JobState.COMPLETED for js in leaf_states):
            return cls.COMPLETED
        # REMOTE_ERROR state does not lead to a failed Flow. Two main reasons:
        # 1) it might be a temporary problem and not a final failure of the Flow
        # 2) Changing the state of the flow would require locking the Flow
        #    when applying the change in the remote state.
        elif any(js == JobState.FAILED for js in jobs_states):
            return cls.FAILED
        elif any(js in (JobState.STOPPED, JobState.USER_STOPPED) for js in jobs_states):
            return cls.STOPPED
        elif any(js == JobState.PAUSED for js in jobs_states):
            return cls.PAUSED
        else:
            return cls.RUNNING
