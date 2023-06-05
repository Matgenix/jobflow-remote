from __future__ import annotations

from enum import Enum


class RemoteState(Enum):
    CHECKED_OUT = "CHECKED_OUT"
    UPLOADED = "UPLOADED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    DOWNLOADED = "DOWNLOADED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    PAUSED = "PAUSED"

    @property
    def next(self):
        try:
            return remote_states_order[remote_states_order.index(self) + 1]
        except Exception:
            pass
        raise RuntimeError(f"No next state for state {self.name}")

    @property
    def previous(self):
        try:
            prev_index = remote_states_order.index(self) - 1
            if prev_index >= 0:
                return remote_states_order[prev_index]
        except ValueError:
            raise RuntimeError(f"No previous state for state {self.name}")


remote_states_order = [
    RemoteState.CHECKED_OUT,
    RemoteState.UPLOADED,
    RemoteState.SUBMITTED,
    RemoteState.RUNNING,
    RemoteState.TERMINATED,
    RemoteState.DOWNLOADED,
    RemoteState.COMPLETED,
]


class JobState(Enum):
    WAITING = "WAITING"
    READY = "READY"
    ONGOING = "ONGOING"
    REMOTE_ERROR = "REMOTE_ERROR"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"

    @classmethod
    def from_states(
        cls, fw_state: str, remote_state: RemoteState | None = None
    ) -> JobState:
        if fw_state in ("WAITING", "READY", "COMPLETED", "PAUSED"):
            return JobState(fw_state)
        elif fw_state in ("RESERVED", "RUNNING"):
            if remote_state == RemoteState.FAILED:
                return JobState.REMOTE_ERROR
            else:
                return JobState.ONGOING
        elif fw_state == "FIZZLED":
            return JobState.FAILED

        raise ValueError(f"Unsupported FW state {fw_state}")

    def to_states(self) -> tuple[list[str], list[RemoteState] | None]:
        if self in (JobState.WAITING, JobState.READY):
            return [self.value], None
        elif self in (JobState.COMPLETED, JobState.PAUSED):
            return [self.value], [RemoteState(self.value)]
        elif self == JobState.ONGOING:
            return ["RESERVED", "RUNNING"], list(remote_states_order)
        elif self == JobState.REMOTE_ERROR:
            return ["RESERVED", "RUNNING"], [RemoteState.FAILED]
        elif self == JobState.FAILED:
            return ["FIZZLED"], [RemoteState.COMPLETED]

        raise ValueError(f"Unhandled state {self}")

    @property
    def short_value(self) -> str:
        if self == JobState.REMOTE_ERROR:
            return "RE"
        return self.value[0]


class FlowState(Enum):
    WAITING = "WAITING"
    READY = "READY"
    ONGOING = "ONGOING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PAUSED = "PAUSED"

    @classmethod
    def from_jobs_states(cls, jobs_states: list[JobState]) -> FlowState:
        if all(js == JobState.WAITING for js in jobs_states):
            return cls.WAITING
        elif all(js in (JobState.WAITING, JobState.READY) for js in jobs_states):
            return cls.READY
        elif all(js == JobState.COMPLETED for js in jobs_states):
            return cls.COMPLETED
        elif any(js in (JobState.FAILED, JobState.REMOTE_ERROR) for js in jobs_states):
            return cls.FAILED
        elif all(js == JobState.PAUSED for js in jobs_states):
            return cls.PAUSED
        else:
            return cls.ONGOING
