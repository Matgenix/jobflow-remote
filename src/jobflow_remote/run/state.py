from __future__ import annotations

from enum import Enum


class RemoteState(Enum):
    CHECKED_OUT = "CHECKED_OUT"
    UPLOADED = "UPLOADED"
    SUBMITTED = "SUBMITTED"
    TERMINATED = "TERMINATED"
    DOWNLOADED = "DOWNLOADED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    PAUSED = "PAUSED"

    @property
    def next(self):
        try:
            return states_order[states_order.index(self) + 1]
        except Exception:
            pass
        raise RuntimeError(f"No next state for state {self.name}")

    @property
    def previous(self):
        try:
            prev_index = states_order.index(self) - 1
            if prev_index >= 0:
                return states_order[prev_index]
        except ValueError:
            raise RuntimeError(f"No previous state for state {self.name}")


states_order = [
    RemoteState.CHECKED_OUT,
    RemoteState.UPLOADED,
    RemoteState.SUBMITTED,
    RemoteState.TERMINATED,
    RemoteState.DOWNLOADED,
    RemoteState.COMPLETED,
]
