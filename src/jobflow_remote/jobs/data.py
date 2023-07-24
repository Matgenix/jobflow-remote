from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from jobflow import Job, JobStore

from jobflow_remote.fireworks.launchpad import (
    FW_UUID_PATH,
    REMOTE_DOC_PATH,
    get_job_doc,
    get_remote_doc,
)
from jobflow_remote.jobs.state import FlowState, JobState, RemoteState
from jobflow_remote.utils.db import MongoLock


@dataclass
class JobData:
    job: Job
    state: JobState
    db_id: int
    store: JobStore
    info: JobInfo | None = None
    remote_state: RemoteState | None = None
    output: dict | None = None


job_info_projection = {
    "fw_id": 1,
    FW_UUID_PATH: 1,
    "state": 1,
    f"{REMOTE_DOC_PATH}.state": 1,
    "name": 1,
    "updated_on": 1,
    f"{REMOTE_DOC_PATH}.updated_on": 1,
    f"{REMOTE_DOC_PATH}.previous_state": 1,
    f"{REMOTE_DOC_PATH}.{MongoLock.LOCK_KEY}": 1,
    f"{REMOTE_DOC_PATH}.{MongoLock.LOCK_TIME_KEY}": 1,
    f"{REMOTE_DOC_PATH}.retry_time_limit": 1,
    f"{REMOTE_DOC_PATH}.process_id": 1,
    f"{REMOTE_DOC_PATH}.run_dir": 1,
    f"{REMOTE_DOC_PATH}.start_time": 1,
    f"{REMOTE_DOC_PATH}.end_time": 1,
    "spec._tasks.worker": 1,
    "spec._tasks.job.hosts": 1,
}


@dataclass
class JobInfo:
    db_id: int
    job_id: str
    state: JobState
    name: str
    last_updated: datetime
    worker: str
    remote_state: RemoteState | None = None
    remote_previous_state: RemoteState | None = None
    lock_id: str | None = None
    lock_time: datetime | None = None
    retry_time_limit: datetime | None = None
    queue_job_id: str | None = None
    run_dir: str | None = None
    error_job: str | None = None
    error_remote: str | None = None
    host_flows_ids: list[str] = field(default_factory=lambda: list())
    start_time: datetime | None = None
    end_time: datetime | None = None

    @classmethod
    def from_fw_dict(cls, d):
        remote = get_remote_doc(d)
        remote_state_val = remote.get("state")
        remote_state = (
            RemoteState(remote_state_val) if remote_state_val is not None else None
        )
        state = JobState.from_states(d["state"], remote_state)
        if isinstance(d["updated_on"], str):
            last_updated = datetime.fromisoformat(d["updated_on"])
        else:
            last_updated = d["updated_on"]
        # the dates should be in utc time. Convert them to the system time
        last_updated = last_updated.replace(tzinfo=timezone.utc).astimezone(tz=None)
        remote_previous_state_val = remote.get("previous_state")
        remote_previous_state = (
            RemoteState(remote_previous_state_val)
            if remote_previous_state_val is not None
            else None
        )
        lock_id = remote.get(MongoLock.LOCK_KEY)
        lock_time = remote.get(MongoLock.LOCK_TIME_KEY)
        if lock_time is not None:
            # TODO when updating the state of a Firework fireworks replaces the dict
            #  with its serialized version, where dates are replaced by strings.
            # Intercept those cases and convert back to dates. This should be removed
            # if fireworks is replaced by another tool.
            if isinstance(lock_time, str):
                lock_time = datetime.fromisoformat(lock_time)
            lock_time = lock_time.replace(tzinfo=timezone.utc).astimezone(tz=None)
        retry_time_limit = remote.get("retry_time_limit")
        if retry_time_limit is not None:
            retry_time_limit = retry_time_limit.replace(tzinfo=timezone.utc).astimezone(
                tz=None
            )

        error_job = None
        launch = d.get("launch") or {}
        if launch:
            launch = launch[0]
            stored_data = launch.get("action", {}).get("stored_data", {})
            message = stored_data.get("_message")
            stack_strace = stored_data.get("_exception", {}).get("_stacktrace")
            if message or stack_strace:
                error_job = f"Message: {message}\nStack trace:\n{stack_strace}"

        queue_job_id = remote.get("process_id")
        if queue_job_id is not None:
            # convert to string in case the format is the one of an integer
            queue_job_id = str(queue_job_id)

        start_time = remote.get("start_time")
        if start_time:
            start_time = start_time.replace(tzinfo=timezone.utc).astimezone(tz=None)
        end_time = remote.get("end_time")
        if end_time:
            end_time = end_time.replace(tzinfo=timezone.utc).astimezone(tz=None)

        return cls(
            db_id=d["fw_id"],
            job_id=d["spec"]["_tasks"][0]["job"]["uuid"],
            state=state,
            name=d["name"],
            last_updated=last_updated,
            worker=d["spec"]["_tasks"][0]["worker"],
            remote_state=remote_state,
            remote_previous_state=remote_previous_state,
            lock_id=lock_id,
            lock_time=lock_time,
            retry_time_limit=retry_time_limit,
            queue_job_id=queue_job_id,
            run_dir=remote.get("run_dir"),
            error_remote=remote.get("error"),
            error_job=error_job,
            host_flows_ids=d["spec"]["_tasks"][0]["job"]["hosts"],
            start_time=start_time,
            end_time=end_time,
        )

    @property
    def run_time(self) -> float | None:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()

        return None

    @property
    def estimated_run_time(self) -> float | None:
        if self.start_time:
            return (
                datetime.now(tz=self.start_time.tzinfo) - self.start_time
            ).total_seconds()

        return None


flow_info_projection = {
    "fws.fw_id": 1,
    f"fws.{FW_UUID_PATH}": 1,
    "fws.state": 1,
    "fws.name": 1,
    f"fws.{REMOTE_DOC_PATH}.state": 1,
    "name": 1,
    "updated_on": 1,
    "fws.updated_on": 1,
    "fws.spec._tasks.worker": 1,
    "metadata.flow_id": 1,
}


@dataclass
class FlowInfo:
    db_ids: list[int]
    job_ids: list[str]
    flow_id: str
    state: FlowState
    name: str
    last_updated: datetime
    workers: list[str]
    job_states: list[JobState]
    job_names: list[str]

    @classmethod
    def from_query_dict(cls, d):
        # the dates should be in utc time. Convert them to the system time
        updated_on = d["updated_on"]
        if isinstance(updated_on, str):
            updated_on = datetime.fromisoformat(updated_on)
        last_updated = updated_on.replace(tzinfo=timezone.utc).astimezone(tz=None)
        flow_id = d["metadata"].get("flow_id")
        fws = d.get("fws") or []
        workers = []
        job_states = []
        job_names = []
        db_ids = []
        job_ids = []
        for fw_doc in fws:
            db_ids.append(fw_doc["fw_id"])
            job_doc = get_job_doc(fw_doc)
            remote_doc = get_remote_doc(fw_doc)
            job_ids.append(job_doc["uuid"])
            job_names.append(fw_doc["name"])
            if remote_doc:
                remote_state = RemoteState(remote_doc["state"])
            else:
                remote_state = None
            fw_state = fw_doc["state"]
            job_states.append(JobState.from_states(fw_state, remote_state))
            workers.append(fw_doc["spec"]["_tasks"][0]["worker"])

        state = FlowState.from_jobs_states(job_states)

        return cls(
            db_ids=db_ids,
            job_ids=job_ids,
            flow_id=flow_id,
            state=state,
            name=d["name"],
            last_updated=last_updated,
            workers=workers,
            job_states=job_states,
            job_names=job_names,
        )
