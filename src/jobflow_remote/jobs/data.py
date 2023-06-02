from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from jobflow import Job

from jobflow_remote.fireworks.launchpad import fw_uuid
from jobflow_remote.jobs.state import FlowState, JobState, RemoteState


@dataclass
class JobData:
    job: Job
    state: JobState
    db_id: int
    remote_state: RemoteState | None = None
    output: dict | None = None


job_info_projection = {
    "fw_id": 1,
    fw_uuid: 1,
    "state": 1,
    "remote.state": 1,
    "name": 1,
    "updated_on": 1,
    "remote.updated_on": 1,
    "remote.previous_state": 1,
    "remote.lock_id": 1,
    "remote.lock_time": 1,
    "remote.retry_time_limit": 1,
    "remote.process_id": 1,
    "remote.run_dir": 1,
    "spec._tasks.machine": 1,
}


@dataclass
class JobInfo:
    db_id: int
    job_id: str
    state: JobState
    name: str
    last_updated: datetime
    machine: str
    remote_state: RemoteState | None = None
    remote_previous_state: RemoteState | None = None
    lock_id: datetime | None = None
    lock_time: datetime | None = None
    retry_time_limit: datetime | None = None
    queue_job_id: str | None = None
    run_dir: str | None = None
    error_job: str | None = None
    error_remote: str | None = None

    @classmethod
    def from_query_dict(cls, d):
        remote = d.get("remote") or {}
        if remote:
            remote = remote[0]
        remote_state_val = remote.get("state")
        remote_state = (
            RemoteState(remote_state_val) if remote_state_val is not None else None
        )
        state = JobState.from_states(d["state"], remote_state)
        # in FW the date is encoded in a string
        fw_update_date = datetime.fromisoformat(d["updated_on"])
        remote_update_date = remote.get("updated_on")
        if remote_update_date:
            remote_update_date = datetime.fromisoformat(d["updated_on"])
            last_updated = max(fw_update_date, remote_update_date)
        else:
            last_updated = fw_update_date
        # the dates should be in utc time. Convert them to the system time
        last_updated = last_updated.replace(tzinfo=timezone.utc).astimezone(tz=None)
        remote_previous_state_val = remote.get("previous_state")
        remote_previous_state = (
            RemoteState(remote_previous_state_val)
            if remote_previous_state_val is not None
            else None
        )
        lock_id = remote.get("lock_id")
        lock_time = remote.get("lock_time")
        if lock_time is not None:
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

        return cls(
            db_id=d["fw_id"],
            job_id=d["spec"]["_tasks"][0]["job"]["uuid"],
            state=state,
            name=d["name"],
            last_updated=last_updated,
            machine=d["spec"]["_tasks"][0]["machine"],
            remote_state=remote_state,
            remote_previous_state=remote_previous_state,
            lock_id=lock_id,
            lock_time=lock_time,
            retry_time_limit=retry_time_limit,
            queue_job_id=str(remote.get("process_id")),
            run_dir=remote.get("run_dir"),
            error_remote=remote.get("error"),
            error_job=error_job,
        )


flow_info_projection = {
    "fws.fw_id": 1,
    f"fws.{fw_uuid}": 1,
    "fws.state": 1,
    "remote.state": 1,
    "name": 1,
    "updated_on": 1,
    "fws.updated_on": 1,
    "remote.updated_on": 1,
    "fws.spec._tasks.machine": 1,
}


@dataclass
class FlowInfo:
    db_ids: int
    job_ids: str
    state: FlowState
    name: str
    last_updated: datetime
    machines: list[str]
    job_states: list[JobState]
    job_names: list[str]

    # @classmethod
    # def from_query_dict(cls, d):
    #     fws = d.get("fws") or []
    #     remotes = d.get("remote") or []
    #
    #     matched_data = defaultdict(list)
    #     for fw in fws:
    #         matched_data[fw.get(fw_uuid)].append(fw)
    #     for r in remotes:
    #         matched_data[r.get("job_id")].append(r)
    #
    #     machines = []
    #     job_states = []
    #     job_names = []
    #     last_updated_list = []
    #     db_ids = []
    #     job_ids = []
    #     for job_id, (fw, r) in matched_data.items():
    #         job_ids.append(job_id)
    #         db_ids.append(fw.get("fw_id"))
    #
    #         last_updated_list.append(datetime.fromisoformat(d["updated_on"]))
    #         remote_update_date = remote.get("updated_on")
    #         if remote_update_date:
    #             remote_update_date = datetime.fromisoformat(d["updated_on"])
    #             last_updated = max(fw_update_date, remote_update_date)
    #         else:
    #             last_updated = fw_update_date
    #         last_updated_list
    #
    #     # for the last updated field, collect all the dates and take the latest one
