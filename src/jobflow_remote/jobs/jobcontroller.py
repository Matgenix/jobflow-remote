from __future__ import annotations

import logging
from datetime import datetime, timezone

from fireworks import Firework
from jobflow import JobStore

from jobflow_remote.config.base import Project
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.launchpad import RemoteLaunchPad
from jobflow_remote.jobs.data import JobData, JobInfo, job_info_projection
from jobflow_remote.jobs.state import FlowState, JobState, RemoteState

logger = logging.getLogger(__name__)


class JobController:
    def __init__(
        self, project_name: str | None = None, jobstore: JobStore | None = None
    ):
        self.project_name = project_name
        self.config_manager: ConfigManager = ConfigManager()
        self.project: Project = self.config_manager.get_project(project_name)
        self.rlpad: RemoteLaunchPad = self.project.get_launchpad()
        if not jobstore:
            jobstore = self.project.get_jobstore()
        self.jobstore = jobstore
        self.jobstore.connect()

    def get_job_data(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        load_output: bool = False,
    ):
        fw, remote_run = self.rlpad.get_fw_remote_run_from_id(
            job_id=job_id, fw_id=db_id
        )
        job = fw.tasks[0].get("job")
        state = JobState.from_states(fw.state, remote_run.state if remote_run else None)
        output = None
        jobstore = fw.tasks[0].get("store") or self.jobstore
        if load_output and state == RemoteState.COMPLETED:
            output = jobstore.query_one({"uuid": job_id}, load=True)

        return JobData(job=job, state=state, db_id=fw.fw_id, output=output)

    def _build_query_fw(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        if state is not None and remote_state is not None:
            raise ValueError("state and remote_state cannot be queried simultaneously")
        if remote_state is not None:
            remote_state = [remote_state]

        if job_ids is not None and not isinstance(job_ids, (list, tuple)):
            job_ids = [job_ids]
        if db_ids is not None and not isinstance(db_ids, (list, tuple)):
            db_ids = [db_ids]

        query: dict = {}

        if db_ids:
            query["fw_id"] = {"$in": db_ids}
        if job_ids:
            query["spec._tasks.job.uuid"] = {"$in": job_ids}

        if state:
            fw_states, remote_state = state.to_states()
            query["state"] = {"$in": fw_states}

        if remote_state:
            query["remote.state"] = {"$in": [rs.value for rs in remote_state]}

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$lte": end_date_str}

        return query

    def _build_query_wf(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:

        if job_ids is not None and not isinstance(job_ids, (list, tuple)):
            job_ids = [job_ids]
        if db_ids is not None and not isinstance(db_ids, (list, tuple)):
            db_ids = [db_ids]

        query: dict = {}

        if db_ids:
            query["nodes"] = {"$in": db_ids}
        if job_ids:
            query["fws.spec._tasks.job.uuid"] = {"$in": job_ids}

        if state:
            if state == FlowState.WAITING:
                not_in_states = list(Firework.STATE_RANKS.keys())
                not_in_states.remove("WAITING")
                query["fws.state"] = {"$nin": not_in_states}
            if state == FlowState.PAUSED:
                not_in_states = list(Firework.STATE_RANKS.keys())
                not_in_states.remove("PAUSED")
                query["fws.state"] = {"$nin": not_in_states}
            elif state == FlowState.READY:
                query["state"] = "READY"
            elif state == FlowState.COMPLETED:
                query["state"] = "COMPLETED"
            elif state == FlowState.ONGOING:
                query["state"] = "RUNNING"
                query["fws.state"] = {"$in": ["RUNNING", "RESERVED"]}
                query["remote.state"] = {
                    "$nin": [JobState.FAILED.value, JobState.REMOTE_ERROR.value]
                }
            elif state == FlowState.FAILED:
                query["$or"] = [
                    {"state": "FIZZLED"},
                    {
                        "remote.state": {
                            "$in": [JobState.FAILED.value, JobState.REMOTE_ERROR.value]
                        }
                    },
                ]

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$lte": end_date_str}

        return query

    def get_jobs_data(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sort: dict | None = None,
        limit: int = 0,
        load_output: bool = False,
    ) -> list[JobData]:
        query = self._build_query_fw(
            job_ids=job_ids,
            db_ids=db_ids,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
        )

        data = self.rlpad.get_fw_remote_run(query=query, sort=sort, limit=limit)

        jobs_data = []
        for fw, remote in data:
            job = fw.tasks[0]["job"]
            remote_state_job = remote.state if remote else None
            state = JobState.from_states(fw.state, remote_state_job)
            output = None
            jobstore = fw.tasks[0].get("store") or self.jobstore
            if state == RemoteState.COMPLETED and load_output:
                output = jobstore.query_one({"uuid": job.uuid}, load=True)
            jobs_data.append(
                JobData(
                    job=job,
                    state=state,
                    db_id=fw.fw_id,
                    remote_state=remote_state,
                    output=output,
                )
            )

        return jobs_data

    def get_jobs_info(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[JobInfo]:
        query = self._build_query_fw(
            job_ids=job_ids,
            db_ids=db_ids,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
        )

        data = self.rlpad.get_fw_remote_run_data(
            query=query, sort=sort, limit=limit, projection=job_info_projection
        )

        jobs_data = []
        for d in data:
            jobs_data.append(JobInfo.from_query_dict(d))

        return jobs_data

    def get_job_info(
        self, job_id: str | None, db_id: int | None, full: bool = False
    ) -> JobInfo | None:
        if (job_id is None) == (db_id is None):
            raise ValueError(
                "One and only one among job_id and db_id should be defined"
            )
        query = self._build_query_fw(job_ids=job_id, db_ids=db_id)

        if full:
            proj = dict(job_info_projection)
            proj.update(
                {
                    "launch.action.stored_data": 1,
                    "remote.error": 1,
                }
            )
            data = self.rlpad.get_fw_launch_remote_run_data(
                query=query, projection=proj
            )
        else:
            data = self.rlpad.get_fw_remote_run_data(
                query=query, projection=job_info_projection
            )
        if not data:
            return None

        return JobInfo.from_query_dict(data[0])

    def rerun_jobs(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[int]:
        query = self._build_query_fw(
            job_ids=job_ids,
            db_ids=db_ids,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
        )

        fw_ids = self.rlpad.get_fw_ids(query=query, sort=sort, limit=limit)
        for fw_id in fw_ids:
            self.rlpad.rerun_fw(fw_id=fw_id)

        return fw_ids

    def reset(self, reset_output: bool = False, max_limit: int = 25) -> bool:

        password = datetime.now().strftime("%Y-%m-%d") if max_limit == 0 else None
        try:
            self.rlpad.reset(
                password, require_password=False, max_reset_wo_password=max_limit
            )
        except ValueError as e:
            logger.info(f"database was not reset due to: {repr(e)}")
            return False
        # TODO it should just delete docs related to job removed in the rlpad.reset?
        # what if the outputs are in other stores? Should take those as well
        if reset_output:
            self.jobstore.remove_docs({})

        return True
