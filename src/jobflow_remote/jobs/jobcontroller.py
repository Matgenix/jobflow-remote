from __future__ import annotations

import io
import logging
from contextlib import redirect_stdout
from datetime import datetime, timezone
from typing import cast

from fireworks import Firework
from jobflow import JobStore
from monty.json import MontyDecoder

from jobflow_remote.config.base import Project
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.launchpad import (
    FW_INDEX_PATH,
    FW_UUID_PATH,
    REMOTE_DOC_PATH,
    RemoteLaunchPad,
    get_remote_doc,
)
from jobflow_remote.jobs.data import (
    FlowInfo,
    JobData,
    JobInfo,
    flow_info_projection,
    job_info_projection,
)
from jobflow_remote.jobs.state import FlowState, JobState, RemoteState
from jobflow_remote.utils.db import MongoLock

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
        job_index: int | None = None,
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
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict:
        if state is not None and remote_state is not None:
            raise ValueError("state and remote_state cannot be queried simultaneously")
        if remote_state is not None:
            remote_state = [remote_state]

        if job_ids and not any(isinstance(ji, (list, tuple)) for ji in job_ids):
            # without these cast mypy is confused about the type
            job_ids = cast(list[tuple[str, int]], [job_ids])
        if db_ids is not None and not isinstance(db_ids, (list, tuple)):
            db_ids = [db_ids]

        query: dict = {}

        if db_ids:
            query["fw_id"] = {"$in": db_ids}
        if job_ids:
            job_ids = cast(list[tuple[str, int]], job_ids)
            or_list = []
            for job_id, job_index in job_ids:
                or_list.append({FW_UUID_PATH: job_id, FW_INDEX_PATH: job_index})
            query["$or"] = or_list

        if state:
            fw_states, remote_state = state.to_states()
            query["state"] = {"$in": fw_states}

        if remote_state:
            query[f"{REMOTE_DOC_PATH}.state"] = {
                "$in": [rs.value for rs in remote_state]
            }

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc).isoformat()
            query["updated_on"] = {"$lte": end_date_str}

        if locked:
            query[f"{REMOTE_DOC_PATH}.{MongoLock.LOCK_KEY}"] = {"$exists": True}

        return query

    def _build_query_wf(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | None = None,
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
            query[f"fws.{FW_UUID_PATH}"] = {"$in": job_ids}

        if flow_ids:
            query["metadata.flow_id"] = {"$in": flow_ids}

        if state:
            if state == FlowState.WAITING:
                not_in_states = list(Firework.STATE_RANKS.keys())
                not_in_states.remove("WAITING")
                query["fws.state"] = {"$nin": not_in_states}
            elif state == FlowState.PAUSED:
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
                query[f"fws.{REMOTE_DOC_PATH}.state"] = {
                    "$nin": [JobState.FAILED.value, JobState.REMOTE_ERROR.value]
                }
            elif state == FlowState.FAILED:
                query["$or"] = [
                    {"state": "FIZZLED"},
                    {"$and": [{"state": "DEFUSED"}, {"fws.state": {"$in": ["FIZZLED"]}}]},
                    {
                        f"fws.{REMOTE_DOC_PATH}.state": {
                            "$in": [JobState.FAILED.value, JobState.REMOTE_ERROR.value]
                        }
                    },
                ]
            elif state == FlowState.STOPPED:
                query["state"] = "DEFUSED"
                query["fws.state"] = {"$nin": ["FIZZLED"]}
            else:
                raise RuntimeError("Unknown flow state.")

        # at variance with Firework doc, the dates in the Workflow are Date objects
        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            query["updated_on"] = {"$lte": end_date_str}

        return query

    def get_jobs_data(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
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

        data = self.rlpad.fireworks.find(query, sort=sort, limit=limit)
        jobs_data = []
        for fw_dict in data:
            # deserialize the task to get the objects
            decoded_task = MontyDecoder().process_decoded(fw_dict["spec"]["_tasks"][0])
            job = decoded_task["job"]
            remote_dict = get_remote_doc(fw_dict)
            remote_state_job = (
                RemoteState(remote_dict["state"]) if remote_dict else None
            )
            state = JobState.from_states(fw_dict["state"], remote_state_job)
            store = decoded_task.get("store") or self.jobstore
            info = JobInfo.from_fw_dict(fw_dict)

            output = None
            if state == RemoteState.COMPLETED and load_output:
                output = store.query_one({"uuid": job.uuid}, load=True)
            jobs_data.append(
                JobData(
                    job=job,
                    state=state,
                    db_id=fw_dict["fw_id"],
                    remote_state=remote_state,
                    store=store,
                    info=info,
                    output=output,
                )
            )

        return jobs_data

    def get_jobs_info_query(
        self,
        query: dict = None,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[JobInfo]:
        data = self.rlpad.fireworks.find(
            query, sort=sort, limit=limit, projection=job_info_projection
        )

        jobs_data = []
        for d in data:
            jobs_data.append(JobInfo.from_fw_dict(d))

        return jobs_data

    def get_jobs_info(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        locked: bool = False,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[JobInfo]:
        query = self._build_query_fw(
            job_ids=job_ids,
            db_ids=db_ids,
            state=state,
            remote_state=remote_state,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
        )
        return self.get_jobs_info_query(query=query, sort=sort, limit=limit)

    def get_job_info(
        self,
        job_id: str | None,
        db_id: int | None,
        job_index: int | None = None,
        full: bool = False,
    ) -> JobInfo | None:
        query, sort = self.rlpad.generate_id_query(db_id, job_id, job_index)

        if full:
            proj = dict(job_info_projection)
            proj.update(
                {
                    "launch.action.stored_data": 1,
                    f"{REMOTE_DOC_PATH}.error": 1,
                }
            )
            data = list(
                self.rlpad.get_fw_launch_remote_run_data(
                    query=query, projection=proj, sort=sort, limit=1
                )
            )
        else:
            data = list(
                self.rlpad.fireworks.find(
                    query, projection=job_info_projection, sort=sort, limit=1
                )
            )
        if not data:
            return None

        return JobInfo.from_fw_dict(data[0])

    def rerun_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
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

    def set_remote_state(
        self,
        state: RemoteState,
        job_id: str | None,
        db_id: int | None,
        job_index: int | None = None,
    ) -> bool:
        values = {
            "state": state.value,
            "step_attempts": 0,
            "retry_time_limit": None,
            "previous_state": None,
            "queue_state": None,
            "error": None,
        }
        return self.rlpad.set_remote_values(
            values=values, job_id=job_id, fw_id=db_id, job_index=job_index
        )

    def reset_remote_attempts(
        self, job_id: str | None, db_id: int | None, job_index: int | None = None
    ) -> bool:
        values = {
            "step_attempts": 0,
            "retry_time_limit": None,
        }
        return self.rlpad.set_remote_values(
            values=values, job_id=job_id, fw_id=db_id, job_index=job_index
        )

    def reset_failed_state(
        self, job_id: str | None, db_id: int | None, job_index: int | None = None
    ) -> bool:
        return self.rlpad.reset_failed_state(
            job_id=job_id, fw_id=db_id, job_index=job_index
        )

    def get_flows_info(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[FlowInfo]:
        query = self._build_query_wf(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
        )

        data = self.rlpad.get_wf_fw_data(
            query=query, sort=sort, limit=limit, projection=flow_info_projection
        )

        jobs_data = []
        for d in data:
            jobs_data.append(FlowInfo.from_query_dict(d))

        return jobs_data

    def delete_flows(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
    ):
        if (job_ids is None) == (db_ids is None):
            raise ValueError(
                "One and only one among job_ids and db_ids should be defined"
            )

        if job_ids:
            ids_list: str | int | list = job_ids
            arg = "job_id"
        else:
            ids_list = db_ids
            arg = "fw_id"

        if not isinstance(ids_list, (list, tuple)):
            ids_list = [ids_list]
        for jid in ids_list:
            try:
                # the fireworks launchpad has "print" in it for the out. Capture it
                # to avoid exposing Fireworks output
                with redirect_stdout(io.StringIO()):
                    self.rlpad.delete_wf(**{arg: jid})
            except ValueError as e:
                logger.warning(f"Error while deleting flow: {getattr(e, 'message', e)}")

    def remove_lock(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        state: JobState | None = None,
        remote_state: RemoteState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> int:
        query = self._build_query_fw(
            job_ids=job_ids,
            db_ids=db_ids,
            state=state,
            remote_state=remote_state,
            start_date=start_date,
            end_date=end_date,
            locked=True,
        )

        return self.rlpad.remove_lock(query=query)
