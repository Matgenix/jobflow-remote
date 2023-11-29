from __future__ import annotations

import contextlib
import fnmatch
import logging
import traceback
import warnings
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, cast

import jobflow
import pymongo
from jobflow import JobStore, OnMissing
from maggma.stores import MongoStore
from monty.serialization import loadfn
from qtoolkit.core.data_objects import CancelStatus, QResources

from jobflow_remote.config.base import ConfigError, ExecutionConfig, Project
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.jobs.data import (
    OUT_FILENAME,
    DynamicResponseType,
    FlowDoc,
    FlowInfo,
    JobDoc,
    JobInfo,
    RemoteError,
    get_initial_flow_doc_dict,
    get_initial_job_doc_dict,
    get_reset_job_base_dict,
    projection_job_info,
)
from jobflow_remote.jobs.state import (
    PAUSABLE_STATES,
    RESETTABLE_STATES,
    RUNNING_STATES,
    FlowState,
    JobState,
)
from jobflow_remote.remote.data import get_remote_store, update_store
from jobflow_remote.remote.queue import QueueManager
from jobflow_remote.utils.data import deep_merge_dict
from jobflow_remote.utils.db import FlowLockedError, JobLockedError, MongoLock

logger = logging.getLogger(__name__)


class JobController:
    def __init__(
        self,
        queue_store: MongoStore,
        jobstore: JobStore,
        flows_collection: str = "flows",
        auxiliary_collection: str = "jf_auxiliary",
        project: Project | None = None,
    ):
        self.queue_store = queue_store
        self.jobstore = jobstore
        self.jobs_collection = self.queue_store.collection_name
        self.flows_collection = flows_collection
        self.auxiliary_collection = auxiliary_collection
        # TODO should it connect here? Or the passed stores should be connected?
        self.queue_store.connect()
        self.jobstore.connect()
        self.db = self.queue_store._collection.database
        self.jobs = self.queue_store._collection
        self.flows = self.db[self.flows_collection]
        self.auxiliary = self.db[self.auxiliary_collection]
        self.project = project

    @classmethod
    def from_project_name(cls, project_name: str | None = None):
        config_manager: ConfigManager = ConfigManager()
        project: Project = config_manager.get_project(project_name)
        queue_store = project.get_queue_store()
        jobstore = project.get_jobstore()
        return cls(queue_store=queue_store, jobstore=jobstore, project=project)

    @classmethod
    def from_project(cls, project: Project):
        queue_store = project.get_queue_store()
        jobstore = project.get_jobstore()
        return cls(queue_store=queue_store, jobstore=jobstore, project=project)

    def close(self):
        try:
            self.queue_store.close()
        except Exception:
            logger.error(
                "Error while closing the connection to the queue store", exc_info=True
            )

        try:
            self.jobstore.close()
        except Exception:
            logger.error(
                "Error while closing the connection to the job store", exc_info=True
            )

    def _build_query_job(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        if job_ids and not any(isinstance(ji, (list, tuple)) for ji in job_ids):
            # without these cast mypy is confused about the type
            job_ids = cast(list[tuple[str, int]], [job_ids])
        if db_ids is not None and not isinstance(db_ids, (list, tuple)):
            db_ids = [db_ids]
        if flow_ids and not isinstance(flow_ids, (list, tuple)):
            flow_ids = [flow_ids]

        query: dict = {}

        if db_ids:
            query["db_id"] = {"$in": db_ids}
        if job_ids:
            job_ids = cast(list[tuple[str, int]], job_ids)
            or_list = []
            for job_id, job_index in job_ids:
                or_list.append({"uuid": job_id, "index": job_index})
            query["$or"] = or_list

        if flow_ids:
            query["hosts"] = {"$in": flow_ids}

        if state:
            query["state"] = state.value

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            query["updated_on"] = {"$lte": end_date_str}

        if locked:
            query["lock_id"] = {"$ne": None}

        if name:
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            query["name"] = {"$regex": mongo_regex}

        if metadata:
            metadata_dict = {f"metadata.{k}": v for k, v in metadata.items()}
            query.update(metadata_dict)

        return query

    def _build_query_flow(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
    ) -> dict:
        if job_ids is not None and not isinstance(job_ids, (list, tuple)):
            job_ids = [job_ids]
        if db_ids is not None and not isinstance(db_ids, (list, tuple)):
            db_ids = [db_ids]

        query: dict = {}

        if db_ids:
            # the "0" refers to the index in the ids list.
            # needs to be a string, but is correctly recognized by MongoDB
            query["ids"] = {"$elemMatch": {"0": {"$in": db_ids}}}
        if job_ids:
            query["jobs"] = {"$in": job_ids}

        if flow_ids:
            query["uuid"] = {"$in": flow_ids}

        if state:
            query["state"] = state.value

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            query["updated_on"] = {"$lte": end_date_str}

        if name:
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            query["name"] = {"$regex": mongo_regex}

        return query

    def get_jobs_info_query(self, query: dict = None, **kwargs) -> list[JobInfo]:
        data = self.jobs.find(query, projection=projection_job_info, **kwargs)

        jobs_data = []
        for d in data:
            jobs_data.append(JobInfo.from_query_output(d))

        return jobs_data

    def get_jobs_info(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        locked: bool = False,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[JobInfo]:
        query = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
        )
        return self.get_jobs_info_query(query=query, sort=sort, limit=limit)

    def get_jobs_doc_query(self, query: dict = None, **kwargs) -> list[JobDoc]:
        data = self.jobs.find(query, **kwargs)

        jobs_data = []
        for d in data:
            jobs_data.append(JobDoc.model_validate(d))

        return jobs_data

    def get_jobs_doc(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        locked: bool = False,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[JobDoc]:
        query = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
        )
        return self.get_jobs_doc_query(query=query, sort=sort, limit=limit)

    @staticmethod
    def generate_job_id_query(
        db_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> tuple[dict, list | None]:
        query: dict = {}
        sort: list | None = None

        if (job_id is None) == (db_id is None):
            raise ValueError(
                "One and only one among job_id and db_id should be defined"
            )

        if db_id:
            query["db_id"] = db_id
        if job_id:
            query["uuid"] = job_id
            if job_index is None:
                # note: this format is suitable for collection.find(sort=.),
                # but not for $sort in an aggregation.
                sort = [["index", pymongo.DESCENDING]]
            else:
                query["index"] = job_index
        if not query:
            raise ValueError("At least one among db_id and job_id should be specified")
        return query, sort

    def get_job_info(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
    ) -> JobInfo | None:
        query, sort = self.generate_job_id_query(db_id, job_id, job_index)

        data = list(
            self.jobs.find(query, projection=projection_job_info, sort=sort, limit=1)
        )
        if not data:
            return None

        return JobInfo.from_query_output(data[0])

    def _many_jobs_action(
        self,
        method: Callable,
        action_description: str,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        **method_kwargs,
    ) -> list[int]:
        query = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
        )
        result = self.jobs.find(query, projection=["db_id"])

        queried_dbs_ids = [r["db_id"] for r in result]

        updated_ids = set()
        for db_id in queried_dbs_ids:
            try:
                job_updated_ids = method(db_id=db_id, **method_kwargs)
                if job_updated_ids:
                    updated_ids.update(job_updated_ids)
            except Exception:
                if raise_on_error:
                    raise
                logger.error(
                    f"Error while {action_description} for job {db_id}", exc_info=True
                )

        return list(updated_ids)

    def rerun_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        return self._many_jobs_action(
            method=self.rerun_job,
            action_description="rerunning",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            force=force,
            wait=wait,
            break_lock=break_lock,
        )

    def rerun_job(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10

        modified_jobs: list[int] = []
        # the job to rerun is the last to be released since this prevents
        # a checkout of the job while the flow is still locked
        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            projection=["uuid", "index", "db_id", "state"],
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
        ) as job_lock:
            job_doc_dict = job_lock.locked_document
            if not job_doc_dict:
                if job_lock.unavailable_document:
                    raise JobLockedError.from_job_doc(job_lock.unavailable_document)
                raise ValueError(f"No Job document matching criteria {lock_filter}")
            job_state = JobState(job_doc_dict["state"])

            if job_state in [JobState.READY]:
                raise ValueError("The Job is in the READY state. No need to rerun.")
            elif job_state in RESETTABLE_STATES:
                # if in one of the resettable states no need to lock the flow or
                # update children.
                doc_update = self._reset_remote(job_doc_dict)
                modified_jobs = []
            elif (
                job_state not in [JobState.FAILED, JobState.REMOTE_ERROR] and not force
            ):
                raise ValueError(
                    f"Job in state {job_doc_dict['state']} cannot be rerun. "
                    "Use the 'force' option to override this check."
                )
            else:
                # full restart required
                doc_update, modified_jobs = self._full_rerun(
                    job_doc_dict,
                    sleep=sleep,
                    wait=wait,
                    break_lock=break_lock,
                    force=force,
                )

            modified_jobs.append(job_doc_dict["db_id"])

            set_doc = {"$set": doc_update}
            job_lock.update_on_release = set_doc

        return modified_jobs

    def _full_rerun(
        self,
        doc: dict,
        sleep: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        force: bool = False,
    ) -> tuple[dict, list[int]]:
        job_id = doc["uuid"]
        job_index = doc["index"]
        modified_jobs = []

        flow_filter = {"jobs": job_id}
        with self.lock_flow(
            filter=flow_filter,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
            break_lock=break_lock,
        ) as flow_lock:
            if not flow_lock.locked_document:
                if flow_lock.unavailable_document:
                    raise FlowLockedError.from_flow_doc(flow_lock.unavailable_document)
                raise ValueError(f"No Flow document matching criteria {flow_filter}")

            flow_doc = FlowDoc.model_validate(flow_lock.locked_document)

            # only the job with the largest index currently present in the db
            # can be rerun to avoid inconsistencies. (rerunning a smaller index
            # would still leave the job with larger indexes in the DB with no
            # clear way of how to deal with them)
            if max(flow_doc.ids_mapping[job_id]) > job_index:
                raise ValueError(
                    f"Job {job_id} is not the highest index ({job_index}). "
                    "Rerunning it will lead to inconsistencies and is not allowed."
                )

            # check that the all the children only those with the largest index
            # in the flow are present.
            # If that is the case the rerun would lead to inconsistencies.
            # If only the last one is among the children it is acceptable
            # to rerun, but in case of a child with lower index a dynamical
            # action that cannot be reverted has been already applied.
            # Do not allow this even if force==True.
            # if not force, only the first level children need to be checked
            if not force:
                descendants = flow_doc.children.get(job_id, [])
            else:
                descendants = flow_doc.descendants(job_id)
            for dep_id, dep_index in descendants:
                if max(flow_doc.ids_mapping[dep_id]) > dep_index:
                    raise ValueError(
                        f"Job {job_id} has a child job ({dep_id}) which is not the last index ({dep_index}. "
                        "Rerunning the Job will lead to inconsistencies and is not allowed."
                    )

            # TODO should STOPPED be acceptable?
            acceptable_child_states = [
                JobState.READY.value,
                JobState.WAITING.value,
                JobState.PAUSED.value,
            ]
            # Update the state of the descendants
            with ExitStack() as stack:
                # first acquire the lock on all the descendants and
                # check their state if needed. Break immediately if
                # the lock cannot be acquired on one of the children
                # or if the states do not satisfy the requirements
                children_locks = []
                for dep_id, dep_index in descendants:
                    # TODO consider using the db_id for the query. may be faster?
                    child_lock = stack.enter_context(
                        self.lock_job(
                            filter={"uuid": dep_id, "index": dep_index},
                            break_lock=break_lock,
                            projection=["uuid", "index", "db_id", "state"],
                            sleep=sleep,
                            max_wait=wait,
                            get_locked_doc=True,
                        )
                    )
                    child_doc_dict = child_lock.locked_document
                    if not child_doc_dict:
                        if child_lock.unavailable_document:
                            raise JobLockedError.from_job_doc(
                                child_lock.unavailable_document,
                                f"The parent Job with uuid {job_id} cannot be rerun",
                            )
                        raise ValueError(
                            f"The child of Job {job_id} to rerun with uuid {dep_id} and index {dep_index} could not be found in the database"
                        )

                    # check that the children have not been started yet.
                    # the only case being if some children allow failed parents.
                    # Put a lock on each of the children, so that if they are READY
                    # they will not be checked out
                    if (
                        not force
                        and child_doc_dict["state"] not in acceptable_child_states
                    ):
                        msg = (
                            f"The child of Job {job_id} to rerun with uuid {dep_id} and "
                            f"index {dep_index} has state {child_doc_dict['state']} which "
                            "is not acceptable. Use the 'force' option to override this check."
                        )
                        raise ValueError(msg)
                    children_locks.append(child_lock)

                # Here all the descendants are locked and could be set to WAITING.
                # Set the new state for all of them.
                for child_lock in children_locks:
                    if child_lock.locked_document["state"] != JobState.WAITING.value:
                        modified_jobs.append(child_lock.locked_document["db_id"])
                    child_doc_update = get_reset_job_base_dict()
                    child_doc_update["state"] = JobState.WAITING.value
                    child_lock.update_on_release = {"$set": child_doc_update}

            # if everything is fine here, update the state of the flow
            # before releasing its lock and set the update for the original job
            # pass explicitly the new state of the job, since it is not updated
            # in the DB. The Job is the last lock to be released.
            updated_states = {job_id: {job_index: JobState.READY}}
            self.update_flow_state(
                flow_uuid=flow_doc.uuid, updated_states=updated_states
            )

            job_doc_update = get_reset_job_base_dict()
            job_doc_update["state"] = JobState.READY.value

        return job_doc_update, modified_jobs

    def _reset_remote(self, doc: dict) -> dict:
        if doc["state"] in [JobState.SUBMITTED.value, JobState.RUNNING.value]:
            # try cancelling the job submitted to the remote queue
            try:
                self._cancel_queue_process(doc)
            except Exception:
                logger.warning(
                    f"Failed cancelling the process for Job {doc['uuid']} {doc['index']}",
                    exc_info=True,
                )

        job_doc_update = get_reset_job_base_dict()
        job_doc_update["state"] = JobState.CHECKED_OUT.value

        return job_doc_update

    def _set_job_properties(
        self,
        values: dict,
        db_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
    ) -> list[int]:
        sleep = None
        if wait:
            sleep = 10
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        projection = ["db_id", "uuid", "index", "state"]
        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            sleep=sleep,
            max_wait=wait,
            projection=projection,
        ) as lock:
            doc = lock.locked_document
            if doc:
                if (
                    acceptable_states
                    and JobState(doc["state"]) not in acceptable_states
                ):
                    raise ValueError(
                        f"Job in state {doc['state']}. The action cannot be performed"
                    )
                values = dict(values)
                # values["updated_on"] = datetime.utcnow()
                lock.update_on_release = {"$set": values}
                return [doc["db_id"]]

        return []

    def set_job_state(
        self,
        state: JobState,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        values = {
            "state": state.value,
            "remote.step_attempts": 0,
            "remote.retry_time_limit": None,
            "previous_state": None,
            "remote.queue_state": None,
            "remote.error": None,
            "error": None,
        }
        return self._set_job_properties(
            values=values,
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
        )

    def retry_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ):
        return self._many_jobs_action(
            method=self.retry_job,
            action_description="rerunning",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def retry_job(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10

        with self.lock_job(
            filter=lock_filter,
            sort=sort,
            get_locked_doc=True,
            sleep=sleep,
            max_wait=wait,
            break_lock=break_lock,
        ) as lock:
            doc = lock.locked_document
            if not doc:
                if lock.unavailable_document:
                    raise JobLockedError(
                        f"The Job matching criteria {lock_filter} is locked."
                    )
                raise ValueError(f"No Job matching criteria {lock_filter}")
            state = JobState(doc["state"])
            if state == JobState.REMOTE_ERROR:
                previous_state = doc["previous_state"]
                try:
                    JobState(previous_state)
                except ValueError:
                    raise ValueError(
                        f"The registered previous state: {previous_state} is not a valid state"
                    )
                set_dict = get_reset_job_base_dict()
                set_dict["state"] = previous_state

                lock.update_on_release = {"$set": set_dict}
            elif state in RUNNING_STATES:
                set_dict = {
                    "remote.step_attempts": 0,
                    "remote.retry_time_limit": None,
                }
                lock.update_on_release = {"$set": set_dict}
            else:
                raise ValueError(f"Job in state {state.value} cannot be retried.")
            return [doc["db_id"]]

    def pause_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
    ) -> list[int]:
        return self._many_jobs_action(
            method=self.pause_job,
            action_description="pausing",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            wait=wait,
        )

    def cancel_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        return self._many_jobs_action(
            method=self.cancel_job,
            action_description="cancelling",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def cancel_job(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        job_lock_kwargs = dict(
            projection=["uuid", "index", "db_id", "state", "remote", "worker"]
        )
        flow_lock_kwargs = dict(projection=["uuid"])
        with self.lock_job_flow(
            acceptable_states=[JobState.READY] + RUNNING_STATES,
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            job_state = JobState(job_doc["state"])
            if job_state in [JobState.SUBMITTED.value, JobState.RUNNING.value]:
                # try cancelling the job submitted to the remote queue
                try:
                    self._cancel_queue_process(job_doc)
                except Exception:
                    logger.warning(
                        f"Failed cancelling the process for Job {job_doc['uuid']} {job_doc['index']}",
                        exc_info=True,
                    )
            updated_states = {job_id: {job_index: JobState.CANCELLED}}
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"$set": {"state": JobState.CANCELLED.value}}
            return [job_lock.locked_document["db_id"]]

    def pause_job(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
    ) -> list[int]:
        job_lock_kwargs = dict(projection=["uuid", "index", "db_id", "state"])
        flow_lock_kwargs = dict(projection=["uuid"])
        with self.lock_job_flow(
            acceptable_states=PAUSABLE_STATES,
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=False,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            updated_states = {job_id: {job_index: JobState.PAUSED}}
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"$set": {"state": JobState.PAUSED.value}}
            return [job_lock.locked_document["db_id"]]

    def play_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        return self._many_jobs_action(
            method=self.play_job,
            action_description="playing",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def play_job(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        job_lock_kwargs = dict(
            projection=["uuid", "index", "db_id", "state", "job.config", "parents"]
        )
        flow_lock_kwargs = dict(projection=["uuid"])
        with self.lock_job_flow(
            acceptable_states=[JobState.PAUSED],
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            on_missing = job_doc["job"]["config"]["on_missing_references"]
            allow_failed = on_missing != OnMissing.ERROR.value

            # in principle the lock on each of the parent jobs is not needed
            # since a parent Job cannot change to COMPLETED or FAILED while
            # the flow is locked
            for parent in self.jobs.find(
                {"uuid": {"$in": job_doc["parents"]}}, projection=["state"]
            ):
                parent_state = JobState(parent["state"])
                if parent_state != JobState.COMPLETED:
                    if parent_state == JobState.FAILED and allow_failed:
                        continue
                    final_state = JobState.WAITING
                    break
            else:
                final_state = JobState.READY

            updated_states = {job_id: {job_index: final_state}}
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"$set": {"state": final_state.value}}
            return [job_lock.locked_document["db_id"]]

    def set_job_run_properties(
        self,
        worker: str | None = None,
        exec_config: str | ExecutionConfig | dict | None = None,
        resources: dict | QResources | None = None,
        update: bool = True,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
    ) -> list[int]:
        set_dict = {}
        if worker:
            if worker not in self.project.workers:
                raise ValueError(f"worker {worker} is not present in the project")
            set_dict["worker"] = worker

        if exec_config:
            if (
                isinstance(exec_config, str)
                and exec_config not in self.project.exec_config
            ):
                raise ValueError(
                    f"exec_config {exec_config} is not present in the project"
                )
            elif isinstance(exec_config, ExecutionConfig):
                exec_config = exec_config.dict()

            if update and isinstance(exec_config, dict):
                for k, v in exec_config.items():
                    set_dict[f"exec_config.{k}"] = v
            else:
                set_dict["exec_config"] = exec_config

        if resources:
            if isinstance(resources, QResources):
                resources = resources.as_dict()
            if update:
                for k, v in resources.items():
                    set_dict[f"resources.{k}"] = v
            else:
                set_dict["resources"] = resources

        return self._many_jobs_action(
            method=self._set_job_properties,
            action_description="setting",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            raise_on_error=raise_on_error,
            values=set_dict,
            acceptable_states=[JobState.READY, JobState.WAITING],
        )

    def get_flow_job_aggreg(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: list[tuple] | None = None,
        limit: int = 0,
    ) -> list[dict]:
        pipeline: list[dict] = [
            {
                "$lookup": {
                    "from": self.jobs_collection,
                    "localField": "jobs",
                    "foreignField": "uuid",
                    "as": "jobs_list",
                }
            }
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": {k: v for (k, v) in sort}})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.flows.aggregate(pipeline))

    def get_flows_info(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        sort: list[tuple] | None = None,
        limit: int = 0,
        full: bool = False,
    ) -> list[FlowInfo]:
        query = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            name=name,
        )

        # Only use the full aggregation if more job details are needed.
        # The single flow document is enough for basic information
        if full:
            # TODO reduce the projection to the bare minimum to reduce the amount of
            # fecthed data?
            projection = {f"jobs_list.{f}": 1 for f in projection_job_info}
            projection["jobs_list.job.hosts"] = 1
            for k in FlowDoc.model_fields.keys():
                projection[k] = 1

            data = self.get_flow_job_aggreg(
                query=query, sort=sort, limit=limit, projection=projection
            )
        else:
            data = list(self.flows.find(query, sort=sort, limit=limit))

        jobs_data = []
        for d in data:
            jobs_data.append(FlowInfo.from_query_dict(d))

        return jobs_data

    def delete_flows(
        self,
        flow_ids: str | list[str] | None = None,
        confirm: bool = False,
        delete_output: bool = False,
    ) -> int:
        if isinstance(flow_ids, str):
            flow_ids = [flow_ids]

        if flow_ids is None:
            flow_ids = [f["uuid"] for f in self.flows.find({}, projection=["uuid"])]

        if len(flow_ids) > 10 and not confirm:
            raise ValueError(
                "Deleting more than 10 flows requires explicit confirmation"
            )
        deleted = 0
        for fid in flow_ids:
            # TODO should it catch errors?
            if self.delete_flow(fid, delete_output):
                deleted += 1

        return deleted

    def delete_flow(self, flow_id: str, delete_output: bool = False):
        # TODO should this lock anything (FW does not lock)?
        flow = self.get_flow_info_by_flow_uuid(flow_id)
        if not flow:
            return False
        job_ids = flow["jobs"]
        if delete_output:
            self.jobstore.remove_docs({"uuid": {"$in": job_ids}})

        self.jobs.delete_many({"uuid": {"$in": job_ids}})
        self.flows.delete_one({"uuid": flow_id})
        return True

    def remove_lock_job(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        query = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            locked=True,
            name=name,
            metadata=metadata,
        )

        result = self.jobs.update_many(
            filter=query,
            update={"$set": {"lock_id": None, "lock_time": None}},
        )
        return result.modified_count

    def reset(self, reset_output: bool = False, max_limit: int = 25):
        # TODO should it just delete docs related to job removed in the reset?
        # what if the outputs are in other stores? Should take those as well
        if max_limit:
            n_flows = self.flows.count_documents({})
            if n_flows >= max_limit:
                logger.warning(
                    f"The database contains {n_flows} flows and will not be reset. "
                    "Increase the max_limit value or set it to 0"
                )
                return False

        if reset_output:
            self.jobstore.remove_docs({})

        self.jobs.drop()
        self.flows.drop()
        self.auxiliary.drop()
        self.auxiliary.insert_one({"next_id": 1})
        self.build_indexes()
        return True

    def build_indexes(
        self,
        background=True,
        job_custom_indexes: list[str | list] | None = None,
        flow_custom_indexes: list[str | list] | None = None,
    ):
        """
        Build indexes
        """

        self.jobs.create_index("db_id", unique=True, background=background)

        job_indexes = [
            "uuid",
            "index",
            "state",
            "updated_on",
            "name",
            "worker",
            [("priority", pymongo.DESCENDING)],
            ["state", "remote.retry_time_limit"],
        ]
        for f in job_indexes:
            self.jobs.create_index(f, background=background)

        if job_custom_indexes:
            for idx in job_custom_indexes:
                self.jobs.create_index(idx, background=background)

        flow_indexes = [
            "uuid",
            "name",
            "state",
            "updated_on",
            "ids",
            "jobs",
        ]

        for idx in flow_indexes:
            self.flows.create_index(idx, background=background)

        if flow_custom_indexes:
            for idx in flow_custom_indexes:
                self.flows.create_index(idx, background=background)

    def compact(self):
        self.db.command({"compact": self.jobs_collection})
        self.db.command({"compact": self.flows_collection})

    def get_flow_info_by_flow_uuid(
        self, flow_uuid: str, projection: list | dict | None = None
    ):
        return self.flows.find_one({"uuid": flow_uuid}, projection=projection)

    def get_flow_info_by_job_uuid(
        self, job_uuid: str, projection: list | dict | None = None
    ):
        return self.flows.find_one({"jobs": job_uuid}, projection=projection)

    def get_job_info_by_job_uuid(
        self,
        job_uuid: str,
        job_index: int | str = "last",
        projection: list | dict | None = None,
    ):
        query: dict[str, Any] = {"uuid": job_uuid}
        sort = None
        if isinstance(job_index, int):
            query["index"] = job_index
        elif job_index == "last":
            sort = {"index": -1}
        else:
            raise ValueError(f"job_index value: {job_index} is not supported")
        return self.jobs.find_one(query, projection=projection, sort=sort)

    def get_job_doc_by_job_uuid(self, job_uuid: str, job_index: int | str = "last"):
        query: dict[str, Any] = {"uuid": job_uuid}
        sort = None
        if isinstance(job_index, int):
            query["index"] = job_index
        elif job_index == "last":
            sort = [("index", -1)]
        else:
            raise ValueError(f"job_index value: {job_index} is not supported")
        doc = self.jobs.find_one(query, sort=sort)
        if doc:
            return JobDoc.model_validate(doc)
        return None

    def get_jobs(self, query, projection: list | dict | None = None):
        return list(self.jobs.find(query, projection=projection))

    def count_jobs(
        self,
        query: dict | None = None,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
    ):
        if query is None:
            query = self._build_query_job(
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                state=state,
                locked=locked,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
            )
        return self.jobs.count_documents(query)

    def count_flows(
        self,
        query: dict | None = None,
        job_ids: str | list[str] | None = None,
        db_ids: int | list[int] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
    ):
        if not query:
            query = self._build_query_flow(
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                state=state,
                start_date=start_date,
                end_date=end_date,
                name=name,
            )
        return self.flows.count_documents(query)

    def get_jobs_info_by_flow_uuid(
        self, flow_uuid, projection: list | dict | None = None
    ):
        query = {"job.hosts": flow_uuid}
        return list(self.jobs.find(query, projection=projection))

    # TODO exec_config and resources can be removed and taken from the job.
    # The value could be set in each job by the submit_job or by the user.
    # Doing the same for the worker? In this way it could be set dynamically
    def add_flow(
        self,
        flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
        worker: str,
        allow_external_references: bool = False,
        exec_config: ExecutionConfig | None = None,
        resources: dict | QResources | None = None,
    ) -> list[int]:
        from jobflow.core.flow import get_flow

        flow = get_flow(flow, allow_external_references=allow_external_references)

        jobs_list = list(flow.iterflow())
        job_dicts = []
        n_jobs = len(jobs_list)

        doc_next_id = self.auxiliary.find_one_and_update(
            {"next_id": {"$exists": True}}, {"$inc": {"next_id": n_jobs}}
        )
        if doc_next_id is None:
            raise ValueError(
                "It seems that the database has not been initialised. If that is the"
                " case run `jf admin reset` or use the reset() method of JobController"
            )
        first_id = doc_next_id["next_id"]
        db_ids = []
        for (job, parents), db_id in zip(jobs_list, range(first_id, first_id + n_jobs)):
            db_ids.append(db_id)
            job_dicts.append(
                get_initial_job_doc_dict(
                    job,
                    parents,
                    db_id,
                    worker=worker,
                    exec_config=exec_config,
                    resources=resources,
                )
            )

        flow_doc = get_initial_flow_doc_dict(flow, job_dicts)

        # inserting first the flow document and, iteratively, all the jobs
        # should not lead to inconsistencies in the states, even if one of
        # the jobs is checked out in the meanwhile. The opposite could lead
        # to errors.
        self.flows.insert_one(flow_doc)
        self.jobs.insert_many(job_dicts)

        logger.info(f"Added flow ({flow.uuid}) with jobs: {flow.job_uuids}")

        return db_ids

    def _append_flow(
        self,
        job_doc: JobDoc,
        flow_dict: dict,
        new_flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
        worker: str,
        response_type: DynamicResponseType,
        exec_config: ExecutionConfig | None = None,
        resources: QResources | None = None,
    ):
        from jobflow.core.flow import get_flow

        new_flow = get_flow(new_flow, allow_external_references=True)

        # get job parents
        if response_type == DynamicResponseType.REPLACE:
            job_parents = job_doc.parents
        else:
            job_parents = [(job_doc.uuid, job_doc.index)]

        # add new jobs to flow
        flow_dict = dict(flow_dict)
        flow_updates: dict[str, dict[str, Any]] = {
            "$push": {"jobs": {"$each": new_flow.job_uuids}}
        }

        # add new jobs
        jobs_list = list(new_flow.iterflow())
        n_new_jobs = len(jobs_list)
        first_id = self.auxiliary.find_one_and_update(
            {"next_id": {"$exists": True}}, {"$inc": {"next_id": n_new_jobs}}
        )["next_id"]
        job_dicts = []
        flow_updates["$set"] = {}
        ids_to_push = []
        for (job, parents), db_id in zip(
            jobs_list, range(first_id, first_id + n_new_jobs)
        ):
            # inherit the parents of the job to which we are appending
            parents = parents if parents else job_parents
            job_dicts.append(
                get_initial_job_doc_dict(
                    job,
                    parents,
                    db_id,
                    worker=worker,
                    exec_config=exec_config,
                    resources=resources,
                )
            )
            # if job.index > 1:
            #     flow_dict["parents"][job.uuid][str(job.index)] = parents
            # else:
            #     flow_dict["parents"][job.uuid] = {str(job.index): parents}
            flow_updates["$set"][f"parents.{job.uuid}.{job.index}"] = parents
            # flow_dict["ids"].append((job_dicts[-1]["db_id"], job.uuid, job.index))
            ids_to_push.append((job_dicts[-1]["db_id"], job.uuid, job.index))
        flow_updates["$push"]["ids"] = {"$each": ids_to_push}

        if response_type == DynamicResponseType.DETOUR:
            # if detour, update the parents of the child jobs
            leaf_uuids = [v for v, d in new_flow.graph.out_degree() if d == 0]
            self.jobs.update_many(
                {"parents": job_doc.uuid}, {"$push": {"parents": {"$each": leaf_uuids}}}
            )

        # flow_dict["updated_on"] = datetime.utcnow()
        flow_updates["$set"]["updated_on"] = datetime.utcnow()

        # TODO, this could be replaced by the actual change, instead of the replace
        self.flows.update_one({"uuid": flow_dict["uuid"]}, flow_updates)
        self.jobs.insert_many(job_dicts)

        logger.info(f"Appended flow ({new_flow.uuid}) with jobs: {new_flow.job_uuids}")

    def checkout_job(
        self,
        query=None,
        flow_uuid: str = None,
        sort: list[tuple[str, int]] | None = None,
    ) -> tuple[str, int] | None:
        """
        Check out one job.

        Set the job state from READY to CHECKED_OUT with an atomic update.
        Flow state is also updated if needed.

        NB: flow is not locked during the checkout at any time.
        Does not require lock of the Job document.
        """
        # comment on locking: lock during check out may serve two purposes:
        # 1) update the state of the Flow object. With the conditional set
        #    this should be fine even without locking
        # 2) to prevent checking out jobs while other processes may be working
        #    on the same flow. (e.g. while rerunning a parent of a READY child,
        #    it would be necessary that the job is not started in the meanwhile).
        #    Without a full Flow lock this case may show up.
        # For the time being do not lock the flow and check if issues are arising.

        query = {} if query is None else dict(query)
        query.update({"state": JobState.READY.value})

        if flow_uuid is not None:
            # if flow uuid provided, only include job ids in that flow
            job_uuids = self.get_flow_info_by_flow_uuid(flow_uuid, ["jobs"])["jobs"]
            query["uuid"] = {"$in": job_uuids}

        if sort is None:
            sort = [("priority", pymongo.DESCENDING), ("created_on", pymongo.ASCENDING)]

        result = self.jobs.find_one_and_update(
            query,
            {
                "$set": {
                    "state": JobState.CHECKED_OUT.value,
                    "updated_on": datetime.utcnow(),
                }
            },
            projection=["uuid", "index"],
            sort=sort,
            # return_document=ReturnDocument.AFTER,
        )

        if not result:
            return None

        reserved_uuid = result["uuid"]
        reserved_index = result["index"]

        # update flow state. If it is READY switch its state, otherwise no change
        # to the state. The operation is atomic.
        # Filtering on the index is not needed
        state_cond = {
            "$cond": {
                "if": {"$eq": ["$state", "READY"]},
                "then": "RUNNING",
                "else": "$state",
            }
        }
        updated_cond = {
            "$cond": {
                "if": {"$eq": ["$state", "READY"]},
                "then": datetime.utcnow(),
                "else": "$updated_on",
            }
        }
        self.flows.find_one_and_update(
            {"jobs": reserved_uuid},
            [{"$set": {"state": state_cond, "updated_on": updated_cond}}],
        )

        return reserved_uuid, reserved_index

    # TODO if jobstore is not an option anymore, the "store" argument
    # can be removed and just use self.jobstore.
    def complete_job(
        self, job_doc: JobDoc, local_path: Path | str, store: JobStore
    ) -> bool:
        # Don't sleep if the flow is locked. Only the Runner should call this,
        # and it will handle the fact of having a locked Flow.
        # Lock before reading the data. locks the Flow for a longer time, but
        # avoids parsing (potentially large) files to discover that the flow is
        # already locked.
        with self.lock_flow(
            filter={"jobs": job_doc.uuid}, get_locked_doc=True
        ) as flow_lock:
            if flow_lock.locked_document:
                local_path = Path(local_path)
                out_path = local_path / OUT_FILENAME
                if not out_path.exists():
                    msg = (
                        f"The output file {OUT_FILENAME} was not present in the download "
                        f"folder {local_path} and it is required to complete the job"
                    )
                    self.checkin_job(
                        job_doc, flow_lock.locked_document, response=None, error=msg
                    )
                    self.update_flow_state(job_doc.job.hosts[-1])
                    return True

                out = loadfn(out_path)
                doc_update = {"start_time": out["start_time"]}
                # update the time of the JobDoc, will be used in the checkin
                end_time = out.get("end_time")
                if end_time:
                    doc_update["end_time"] = end_time

                error = out.get("error")
                if error:
                    self.checkin_job(
                        job_doc,
                        flow_lock.locked_document,
                        response=None,
                        error=error,
                        doc_update=doc_update,
                    )
                    self.update_flow_state(job_doc.job.hosts[-1])
                    return True

                response = out.get("response")
                if not response:
                    msg = (
                        f"The output file {OUT_FILENAME} was downloaded, but it does "
                        "not contain the response. The job was likely killed "
                        "before completing"
                    )
                    self.checkin_job(
                        job_doc,
                        flow_lock.locked_document,
                        response=None,
                        error=msg,
                        doc_update=doc_update,
                    )
                    self.update_flow_state(job_doc.job.hosts[-1])
                    return True

                save = {
                    k: "output" if v is True else v
                    for k, v in job_doc.job._kwargs.items()
                }

                remote_store = get_remote_store(store, local_path)
                remote_store.connect()
                update_store(store, remote_store, save)
                self.checkin_job(
                    job_doc,
                    flow_lock.locked_document,
                    response=response,
                    doc_update=doc_update,
                )
                self.update_flow_state(job_doc.job.hosts[-1])
                return True
            elif flow_lock.unavailable_document:
                # raising the error if the lock could not be acquired leaves
                # the caller handle the issue. In general, it should be the
                # runner, that will retry at a later time.
                raise FlowLockedError.from_flow_doc(
                    flow_lock.unavailable_document, "Could not complete the job"
                )

        return False

    def checkin_job(
        self,
        job_doc: JobDoc,
        flow_dict: dict,
        response: jobflow.Response | None,
        error: str | None = None,
        doc_update: dict | None = None,
    ):
        stored_data = None
        if response is None:
            new_state = JobState.FAILED.value
        # handle response
        else:
            new_state = JobState.COMPLETED.value
            if response.replace is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response.replace,
                    response_type=DynamicResponseType.REPLACE,
                    worker=job_doc.worker,
                )

            if response.addition is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response.addition,
                    response_type=DynamicResponseType.ADDITION,
                    worker=job_doc.worker,
                )

            if response.detour is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response.detour,
                    response_type=DynamicResponseType.DETOUR,
                    worker=job_doc.worker,
                )

            if response.stored_data is not None:
                from monty.json import jsanitize

                stored_data = jsanitize(
                    response.stored_data, strict=True, enum_values=True
                )

            if response.stop_children:
                self.stop_children(job_doc.uuid)

            if response.stop_jobflow:
                self.stop_jobflow(job_uuid=job_doc.uuid)

        if not doc_update:
            doc_update = {}
        doc_update.update(
            {"state": new_state, "stored_data": stored_data, "error": error}
        )

        result = self.jobs.update_one(
            {"uuid": job_doc.uuid, "index": job_doc.index}, {"$set": doc_update}
        )
        if result.modified_count == 0:
            raise RuntimeError(
                f"The job {job_doc.uuid} index {job_doc.index} has not been updated in the database"
            )

        # TODO it should be fine to replace this query by constructing the list of
        # job uuids from the original + those added. Should be verified.
        job_uuids = self.get_flow_info_by_job_uuid(job_doc.uuid, ["jobs"])["jobs"]
        return len(self.refresh_children(job_uuids)) + 1

    # TODO should this refresh all the kind of states? Or just set to ready?
    def refresh_children(self, job_uuids):
        # go through and look for jobs whose state we can update to ready.
        # Need to ensure that all parent uuids with all indices are completed
        # first find state of all jobs; ensure larger indices are returned last.
        flow_jobs = self.jobs.find(
            {"uuid": {"$in": job_uuids}},
            sort=[("index", 1)],
            projection=["uuid", "index", "parents", "state", "job.config", "db_id"],
        )
        # the mapping only contains jobs with the larger index
        jobs_mapping = {j["uuid"]: j for j in flow_jobs}

        # Now find jobs that are queued and whose parents are all completed
        # (or allowed to fail) and ready them. Assume that none of the children
        # can be in a running state and thus no need to lock them.
        to_ready = []
        for _, job in jobs_mapping.items():
            allowed_states = [JobState.COMPLETED.value]
            on_missing_ref = (
                job.get("job", {}).get("config", {}).get("on_missing_references", None)
            )
            if on_missing_ref == jobflow.OnMissing.NONE.value:
                allowed_states.extend((JobState.FAILED.value, JobState.CANCELLED.value))
            if job["state"] == JobState.WAITING.value and all(
                [jobs_mapping[p]["state"] in allowed_states for p in job["parents"]]
            ):
                # Use the db_id to identify the children, since the uuid alone is not
                # enough in some cases.
                to_ready.append(job["db_id"])

        # Here it is assuming that there will be only one job with each uuid, as
        # it should be when switching state to READY the first time.
        # The code forbids rerunning a job that have children with index larger than 1,
        # to this should always be consistent.
        if len(to_ready) > 0:
            self.jobs.update_many(
                {"db_id": {"$in": to_ready}}, {"$set": {"state": JobState.READY.value}}
            )
        return to_ready

    def stop_children(self, job_uuid: str) -> int:
        result = self.jobs.update_many(
            {"parents": job_uuid, "state": JobState.WAITING.value},
            {"$set": {"state": JobState.STOPPED.value}},
        )
        return result.modified_count

    def stop_jobflow(self, job_uuid: str = None, flow_uuid: str = None) -> int:
        if job_uuid is None and flow_uuid is None:
            raise ValueError("Either job_uuid or flow_uuid must be set.")

        if job_uuid is not None and flow_uuid is not None:
            raise ValueError("Only one of job_uuid and flow_uuid should be set.")

        if job_uuid is not None:
            criteria = {"jobs": job_uuid}
        else:
            criteria = {"uuid": flow_uuid}

        # get uuids of jobs in the flow
        flow_dict = self.flows.find_one(criteria, projection=["jobs"])
        if not flow_dict:
            return 0
        job_uuids = flow_dict["jobs"]

        result = self.jobs.update_many(
            {"uuid": {"$in": job_uuids}, "state": JobState.WAITING.value},
            {"$set": {"state": JobState.STOPPED.value}},
        )
        return result.modified_count

    def get_job_uuids(self, flow_uuids: list[str]) -> list[str]:
        job_uuids = []
        for flow in self.flows.find_one(
            {"uuid": {"$in": flow_uuids}}, projection=["jobs"]
        ):
            job_uuids.extend(flow["jobs"])
        return job_uuids

    def get_flow_jobs_data(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[dict]:
        pipeline: list[dict] = [
            {
                "$lookup": {
                    "from": self.jobs_collection,
                    "localField": "jobs",
                    "foreignField": "uuid",
                    "as": "jobs",
                }
            }
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": {k: v for (k, v) in sort}})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.flows.aggregate(pipeline))

    def update_flow_state(
        self,
        flow_uuid: str,
        updated_states: dict[str, dict[int, JobState]] | None = None,
    ):
        updated_states = updated_states or {}
        projection = ["uuid", "index", "parents", "state"]
        flow_jobs = self.get_jobs_info_by_flow_uuid(
            flow_uuid=flow_uuid, projection=projection
        )

        jobs_states = [
            updated_states.get(j["uuid"], {}).get(j["index"], JobState(j["state"]))
            for j in flow_jobs
        ]
        leafs = get_flow_leafs(flow_jobs)
        leaf_states = [JobState(j["state"]) for j in leafs]
        flow_state = FlowState.from_jobs_states(
            jobs_states=jobs_states, leaf_states=leaf_states
        )
        set_state = {"$set": {"state": flow_state.value}}
        self.flows.find_one_and_update({"uuid": flow_uuid}, set_state)

    @contextlib.contextmanager
    def lock_job(self, **lock_kwargs):
        with MongoLock(collection=self.jobs, **lock_kwargs) as lock:
            yield lock

    @contextlib.contextmanager
    def lock_flow(self, **lock_kwargs):
        with MongoLock(collection=self.flows, **lock_kwargs) as lock:
            yield lock

    @contextlib.contextmanager
    def lock_job_for_update(
        self,
        query,
        max_step_attempts,
        delta_retry,
        **kwargs,
    ):
        db_filter = dict(query)
        db_filter["remote.retry_time_limit"] = {"$not": {"$gt": datetime.utcnow()}}

        if "sort" not in kwargs:
            kwargs["sort"] = [
                ("priority", pymongo.DESCENDING),
                ("created_on", pymongo.ASCENDING),
            ]

        with self.lock_job(
            filter=db_filter,
            **kwargs,
        ) as lock:
            doc = lock.locked_document

            no_retry = False
            error = None
            try:
                yield lock
            except ConfigError:
                error = traceback.format_exc()
                warnings.warn(error, stacklevel=2)
                no_retry = True
            except RemoteError as e:
                error = f"Remote error: {e.msg}"
                no_retry = e.no_retry
            except Exception:
                error = traceback.format_exc()
                warnings.warn(error, stacklevel=2)

            set_output = lock.update_on_release

            if lock.locked_document:
                if not error:
                    succeeded_update = {
                        "$set": {
                            "remote.step_attempts": 0,
                            "remote.retry_time_limit": None,
                            "remote.error": None,
                        }
                    }
                    update_on_release = deep_merge_dict(
                        succeeded_update, set_output or {}
                    )
                else:
                    step_attempts = doc["remote"]["step_attempts"]
                    no_retry = no_retry or step_attempts >= max_step_attempts
                    if no_retry:
                        update_on_release = {
                            "$set": {
                                "state": JobState.REMOTE_ERROR.value,
                                "previous_state": doc["state"],
                                "remote.error": error,
                            }
                        }
                    else:
                        step_attempts += 1
                        ind = min(step_attempts, len(delta_retry)) - 1
                        delta = delta_retry[ind]
                        retry_time_limit = datetime.utcnow() + timedelta(seconds=delta)
                        update_on_release = {
                            "$set": {
                                "remote.step_attempts": step_attempts,
                                "remote.retry_time_limit": retry_time_limit,
                                "remote.error": error,
                            }
                        }
                if "$set" in update_on_release:
                    update_on_release["$set"]["updated_on"] = datetime.utcnow()

                lock.update_on_release = update_on_release

    @contextlib.contextmanager
    def lock_job_flow(
        self,
        job_id: str | None = None,
        db_id: int | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        job_lock_kwargs: dict | None = None,
        flow_lock_kwargs: dict | None = None,
    ):
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10
        job_lock_kwargs = job_lock_kwargs or {}
        flow_lock_kwargs = flow_lock_kwargs or {}
        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
            **job_lock_kwargs,
        ) as job_lock:
            job_doc_dict = job_lock.locked_document
            if not job_doc_dict:
                if job_lock.unavailable_document:
                    raise JobLockedError.from_job_doc(job_lock.unavailable_document)
                raise ValueError(f"No Job document matching criteria {lock_filter}")
            job_state = JobState(job_doc_dict["state"])
            if acceptable_states and job_state not in acceptable_states:
                raise ValueError(
                    f"Job in state {job_doc_dict['state']}. The action cannot be performed"
                )

            flow_filter = {"jobs": job_doc_dict["uuid"]}
            with self.lock_flow(
                filter=flow_filter,
                sleep=sleep,
                max_wait=wait,
                get_locked_doc=True,
                break_lock=break_lock,
                **flow_lock_kwargs,
            ) as flow_lock:
                if not flow_lock.locked_document:
                    if flow_lock.unavailable_document:
                        raise FlowLockedError.from_flow_doc(
                            flow_lock.unavailable_document
                        )
                    raise ValueError(
                        f"No Flow document matching criteria {flow_filter}"
                    )

                yield job_lock, flow_lock

    def ping_flow_doc(self, uuid: str):
        self.flows.find_one_and_update(
            {"nodes": uuid}, {"$set": {"updated_on": datetime.utcnow()}}
        )

    def _cancel_queue_process(self, job_doc: dict):
        queue_process_id = job_doc["remote"]["process_id"]
        if not queue_process_id:
            raise ValueError("The process id is not defined in the job document")
        worker = self.project.workers[job_doc["worker"]]
        host = worker.get_host()
        try:
            host.connect()
            queue_manager = QueueManager(worker.get_scheduler_io(), host)
            cancel_result = queue_manager.cancel(queue_process_id)
            if cancel_result.status != CancelStatus.SUCCESSFUL:
                raise RuntimeError(
                    f"Cancelling queue process {queue_process_id} failed. stdout: {cancel_result.stdout}. stderr: {cancel_result.stderr}"
                )
        finally:
            try:
                host.close()
            except Exception:
                logger.warning(
                    f"The connection to host {host} could not be closed.", exc_info=True
                )

    def get_batch_processes(self, worker: str) -> dict[str, str]:
        result = self.auxiliary.find_one({"batch_processes": {"$exists": True}})
        if result:
            return result["batch_processes"].get(worker, {})
        return {}

    def add_batch_process(self, process_id: str, process_uuid: str, worker: str):
        self.auxiliary.find_one_and_update(
            {"batch_processes": {"$exists": True}},
            {"$push": {f"batch_processes.{worker}.{process_id}": process_uuid}},
            upsert=True,
        )

    def remove_batch_process(self, process_id: str, worker: str):
        self.auxiliary.find_one_and_update(
            {"batch_processes": {"$exists": True}},
            {"$unset": {f"batch_processes.{worker}.{process_id}": ""}},
            upsert=True,
        )


def get_flow_leafs(job_docs: list[dict]) -> list[dict]:
    # first sort the list, so that only the largest indexes are kept in the dictionary
    job_docs = sorted(job_docs, key=lambda j: j["index"])
    d = {j["uuid"]: j for j in job_docs}
    for j in job_docs:
        if j["parents"]:
            for parent_id in j["parents"]:
                d.pop(parent_id, None)

    return list(d.values())
