from __future__ import annotations

import contextlib
import fnmatch
import logging
import traceback
import warnings
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

import jobflow
import pymongo
from jobflow import JobStore, OnMissing
from maggma.stores import MongoStore
from monty.json import MontyDecoder
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

if TYPE_CHECKING:
    from collections.abc import Generator


logger = logging.getLogger(__name__)


class JobController:
    """
    Main entry point for all the interactions with the Stores.

    Maintains a connection to both the queue Store and the results JobStore.
    It is required that the queue Store is a MongoStore, as it will access
    the database, and work with different collections.

    The main functionalities are those for updating the state of the database
    and querying the Jobs and Flows status information.
    """

    def __init__(
        self,
        queue_store: MongoStore,
        jobstore: JobStore,
        flows_collection: str = "flows",
        auxiliary_collection: str = "jf_auxiliary",
        project: Project | None = None,
    ):
        """
        Parameters
        ----------
        queue_store
            The Store used to save information about the status of the Jobs.
            Should be a MongoStore and other collections are used from the same
            database.
        jobstore
            The JobStore containing the output of the jobflow Flows.
        flows_collection
            The name of the collection used to store the Flows data.
            Uses the DB defined in the queue_store.
        auxiliary_collection
            The name of the collection used to store other auxiliary data.
            Uses the DB defined in the queue_store.
        project
            The project where the Stores were defined.
        """
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
    def from_project_name(cls, project_name: str | None = None) -> JobController:
        """
        Generate an instance of JobController from the project name.

        Parameters
        ----------
        project_name
            The name of the project. If None the default project will be used.
        Returns
        -------
        JobController
            An instance of JobController associated with the project.
        """
        config_manager: ConfigManager = ConfigManager()
        project: Project = config_manager.get_project(project_name)
        return cls.from_project(project=project)

    @classmethod
    def from_project(cls, project: Project) -> JobController:
        """
        Generate an instance of JobController from a Project object.

        Parameters
        ----------
        project
            The project used to generate the JobController. If None the default
            project will be used.
        Returns
        -------
        JobController
            An instance of JobController associated with the project.
        """
        queue_store = project.get_queue_store()
        flows_collection = project.queue.flows_collection
        auxiliary_collection = project.queue.auxiliary_collection
        jobstore = project.get_jobstore()
        return cls(
            queue_store=queue_store,
            jobstore=jobstore,
            flows_collection=flows_collection,
            auxiliary_collection=auxiliary_collection,
            project=project,
        )

    def close(self):
        """
        Close the connections to all the Stores in JobController.
        """
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
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
    ) -> dict:
        """
        Build a query to search for Jobs, based on standard parameters.
        The Jobs will need to satisfy all the defined conditions.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        locked
            If True only locked Jobs will be selected.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        Returns
        -------
        dict
            A dictionary with the query to be applied to a collection
            containing JobDocs.
        """
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
            # Add the beginning of the line, so that it will match the string
            # exactly if no wildcard is given. Otherwise will match substrings.
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            query["name"] = {"$regex": mongo_regex}

        if metadata:
            metadata_dict = {f"metadata.{k}": v for k, v in metadata.items()}
            query.update(metadata_dict)

        return query

    def _build_query_flow(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        locked: bool = False,
    ) -> dict:
        """
        Build a query to search for Flows, based on standard parameters.
        The Flows will need to satisfy all the defined conditions.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        state
            The state of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        locked
            If True only locked Flows will be selected.

        Returns
        -------
        dict
            A dictionary with the query to be applied to a collection
            containing FlowDocs.
        """
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

        if locked:
            query["lock_id"] = {"$ne": None}

        return query

    def get_jobs_info_query(self, query: dict = None, **kwargs) -> list[JobInfo]:
        """
        Get a list of JobInfo based on a generic query.

        Parameters
        ----------
        query
            The query to be performed.
        kwargs
            arguments passed to MongoDB find().

        Returns
        -------
        list
            A list of JobInfo matching the criteria.
        """
        data = self.jobs.find(query, projection=projection_job_info, **kwargs)

        jobs_data = []
        for d in data:
            jobs_data.append(JobInfo.from_query_output(d))

        return jobs_data

    def get_jobs_info(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
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
        """
        Query for Jobs based on standard parameters and return a list of JobInfo.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        locked
            If True only locked Jobs will be selected.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        sort
            A list of (key, direction) pairs specifying the sort order for this
            query. Follows pymongo conventions.
        limit
            Maximum number of entries to retrieve. 0 means no limit.

        Returns
        -------
        list
            A list of JobInfo objects for the Jobs matching the criteria.
        """
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
        """
        Query for Jobs based on a generic filter and return a list of JobDoc.

        Parameters
        ----------
        query
            A dictionary representing the filter.
        kwargs
            All arguments passed to pymongo's Collection.find() method.
        Returns
        -------
        list
            A list of JobDoc objects for the Jobs matching the criteria.
        """
        data = self.jobs.find(query, **kwargs)

        jobs_data = []
        for d in data:
            jobs_data.append(JobDoc.model_validate(d))

        return jobs_data

    def get_jobs_doc(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
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
        """
        Query for Jobs based on standard parameters and return a list of JobDoc.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        locked
            If True only locked Jobs will be selected.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        sort
            A list of (key, direction) pairs specifying the sort order for this
            query. Follows pymongo conventions.
        limit
            Maximum number of entries to retrieve. 0 means no limit.

        Returns
        -------
        list
            A list of JobDoc objects for the Jobs matching the criteria.
        """
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
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> tuple[dict, list | None]:
        """
        Generate a query for a single Job based on db_id or uuid+index.
        Only one among db_id and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job the sorting will be
            added to get the highest index.
        Returns
        -------
        dict, list
            A dict and an optional list to be used as query and sort,
            respectively, in a query for a single Job.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
    ) -> JobInfo | None:
        """
        Get the JobInfo for a single Job based on db_id or uuid+index.
        Only one among db_id and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.

        Returns
        -------
        JobInfo
            A JobInfo, or None if no Job matches the criteria.
        """
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
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        **method_kwargs,
    ) -> list[int]:
        """
        Helper method to query Jobs based on criteria and sequentially apply an
        action on all those retrieved.

        Used to provide a common interface between all the methods that
        should be applied on a list of jobs sequentially.

        Parameters
        ----------
        method
            The function that should be applied on a single Job.
        action_description
            A description of the action being performed. For logging purposes.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        locked
            If True only locked Jobs will be selected.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        method_kwargs
            Kwargs passed to the method called on each Job
        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        db_ids: str | list[str] | None = None,
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
        """
        Rerun a list of selected Jobs, i.e. bring their state back to READY.
        See the docs of `rerun_job` for more details.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        force
            Bypass the limitation that only failed Jobs can be rerun.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        """
        Rerun a single Job, i.e. bring its state back to READY.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        By default, only Jobs in one of the running states (CHECKED_OUT,
        UPLOADED, ...), in the REMOTE_ERROR state or FAILED with
        children in the READY or WAITING state can be rerun.
        This should guarantee that no unexpected inconsistencies due to
        dynamic Jobs generation should appear. This limitation can be bypassed
        with the `force` option.
        In any case, no Job with children with index > 1 can be rerun, as there
        is no sensible way of handling it.

        Rerunning a Job in a REMOTE_ERROR or on an intermediate STATE also
        results in a reset of the remote attempts and errors.
        When rerunning a Job in a SUBMITTED or RUNNING state the system also
        tries to cancel the process in the worker.
        Rerunning a FAILED Job also lead to change of state in its children.
        The full list of modified Jobs is returned.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        force
            Bypass the limitation that only Jobs in a certain state can be rerun.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        """
        Perform the full rerun of Job, in case a Job is FAILED or in one of the
        usually not admissible states. This requires actions on the original
        Job's children and will need to acquire the lock on all of them as well
        as on the Flow.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
            Just the "uuid", "index", "db_id", "state" values are required.
        sleep
            Amounts of seconds to wait between checks that the lock has been released.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents.
        force
            Bypass the limitation that only Jobs in a certain state can be rerun.
        Returns
        -------
        dict, list
            Updates to be set on the rerun Job upon lock release and the list
            of db_ids of the modified Jobs.
        """
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
        """
        Simple reset of a Job in a running state or REMOTE_ERROR.
        Does not require additional locking on the Flow or other Jobs.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
            Just the "uuid", "index", "state" values are required.

        Returns
        -------
        dict
            Updates to be set on the Job upon lock release.
        """
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
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
    ) -> list[int]:
        """
        Helper to set multiple values in a JobDoc while locking the Job.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        values
            Dictionary with the values to be set. Will be passed to a pymongo
            `find_one_and_update` method.
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents.
        acceptable_states
            List of JobState for which the Job values can be changed.
            If None all states are acceptable.
        Returns
        -------
        list
            List of db_ids of updated Jobs. Could be an empty list or a list
            with a single element.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        """
        Set the state of a Job to an arbitrary JobState.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        No check is performed! Any job can be set to any state.
        Only for advanced users or for debugging purposes.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents.
        Returns
        -------
        list
            List of db_ids of updated Jobs. Could be an empty list or a list
            with a single element.
        """
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
        db_ids: str | list[str] | None = None,
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
        """
        Retry selected Jobs, i.e. bring them back to its previous state if REMOTE_ERROR,
        or reset the remote attempts and time of retry if in another running state.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        """
        Retry a single Job, i.e. bring it back to its previous state if REMOTE_ERROR,
        or reset the remote attempts and time of retry if in another running state.
        Jobs in other states cannot be retried.
        The Job is selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Only locking of the retried Job is required.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List containing the db_id of the updated Job.
        """
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
                    "remote.error": None,
                }
                lock.update_on_release = {"$set": set_dict}
            else:
                raise ValueError(f"Job in state {state.value} cannot be retried.")
            return [doc["db_id"]]

    def pause_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
    ) -> list[int]:
        """
        Pause selected Jobs. Only READY and WAITING Jobs can be paused.
        The action is reversible.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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

    def stop_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
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
        """
        Stop selected Jobs. Only Jobs in the READY and all the running states
        can be stopped.
        The action is not reversible.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.stop_job,
            action_description="stopping",
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

    def stop_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        """
        Stop a single Job. Only Jobs in the READY and all the running states
        can be stopped.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.
        The action is not reversible.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
            job_id = job_doc["uuid"]
            job_index = job_doc["index"]
            updated_states = {job_id: {job_index: JobState.USER_STOPPED}}
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {
                "$set": {"state": JobState.USER_STOPPED.value}
            }
            return [job_lock.locked_document["db_id"]]

    def pause_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
    ) -> list[int]:
        """
        Pause a single Job. Only READY and WAITING Jobs can be paused.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.
        The action is reversible.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
            job_doc = job_lock.locked_document
            job_id = job_doc["uuid"]
            job_index = job_doc["index"]
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
        db_ids: str | list[str] | None = None,
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
        """
        Restart selected Jobs that were previously paused.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[int]:
        """
        Restart a single Jobs that was previously paused.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
            job_id = job_doc["uuid"]
            job_index = job_doc["index"]
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
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        raise_on_error: bool = True,
    ) -> list[int]:
        """
        Set execution properties for selected Jobs:
        worker, exec_config and resources.

        Parameters
        ----------
        worker
            The name of the worker to set.
        exec_config
            The name of the exec_config to set or an explicit value of
            ExecutionConfig or dict.
        resources
            The resources to be set, either as a dict or a QResources instance.
        update
            If True, when setting exec_config and resources a passed dictionary
            will be used to update already existing values.
            If False it will replace the original values.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
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
        """
        Retrieve data about Flows and all their Jobs through an aggregation.

        In the aggregation the list of Jobs are identified as `jobs_list`.

        Parameters
        ----------
        query
            A dictionary representing the filter.
        projection
            Projection of the fields passed to the aggregation.
        sort
            A list of (key, direction) pairs specifying the sort order for this
            query. Follows pymongo conventions.
        limit
            Maximum number of entries to retrieve. 0 means no limit.
        Returns
        -------
        list
            The list of dictionaries resulting from the query.
        """
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
        db_ids: str | list[str] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        sort: list[tuple] | None = None,
        limit: int = 0,
        full: bool = False,
    ) -> list[FlowInfo]:
        """
        Query for Flows based on standard parameters and return a list of JobFlows.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        state
            The state of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        sort
            A list of (key, direction) pairs specifying the sort order for this
            query. Follows pymongo conventions.
        limit
            Maximum number of entries to retrieve. 0 means no limit.
        full
            If True data is fetched from both the Flow collection and Job collection
            with an aggregate. Otherwise, only the Job information in the Flow
            document will be used.

        Returns
        -------
        list
            A list of JobFlows.
        """
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
        """
        Delete a list of Flows based on the flow uuids.

        Parameters
        ----------
        flow_ids
            One or more Flow uuids.
        confirm
            If False only a maximum of 10 Flows can be deleted.
        delete_output
            If True also delete the associated output in the JobStore.

        Returns
        -------
        int
            Number of delete Flows.
        """
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
        """
        Delete a single Flow based on the uuid.

        Parameters
        ----------
        flow_id
            One or more Flow uuids.
        delete_output
            If True also delete the associated output in the JobStore.
        """
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
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        state: JobState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
    ) -> int:
        """
        Forcibly remove the lock on a locked Job document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Doing otherwise may result in inconsistencies.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        state
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.

        Returns
        -------
        int
            Number of modified Jobs.
        """
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

    def remove_lock_flow(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | None = None,
        state: FlowState | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
    ) -> int:
        """
        Forcibly remove the lock on a locked Flow document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Doing otherwise may result in inconsistencies.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        state
            The state of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)

        Returns
        -------
        int
            Number of modified Flows.
        """
        query = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            state=state,
            start_date=start_date,
            end_date=end_date,
            locked=True,
            name=name,
        )

        result = self.flows.update_many(
            filter=query,
            update={"$set": {"lock_id": None, "lock_time": None}},
        )
        return result.modified_count

    def reset(self, reset_output: bool = False, max_limit: int = 25) -> bool:
        """
        Reset the content of the queue database and builds the indexes.
        Optionally deletes the content of the JobStore with the outputs.
        In this case all the data contained in the JobStore will be removed,
        not just those associated to the data in the queue.

        Parameters
        ----------
        reset_output
            If True also reset the JobStore containing the outputs.
        max_limit
            Maximum number of Flows present in the DB. If number is larger
            the database will not be reset. Set 0 for not limit.
        Returns
        -------
        bool
            True if the database was reset, False otherwise.
        """
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
        background: bool = True,
        job_custom_indexes: list[str | list] | None = None,
        flow_custom_indexes: list[str | list] | None = None,
    ):
        """
        Build indexes in the database

        Parameters
        ----------
        background
            If True, the indexes should be created in the background.
        job_custom_indexes
            List of custom indexes for the jobs collection. Each element is passed
            to pymongo's create_index, thus following those conventions.
        flow_custom_indexes
            List of custom indexes for the flows collection.
            Same as job_custom_indexes.
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
        """
        Compact jobs and flows collections in MongoDB.
        """
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

    def get_job_doc(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
    ) -> JobDoc | None:
        query, sort = self.generate_job_id_query(db_id, job_id, job_index)

        data = list(self.jobs.find(query, sort=sort, limit=1))
        if not data:
            return None

        return JobDoc.model_validate(data[0])

    def get_jobs(self, query, projection: list | dict | None = None):
        return list(self.jobs.find(query, projection=projection))

    def count_jobs(
        self,
        query: dict | None = None,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
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
        db_ids: str | list[str] | None = None,
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
    ) -> list[str]:
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
        for (job, parents), db_id_int in zip(
            jobs_list, range(first_id, first_id + n_jobs)
        ):
            prefix = self.project.queue.db_id_prefix or ""
            db_id = f"{prefix}{db_id_int}"
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
        job_doc: dict,
        flow_dict: dict,
        new_flow_dict: dict,
        worker: str,
        response_type: DynamicResponseType,
        exec_config: ExecutionConfig | None = None,
        resources: QResources | None = None,
    ):
        from jobflow import Flow, Job

        decoder = MontyDecoder()

        def deserialize_partial_flow(in_dict: dict):
            """
            Recursively deserialize a Flow dictionary, avoiding the deserialization
            of all the elements that may require external packages.
            """
            if in_dict.get("@class", None) == "Flow":
                jobs = [deserialize_partial_flow(d) for d in in_dict.get("jobs")]
                flow_init = {
                    k: v
                    for k, v in in_dict.items()
                    if k not in ("@module", "@class", "@version", "job")
                }
                flow_init["jobs"] = jobs
                return Flow(**flow_init)
            # if it is not a Flow, should be a Job
            job_init = {
                k: v
                for k, v in in_dict.items()
                if k not in ("@module", "@class", "@version")
            }
            job_init["config"] = decoder.process_decoded(job_init["config"])
            return Job(**job_init)

        # It is sure that the new_flow_dict is a serialized Flow (and not Job
        # or list[Job]), because the get_flow has already been applied at run
        # time, during the remote execution.
        # Recursive deserialize the Flow without deserializing function and
        # arguments to take advantage of standard Flow/Job methods.
        new_flow = deserialize_partial_flow(new_flow_dict)

        # get job parents
        if response_type == DynamicResponseType.REPLACE:
            job_parents = job_doc["parents"]
        else:
            job_parents = [(job_doc["uuid"], job_doc["index"])]

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
        for (job, parents), db_id_int in zip(
            jobs_list, range(first_id, first_id + n_new_jobs)
        ):
            prefix = self.project.queue.db_id_prefix or ""
            db_id = f"{prefix}{db_id_int}"
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
            flow_updates["$set"][f"parents.{job.uuid}.{job.index}"] = parents
            ids_to_push.append((job_dicts[-1]["db_id"], job.uuid, job.index))
        flow_updates["$push"]["ids"] = {"$each": ids_to_push}

        if response_type == DynamicResponseType.DETOUR:
            # if detour, update the parents of the child jobs
            leaf_uuids = [v for v, d in new_flow.graph.out_degree() if d == 0]
            self.jobs.update_many(
                {"parents": job_doc["uuid"]},
                {"$push": {"parents": {"$each": leaf_uuids}}},
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
        self, job_doc: dict, local_path: Path | str, store: JobStore
    ) -> bool:
        # Don't sleep if the flow is locked. Only the Runner should call this,
        # and it will handle the fact of having a locked Flow.
        # Lock before reading the data. locks the Flow for a longer time, but
        # avoids parsing (potentially large) files to discover that the flow is
        # already locked.
        with self.lock_flow(
            filter={"jobs": job_doc["uuid"]}, get_locked_doc=True
        ) as flow_lock:
            if flow_lock.locked_document:
                local_path = Path(local_path)
                out_path = local_path / OUT_FILENAME
                host_flow_id = job_doc["job"]["hosts"][-1]
                if not out_path.exists():
                    msg = (
                        f"The output file {OUT_FILENAME} was not present in the download "
                        f"folder {local_path} and it is required to complete the job"
                    )
                    self.checkin_job(
                        job_doc, flow_lock.locked_document, response=None, error=msg
                    )
                    self.update_flow_state(host_flow_id)
                    return True

                # do not deserialize the response or stored data, saves time and
                # avoids the need for packages to be installed.
                out = loadfn(out_path, cls=None)
                decoder = MontyDecoder()
                doc_update = {"start_time": decoder.process_decoded(out["start_time"])}
                # update the time of the JobDoc, will be used in the checkin
                end_time = decoder.process_decoded(out.get("end_time"))
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
                    self.update_flow_state(host_flow_id)
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
                    self.update_flow_state(host_flow_id)
                    return True

                remote_store = get_remote_store(store, local_path)

                update_store(store, remote_store, job_doc["db_id"])

                self.checkin_job(
                    job_doc,
                    flow_lock.locked_document,
                    response=response,
                    doc_update=doc_update,
                )
                self.update_flow_state(host_flow_id)
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
        job_doc: dict,
        flow_dict: dict,
        response: dict | None,
        error: str | None = None,
        doc_update: dict | None = None,
    ):
        stored_data = None
        if response is None:
            new_state = JobState.FAILED.value
        # handle response
        else:
            new_state = JobState.COMPLETED.value
            if response["replace"] is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["replace"],
                    response_type=DynamicResponseType.REPLACE,
                    worker=job_doc["worker"],
                    exec_config=job_doc["exec_config"],
                    resources=job_doc["resources"],
                )

            if response["addition"] is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["addition"],
                    response_type=DynamicResponseType.ADDITION,
                    worker=job_doc["worker"],
                    exec_config=job_doc["exec_config"],
                    resources=job_doc["resources"],
                )

            if response["detour"] is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["detour"],
                    response_type=DynamicResponseType.DETOUR,
                    worker=job_doc["worker"],
                    exec_config=job_doc["exec_config"],
                    resources=job_doc["resources"],
                )

            if response["stored_data"] is not None:
                stored_data = response["stored_data"]

            if response["stop_children"]:
                self.stop_children(job_doc["uuid"])

            if response["stop_jobflow"]:
                self.stop_jobflow(job_uuid=job_doc["uuid"])

        if not doc_update:
            doc_update = {}
        doc_update.update(
            {"state": new_state, "stored_data": stored_data, "error": error}
        )

        result = self.jobs.update_one(
            {"uuid": job_doc["uuid"], "index": job_doc["index"]}, {"$set": doc_update}
        )
        if result.modified_count == 0:
            raise RuntimeError(
                f"The job {job_doc['uuid']} index {job_doc['index']} has not been updated in the database"
            )

        # TODO it should be fine to replace this query by constructing the list of
        # job uuids from the original + those added. Should be verified.
        job_uuids = self.get_flow_info_by_job_uuid(job_doc["uuid"], ["jobs"])["jobs"]
        return len(self.refresh_children(job_uuids)) + 1

    # TODO should this refresh all the kind of states? Or just set to ready?
    def refresh_children(self, job_uuids: list[str]) -> list[int]:
        """
        Set the state of Jobs children to READY following the completion of a Job.

        Parameters
        ----------
        job_uuids
            List of Jobs uuids belonging to a Flow.

        Returns
        -------
            List of db_ids of modified Jobs.
        """
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
                allowed_states.extend(
                    (JobState.FAILED.value, JobState.USER_STOPPED.value)
                )
            if job["state"] == JobState.WAITING.value and all(
                [jobs_mapping[p]["state"] in allowed_states for p in job["parents"]]
            ):
                # Use the db_id to identify the children, since the uuid alone is not
                # enough in some cases.
                to_ready.append(job["db_id"])

        # Here it is assuming that there will be only one job with each uuid, as
        # it should be when switching state to READY the first time.
        # The code forbids rerunning a job that have children with index larger than 1,
        # so this should always be consistent.
        if len(to_ready) > 0:
            self.jobs.update_many(
                {"db_id": {"$in": to_ready}}, {"$set": {"state": JobState.READY.value}}
            )
        return to_ready

    def stop_children(self, job_uuid: str) -> int:
        """
        Stop the direct children of a Job in the WAITING state.

        Parameters
        ----------
        job_uuid
            The uuid of the Job.
        Returns
        -------
            The number of modified Jobs.
        """
        result = self.jobs.update_many(
            {"parents": job_uuid, "state": JobState.WAITING.value},
            {"$set": {"state": JobState.STOPPED.value}},
        )
        return result.modified_count

    def stop_jobflow(self, job_uuid: str = None, flow_uuid: str = None) -> int:
        """
        Stop all the WAITING Jobs in a Flow.

        Parameters
        ----------
        job_uuid
            The uuid of Job to identify the Flow. Incompatible with flow_uuid.
        flow_uuid
            The Flow uuid. Incompatible with job_uuid.
        Returns
        -------
            The number of modified Jobs.
        """
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
        """
        Get the list of Jobs belonging to Flows, based on their uuid.

        Parameters
        ----------
        flow_uuids
            A list of Flow uuids.
        Returns
        -------
            A list of uuids of Jobs belong to the selected Flows.
        """
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
        """
        Get the data of Flows and their Jobs from the DB using an aggregation.

        In the aggregation the Jobs are identified as "jobs".

        Parameters
        ----------
        query
            The query to filter the Flow.
        projection
            The projection for the Flow and Job data.
        sort
            Sorting passed to the aggregation.
        limit
            The maximum number of results returned.
        Returns
        -------
            A list of dictionaries with the result of the query.
        """
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
        """
        Update the state of a Flow in the DB based on the Job's states.

        The Flow should be locked while performing this operation.

        Parameters
        ----------
        flow_uuid
            The uuid of the Flow to update.
        updated_states
            A dictionary with the updated states of Jobs that have not been
            stored in the DB yet. In the form {job_uuid: JobState value}.
        """
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
    def lock_job(self, **lock_kwargs) -> Generator[MongoLock, None, None]:
        """
        Lock a Job document.

        See MongoLock context manager for more details about the locking options.

        Parameters
        ----------
        lock_kwargs
            Kwargs passed to the MongoLock context manager.
        Returns
        -------
        MongoLock
            An instance of MongoLock.
        """
        with MongoLock(collection=self.jobs, **lock_kwargs) as lock:
            yield lock

    @contextlib.contextmanager
    def lock_flow(self, **lock_kwargs) -> Generator[MongoLock, None, None]:
        """
        Lock a Flow document.

        See MongoLock context manager for more details about the locking options.

        Parameters
        ----------
        lock_kwargs
            Kwargs passed to the MongoLock context manager.
        Returns
        -------
        MongoLock
            An instance of MongoLock.
        """
        with MongoLock(collection=self.flows, **lock_kwargs) as lock:
            yield lock

    @contextlib.contextmanager
    def lock_job_for_update(
        self,
        query,
        max_step_attempts,
        delta_retry,
        **kwargs,
    ) -> Generator[MongoLock, None, None]:
        """
        Lock a Job document for state update by the Runner.

        See MongoLock context manager for more details about the locking options.

        Parameters
        ----------
        query
            The query used to select the Job document to lock.
        max_step_attempts
            The maximum number of attempts for a single step after which
            the Job should be set to the REMOTE_ERROR state.
        delta_retry
            List of increasing delay between subsequent attempts when the
            advancement of a remote step fails. Used to set the retry time.
        kwargs
            Kwargs passed to the MongoLock context manager.

        Returns
        -------
        MongoLock
            An instance of MongoLock.
        """
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
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        job_lock_kwargs: dict | None = None,
        flow_lock_kwargs: dict | None = None,
    ) -> Generator[tuple[MongoLock, MongoLock], None, None]:
        """
        Lock one Job document and the Flow document the Job belongs to.

        See MongoLock context manager for more details about the locking options.

        Parameters
        ----------
        job_id
            The uuid of the Job to lock.
        db_id
            The db_id of the Job to lock.
        job_index
            The index of the Job to lock.
        wait
            The amount of seconds to wait for a lock to be released.
        break_lock
            True if the context manager is allowed to forcibly break a lock.
        acceptable_states
            A list of JobStates. If not among these a ValueError exception is
            raised.
        job_lock_kwargs
            Kwargs passed to MongoLock for the Job lock.
        flow_lock_kwargs
            Kwargs passed to MongoLock for the Flow lock.
        Returns
        -------
        MongoLock, MongoLock
            An instance of MongoLock.
        """
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
        """
        Ping a Flow document to update its "updated_on" value.

        Parameters
        ----------
        uuid
            The uuid of the Flow to update.
        """
        self.flows.find_one_and_update(
            {"nodes": uuid}, {"$set": {"updated_on": datetime.utcnow()}}
        )

    def _cancel_queue_process(self, job_doc: dict):
        """
        Cancel the process in the remote queue.

        Parameters
        ----------
        job_doc
            The dict of the JobDoc with the Job to be cancelled.
        """
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
        """
        Get the batch processes associated with a given worker.

        Parameters
        ----------
        worker
            The worker name.
        Returns
        -------
        dict
            A dictionary with the {process_id: process_uuid} of the batch
            jobs running on the selected worker.
        """
        result = self.auxiliary.find_one({"batch_processes": {"$exists": True}})
        if result:
            return result["batch_processes"].get(worker, {})
        return {}

    def add_batch_process(
        self, process_id: str, process_uuid: str, worker: str
    ) -> dict:
        """
        Add a batch process to the list of running processes.

        Two IDs are defined, one to keep track of the actual process number and one
        to be associated to the Jobs that are being executed. The need for two IDs
        originates from the fact that the former may not be known at runtime.

        Parameters
        ----------
        process_id
            The ID of the processes obtained from the QueueManager.
        process_uuid
            A unique ID to identify the processes.
        worker
            The worker where the process is being executed.
        Returns
        -------
        dict
            The updated document.
        """
        return self.auxiliary.find_one_and_update(
            {"batch_processes": {"$exists": True}},
            {"$push": {f"batch_processes.{worker}.{process_id}": process_uuid}},
            upsert=True,
        )

    def remove_batch_process(self, process_id: str, worker: str) -> dict:
        """
        Remove a process from the list of running batch processes.

        Parameters
        ----------
        process_id
            The ID of the processes obtained from the QueueManager.
        worker
            The worker where the process was being executed.
        Returns
        -------
        dict
            The updated document.
        """
        return self.auxiliary.find_one_and_update(
            {"batch_processes": {"$exists": True}},
            {"$unset": {f"batch_processes.{worker}.{process_id}": ""}},
            upsert=True,
        )


def get_flow_leafs(job_docs: list[dict]) -> list[dict]:
    """
    Get the leaf jobs from a list of serilized representation of JobDoc.

    Parameters
    ----------
    job_docs
        The list of serialized JobDocs in the Flow
    Returns
    -------
    list
        The list of serialized JobDocs that are leafs of the Flow.
    """
    # first sort the list, so that only the largest indexes are kept in the dictionary
    job_docs = sorted(job_docs, key=lambda j: j["index"])
    d = {j["uuid"]: j for j in job_docs}
    for j in job_docs:
        if j["parents"]:
            for parent_id in j["parents"]:
                d.pop(parent_id, None)

    return list(d.values())
