from __future__ import annotations

import contextlib
import fnmatch
import importlib.metadata
import logging
import shutil
import traceback
import warnings
from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

import jobflow
import pymongo
from dateutil.tz import gettz
from jobflow import JobStore, OnMissing
from monty.dev import deprecated
from monty.json import MontyDecoder
from monty.serialization import loadfn
from packaging.version import Version
from packaging.version import parse as parse_version
from qtoolkit.core.data_objects import CancelStatus, QResources

import jobflow_remote
from jobflow_remote.config.base import ConfigError, ExecutionConfig, Project
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.jobs.data import (
    OUT_FILENAME,
    DbCollection,
    DynamicResponseType,
    FlowDoc,
    FlowInfo,
    JobDoc,
    JobInfo,
    RemoteError,
    get_initial_flow_doc_dict,
    get_initial_job_doc_dict,
    get_reset_job_base_dict,
    projection_flow_info_jobs,
    projection_job_info,
)
from jobflow_remote.jobs.state import (
    DELETABLE_STATES,
    PAUSABLE_STATES,
    RESETTABLE_STATES,
    RUNNING_STATES,
    FlowState,
    JobState,
)
from jobflow_remote.remote.data import (
    get_local_data_path,
    get_remote_store,
    get_remote_store_filenames,
    update_store,
)
from jobflow_remote.remote.queue import QueueManager
from jobflow_remote.utils.data import (
    deep_merge_dict,
    get_past_time_rounded,
    get_utc_offset,
)
from jobflow_remote.utils.db import (
    FlowLockedError,
    JobLockedError,
    LockedDocumentError,
    MissingDocumentError,
    MongoLock,
    mongodump_from_store,
    mongorestore_to_store,
    pymongo_dump,
    pymongo_restore,
)
from jobflow_remote.utils.remote import SharedHosts, safe_remove_job_files

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from maggma.stores import MongoStore


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
    ) -> None:
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

    def close(self) -> None:
        """Close the connections to all the Stores in JobController."""
        try:
            self.queue_store.close()
        except Exception:
            logger.exception("Error while closing the connection to the queue store")

        try:
            self.jobstore.close()
        except Exception:
            logger.exception("Error while closing the connection to the job store")

    def _build_query_job(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.

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
        if isinstance(states, JobState):
            states = [states]
        if isinstance(workers, str):
            workers = [workers]

        query: dict = defaultdict(dict)

        if db_ids:
            query["db_id"] = {"$in": db_ids}
        if job_ids:
            job_ids = cast(list[tuple[str, int]], job_ids)
            or_list = []
            for job_id, job_index in job_ids:
                or_list.append({"uuid": job_id, "index": job_index})
            query["$or"] = or_list

        if flow_ids:
            query["job.hosts"] = {"$in": flow_ids}

        if states:
            query["state"] = {"$in": [s.value for s in states]}

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            query["updated_on"]["$lte"] = end_date_str

        if locked:
            query["lock_id"] = {"$ne": None}

        if name:
            # Add the beginning of the line, so that it will match the string
            # exactly if no wildcard is given. Otherwise will match substrings.
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            query["job.name"] = {"$regex": mongo_regex}

        if metadata:
            metadata_dict = {f"job.metadata.{k}": v for k, v in metadata.items()}
            query.update(metadata_dict)

        if workers:
            query["worker"] = {"$in": workers}

        return query

    def _build_query_flow(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
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
        states
            One or more states of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
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
        if flow_ids is not None and not isinstance(flow_ids, (list, tuple)):
            flow_ids = [flow_ids]
        if isinstance(states, FlowState):
            states = [states]

        query: dict = {}

        if db_ids:
            # the "0" refers to the index in the ids list.
            # needs to be a string, but is correctly recognized by MongoDB
            query["ids"] = {"$elemMatch": {"0": {"$in": db_ids}}}
        if job_ids:
            query["jobs"] = {"$in": job_ids}

        if flow_ids:
            query["uuid"] = {"$in": flow_ids}

        if states:
            query["state"] = {"$in": [s.value for s in states]}

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            query["updated_on"] = {"$lte": end_date_str}

        if name:
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            query["name"] = {"$regex": mongo_regex}

        if metadata:
            metadata_dict = {f"metadata.{k}": v for k, v in metadata.items()}
            query.update(metadata_dict)

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
        return [JobInfo.from_query_output(d) for d in data]

    def get_jobs_info(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        locked: bool = False,
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
        skip: int = 0,
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
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
            states=states,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
        )
        return self.get_jobs_info_query(query=query, sort=sort, limit=limit, skip=skip)

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

        return [JobDoc.model_validate(d) for d in data]

    def get_jobs_doc(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
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
            states=states,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
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
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        max_limit: int = 0,
        **method_kwargs,
    ) -> list[str]:
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
        states
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        max_limit
            The action will be applied to the Jobs only if the total number is lower
            than the specified limit. 0 means no limit.
        method_kwargs
            Kwargs passed to the method called on each Job

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        filtering_options = [
            job_ids,
            db_ids,
            flow_ids,
            states,
            start_date,
            end_date,
            name,
            metadata,
            workers,
        ]
        if custom_query and any(opt is not None for opt in filtering_options):
            raise ValueError(
                "The custom query option is incompatible with all the other filtering options"
            )
        if custom_query:
            query = custom_query
        else:
            query = self._build_query_job(
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                states=states,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=workers,
            )
        result = self.jobs.find(query, projection=["db_id"])

        queried_dbs_ids = [r["db_id"] for r in result]

        if max_limit != 0 and len(queried_dbs_ids) > max_limit:
            raise ValueError(
                f"Cannot perform {action_description} on {len(queried_dbs_ids)} Jobs "
                f"as they exceeds the specified maximum limit ({max_limit})."
                f"Increase the limit to complete the action on this many Jobs."
            )

        updated_ids = set()
        for db_id in queried_dbs_ids:
            try:
                job_updated_ids = method(db_id=db_id, **method_kwargs)
                if not isinstance(job_updated_ids, (list, tuple)):
                    job_updated_ids = (
                        [] if job_updated_ids is None else [job_updated_ids]
                    )
                if job_updated_ids:
                    updated_ids.update(job_updated_ids)
            except Exception:
                if raise_on_error:
                    raise
                logger.exception(f"Error while {action_description} for job {db_id}")

        return list(updated_ids)

    def rerun_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
        delete_files: bool = True,
    ) -> list[str]:
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
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
        delete_files
            Delete all the files in the worker folder of the Jobs that are rerun.

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
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            force=force,
            wait=wait,
            break_lock=break_lock,
            delete_files=delete_files,
        )

    def rerun_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
        delete_files: bool = True,
    ) -> list[str]:
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
        delete_files
            Delete all the files in the worker folder of the rerun Job.
            Note that the deletion will not be performed directly but only when the job effectively restarts.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10

        modified_jobs: list[str] = []
        # the job to rerun is the last to be released since this prevents
        # a checkout of the job while the flow is still locked
        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            projection=["uuid", "index", "db_id", "state", "worker", "run_dir"],
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
            if job_state in RESETTABLE_STATES:
                # if in one of the resettable states no need to lock the flow or
                # update children.
                doc_update = self._reset_remote(job_doc_dict, delete_files=delete_files)
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
                    delete_files=delete_files,
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
        delete_files: bool = True,
    ) -> tuple[dict, list[str]]:
        """
        Perform the full rerun of Job, in case a Job is FAILED or in one of the
        usually not admissible states. This requires actions on the original
        Job's children and will need to acquire the lock on all of them as well
        as on the Flow.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
            Just the "uuid", "index", "db_id", "state", "worker" values are required.
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
        delete_files
            Delete all the files in the worker folder of the children Jobs that are modified.

        Returns
        -------
        dict, list
            Updates to be set on the rerun Job upon lock release and the list
            of db_ids of the modified Jobs.
        """
        job_id = doc["uuid"]
        job_index = doc["index"]
        modified_jobs: list[str] = []

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
                        f"Job {job_id} has a child job ({dep_id}) which is not the last index ({dep_index}). "
                        "Rerunning the Job will lead to inconsistencies and is not allowed."
                    )

            # TODO should STOPPED be acceptable?
            acceptable_child_states = [
                JobState.READY.value,
                JobState.WAITING.value,
                JobState.PAUSED.value,
            ]
            # Update the state of the descendants
            updated_states: dict[str, dict[int, JobState]] = defaultdict(dict)
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
                            projection=[
                                "uuid",
                                "index",
                                "db_id",
                                "state",
                                "worker",
                                "run_dir",
                            ],
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
                    child_doc = child_lock.locked_document
                    child_doc_update = get_reset_job_base_dict()
                    child_doc_update["state"] = JobState.WAITING.value
                    if child_doc["state"] != JobState.WAITING.value:
                        modified_jobs.append(child_doc["db_id"])
                        if delete_files:
                            child_doc_update["remote.prerun_cleanup"] = True
                    child_lock.update_on_release = {"$set": child_doc_update}
                    updated_states[child_doc["uuid"]][child_doc["index"]] = (
                        JobState.WAITING
                    )
                    self._delete_tmp_folder(child_doc)

            # if everything is fine here, update the state of the flow
            # before releasing its lock and set the update for the original job
            # pass explicitly the new state of the job, since it is not updated
            # in the DB. The Job is the last lock to be released.
            updated_states[job_id][job_index] = JobState.READY
            self.update_flow_state(
                flow_uuid=flow_doc.uuid, updated_states=updated_states
            )

            # delete local temporary folder to avoid parsing
            # previously downloaded files.
            self._delete_tmp_folder(doc)
            job_doc_update = get_reset_job_base_dict()
            job_doc_update["state"] = JobState.READY.value
            if delete_files:
                job_doc_update["remote.prerun_cleanup"] = True

        return job_doc_update, modified_jobs

    def _reset_remote(self, doc: dict, delete_files: bool = True) -> dict:
        """
        Simple reset of a Job in a running state or REMOTE_ERROR.
        Does not require additional locking on the Flow or other Jobs.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
            Just the "uuid", "index", "state" values are required.
        delete_files
            Delete all the files in the worker folder of the rerun Job.

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
        if delete_files:
            job_doc_update["remote.prerun_cleanup"] = True

        return job_doc_update

    @deprecated(
        message="_set_job_properties will be removed. Use the set_job_doc_properties method instead"
    )
    def _set_job_properties(
        self,
        values: dict,
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        use_pipeline: bool = False,
    ) -> str | None:
        """
        Helper to set multiple values in a JobDoc while locking the Job.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        values
            Dictionary with the values to be set. Will be passed to a pymongo
            `update_one` method.
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
        use_pipeline
            if True a pipeline will be used in the update of the document

        Returns
        -------
        str
            The db_id of the updated Job. None if the Job was not updated.
        """
        return self.set_job_doc_properties(
            values=values,
            db_id=db_id,
            job_id=job_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            acceptable_states=acceptable_states,
            use_pipeline=use_pipeline,
        )

    def set_job_doc_properties(
        self,
        values: dict,
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        use_pipeline: bool = False,
    ) -> str:
        """
        Helper to set multiple values in a JobDoc while locking the Job.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        values
            Dictionary with the values to be set. Will be passed to a pymongo
            `update_one` method.
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
        use_pipeline
            if True a pipeline will be used in the update of the document

        Returns
        -------
        str
            The db_id of the updated Job. None if the Job was not updated.
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
                lock.update_on_release = (
                    [{"$set": values}] if use_pipeline else {"$set": values}
                )
                return doc["db_id"]

        return None

    def set_job_state(
        self,
        state: JobState,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
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
        str
            The db_id of the updated Job. None if the Job was not updated.
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
        return self.set_job_doc_properties(
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
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
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
            action_description="retrying",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
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
    ) -> str:
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
        str
            The db_id of the updated Job.
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
                except ValueError as exc:
                    raise ValueError(
                        f"The registered previous state: {previous_state} is not a valid state"
                    ) from exc
                set_dict = get_reset_job_base_dict()
                set_dict["state"] = previous_state

                lock.update_on_release = {"$set": set_dict}
            elif state in RUNNING_STATES:
                set_dict = {
                    "remote.step_attempts": 0,
                    "remote.retry_time_limit": None,
                    "remote.error": None,
                    "remote.queue_out": None,
                    "remote.queue_err": None,
                }
                lock.update_on_release = {"$set": set_dict}
            else:
                raise ValueError(f"Job in state {state.value} cannot be retried.")
            return doc["db_id"]

    def pause_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
    ) -> list[str]:
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
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
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            wait=wait,
        )

    def stop_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
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
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
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
    ) -> str:
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
        str
            The db_id of the updated Job.
        """
        job_lock_kwargs = dict(
            projection=["uuid", "index", "db_id", "state", "remote", "worker"]
        )
        flow_lock_kwargs = dict(projection=["uuid"])
        with self.lock_job_flow(
            acceptable_states=[JobState.READY, *RUNNING_STATES],
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")

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
            if flow_lock.locked_document is None:
                raise RuntimeError("No document found in flow lock")
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {
                "$set": {"state": JobState.USER_STOPPED.value}
            }
            return_doc = job_lock.locked_document
            if return_doc is None:
                raise RuntimeError("No document found in final job lock")

            return return_doc["db_id"]

    def pause_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
    ) -> str:
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
        str
            The db_id of the updated Job.
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
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
            job_id = job_doc["uuid"]
            job_index = job_doc["index"]
            updated_states = {job_id: {job_index: JobState.PAUSED}}
            flow_doc = flow_lock.locked_document
            if flow_doc is None:
                raise RuntimeError("No flow document found in lock")
            self.update_flow_state(
                flow_uuid=flow_doc["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"$set": {"state": JobState.PAUSED.value}}
            return_doc = job_lock.locked_document
            if return_doc is None:
                raise RuntimeError("No document found in final job lock")

            return return_doc["db_id"]

    def play_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
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
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
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
    ) -> str:
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
        str
            The db_id of the updated Job.
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
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
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
            return job_lock.locked_document["db_id"]

    def set_job_run_properties(
        self,
        worker: str | None = None,
        exec_config: str | ExecutionConfig | dict | None = None,
        resources: dict | QResources | None = None,
        priority: int | None = None,
        update: bool = True,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
    ) -> list[str]:
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
        priority
            The priority of the Job.
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        set_dict: dict[str, Any] = {}
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
            if isinstance(exec_config, ExecutionConfig):
                exec_config = exec_config.model_dump()

            if update and isinstance(exec_config, dict):
                # if the content is a string replace even if it is an update,
                # merging is meaningless
                cond = {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$exec_config"}, "string"]},
                        "then": exec_config,
                        "else": {"$mergeObjects": ["$exec_config", exec_config]},
                    }
                }
                set_dict["exec_config"] = cond

            else:
                set_dict["exec_config"] = exec_config

        if resources:
            if isinstance(resources, QResources):
                resources = resources.as_dict()
                # if passing a QResources it is pointless to update
                # all the keywords will be overwritten and if the previous
                # value was a generic dictionary the merged dictionary will fail
                # almost surely lead to failures
                update = False
            if update:
                set_dict["resources"] = {"$mergeObjects": ["$resources", resources]}
            else:
                set_dict["resources"] = resources

        if priority is not None:
            set_dict["priority"] = priority

        acceptable_states = [
            JobState.READY,
            JobState.WAITING,
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.PAUSED,
            JobState.REMOTE_ERROR,
        ]

        return self._many_jobs_action(
            method=self.set_job_doc_properties,
            action_description="setting",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            values=set_dict,
            acceptable_states=acceptable_states,
            use_pipeline=update,
        )

    def get_flow_job_aggreg(
        self,
        query: dict | None = None,
        projection_flow: dict | None = None,
        projection_job: dict | None = None,
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
        projection_flow
            Projection of the fields for the Flow document passed to the aggregation.
        projection_job
            Projection of the fields for the Job document used in the $lookup.
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

        if projection_flow:
            pipeline.append({"$project": projection_flow})

        if projection_job:
            # insert the pipeline for the projection of the Job fields
            # to reduce the impact of the size of the documents.
            # This can help reducing the size of the fetched documents and
            # avoid exceeding the maximum size allowed. Adding the projection
            # in the general pipeline does not have the same effect.
            pipeline[0]["$lookup"]["pipeline"] = [{"$project": projection_job}]
            # if the additional projection is set, the keys need to be specified
            # in that part of the pipeline as well.
            if projection_flow:
                for k in projection_job:
                    pipeline[-1]["$project"][f"jobs_list.{k}"] = 1

        if sort:
            pipeline.append({"$sort": dict(sort)})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.flows.aggregate(pipeline))

    def get_flows_info(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        locked: bool = False,
        sort: list[tuple] | None = None,
        limit: int = 0,
        skip: int = 0,
        full: bool = False,
    ) -> list[FlowInfo]:
        """
        Query for Flows based on standard parameters and return a list of FlowInfo.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flow.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        locked
            If True only locked Flows will be selected.
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
            A list of FlowInfo.
        """
        query = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            locked=locked,
        )

        # Only use the full aggregation if more job details are needed.
        # The single flow document is enough for basic information
        if full:
            projection_job = {f: 1 for f in projection_flow_info_jobs}
            projection_flow = {k: 1 for k in FlowDoc.model_fields}

            data = self.get_flow_job_aggreg(
                query=query,
                sort=sort,
                limit=limit,
                projection_flow=projection_flow,
                projection_job=projection_job,
            )
        else:
            data = list(self.flows.find(query, sort=sort, limit=limit, skip=skip))

        return [FlowInfo.from_query_dict(d) for d in data]

    def delete_flows(
        self,
        flow_ids: str | list[str] | None = None,
        max_limit: int = 10,
        delete_output: bool = False,
        delete_files: bool = False,
    ) -> int:
        """
        Delete a list of Flows based on the flow uuids.

        Parameters
        ----------
        flow_ids
            One or more Flow uuids.
        max_limit
            The Flows will be deleted only if the total number is lower than the
            specified limit. 0 means no limit.
        delete_output
            If True also delete the associated output in the JobStore.
        delete_files
            If True also delete the files on the worker.

        Returns
        -------
        int
            Number of deleted Flows.
        """
        if isinstance(flow_ids, str):
            flow_ids = [flow_ids]

        if flow_ids is None:
            flow_ids = [f["uuid"] for f in self.flows.find({}, projection=["uuid"])]

        if max_limit != 0 and len(flow_ids) > max_limit:
            raise ValueError(
                f"Cannot delete {len(flow_ids)} Flows as they exceeds the specified maximum "
                f"limit ({max_limit}). Increase the limit to delete the Flows."
            )
        deleted = 0
        # Open the SharedHosts so that hosts will be shared for all the Flows
        with SharedHosts(self.project):
            for fid in flow_ids:
                # TODO should it catch errors?
                if self.delete_flow(
                    fid, delete_output=delete_output, delete_files=delete_files
                ):
                    deleted += 1

        return deleted

    def delete_flow(
        self,
        flow_id: str,
        delete_output: bool = False,
        delete_files: bool = False,
    ) -> bool:
        """
        Delete a single Flow based on the uuid.

        Parameters
        ----------
        flow_id
            One or more Flow uuids.
        delete_output
            If True also delete the associated output in the JobStore.
        delete_files
            If True also delete the files on the worker.

        Returns
        -------
        bool
            True if the flow has been deleted.
        """
        # TODO should this lock anything (FW does not lock)?
        flow = self.get_flow_info_by_flow_uuid(flow_id)
        if not flow:
            return False
        job_ids = flow["jobs"]
        if delete_output:
            self.jobstore.remove_docs({"uuid": {"$in": job_ids}})
        if delete_files:
            jobs_info = self.get_jobs_info(flow_ids=[flow_id])
            self._safe_delete_files(jobs_info)

        self.jobs.delete_many({"uuid": {"$in": job_ids}})
        self.flows.delete_one({"uuid": flow_id})
        return True

    def unlock_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
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
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.

        Returns
        -------
        int
            Number of modified Jobs.
        """
        query = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            locked=True,
            name=name,
            metadata=metadata,
            workers=workers,
        )

        result = self.jobs.update_many(
            filter=query,
            update={"$set": {"lock_id": None, "lock_time": None}},
        )
        return result.modified_count

    def _safe_delete_files(
        self, jobs_info: Sequence[JobInfo | dict]
    ) -> list[JobInfo | dict]:
        """
        Delete the files associated to the selected Jobs.

        Checks that the folder to be deleted contains the jfremote_in.json
        file to avoid mistakenly deleting other folders.

        Parameters
        ----------
        jobs_info
            A list of JobInfo whose files should be deleted.

        Returns
        -------
        list
            The list of JobInfo whose files have been actually deleted.
        """
        deleted = []
        with SharedHosts(self.project) as shared_hosts:
            for job_info in jobs_info:
                if isinstance(job_info, JobInfo):
                    run_dir = job_info.run_dir
                    worker = job_info.worker
                else:
                    run_dir = job_info["run_dir"]
                    worker = job_info["worker"]
                if run_dir:
                    host = shared_hosts.get_host(worker)
                    if safe_remove_job_files(
                        host=host, run_dir=run_dir, raise_on_error=False
                    ):
                        deleted.append(job_info)
        return deleted

    def unlock_flows(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
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
        states
            One or more states of the Flows.
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
            states=states,
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

    def unlock_runner(self) -> tuple[int, int]:
        """
        Forcibly remove the lock on a locked Runner document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Should also be done only when no daemon is running.
        Doing otherwise may result in inconsistencies.

        Returns
        -------
        tuple
            Number of runner documents and modified runner documents.
        """
        query = {"running_runner": {"$exists": True}}

        result = self.auxiliary.update_many(
            filter=query,
            update={"$set": {"lock_id": None, "lock_time": None}},
        )
        return result.matched_count, result.modified_count

    def reset(
        self,
        reset_output: bool = False,
        max_limit: int = 25,
        validation: str | None = None,
    ) -> bool:
        """
        Reset the content of the queue database and builds the indexes.
        Optionally deletes the content of the JobStore with the outputs.
        In this case all the data contained in the JobStore will be removed,
        not just those associated to the data in the queue.

        Notes
        -----
        This method will not check whether there is a daemon running.

        Parameters
        ----------
        reset_output
            If True also reset the JobStore containing the outputs.
        max_limit
            Maximum number of Flows present in the DB. If number is larger
            the database a validation should be passed. Set 0 for not limit.
            Setting max_limit to a large number or 0 will always lead to a
            reset of the DB without validation. Prefer setting the password
            to avoid unwanted deletions.
        validation
            A string representing today's date in the format YYYY-MM-DD.
            Required if the number of Flows to delete exceed max_limit.
        Returns
        -------
        bool
            True if the database was reset, False otherwise.
        """
        # TODO should it just delete docs related to job removed in the reset?
        # what if the outputs are in other stores? Should take those as well
        if max_limit:
            n_flows = self.flows.count_documents({})
            today = datetime.now().strftime("%Y-%m-%d")
            if n_flows >= max_limit and today != validation:
                logger.warning(
                    f"The database contains {n_flows} flows and will not be reset. "
                    "Pass today's date in the YYYY-MM-DD format to validate the reset "
                    "or change the max_limit value."
                )
                return False

        if reset_output:
            self.jobstore.remove_docs({})

        self.jobs.drop()
        self.flows.drop()
        self.auxiliary.drop()
        self.auxiliary.insert_one({"next_id": 1})
        self.auxiliary.insert_one({"running_runner": None})
        self.update_version_information()
        self.build_indexes(drop=True)

        return True

    def build_indexes(
        self,
        background: bool = True,
        job_custom_indexes: list[str | list] | None = None,
        flow_custom_indexes: list[str | list] | None = None,
        drop: bool = False,
    ) -> None:
        """
        Build indexes in the database.

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
        drop
            If True all existing indexes in the collections will be dropped.
        """

        if drop:
            self.jobs.drop_indexes()
            self.flows.drop_indexes()
            self.auxiliary.drop_indexes()

        self.jobs.create_index("db_id", unique=True, background=background)
        self.jobs.create_index(
            [("uuid", pymongo.ASCENDING), ("index", pymongo.ASCENDING)],
            unique=True,
            background=background,
        )

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

        self.flows.create_index("uuid", unique=True, background=background)
        flow_indexes = [
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

    def create_indexes(
        self,
        indexes: list[str | list],
        collection: DbCollection = DbCollection.JOBS,
        unique: bool = False,
        background: bool = True,
    ) -> None:
        """
        Build the selected indexes

        Parameters
        ----------
        indexes
            List of indexes to be added to the collection. Each element is passed
            to pymongo's create_index, thus following those conventions.
        collection
            The collection where the index will be created.
        unique

        background
            If True, the indexes should be created in the background.
        """
        coll = self.get_collection(collection)
        for idx in indexes:
            coll.create_index(idx, background=background, unique=unique)

    def get_collection(self, collection: DbCollection):
        """
        Return the internal collection corresponding to the selected DbCollection.

        Parameters
        ----------
        collection
            The collection selected.

        Returns
        -------
            The internal instance of the MongoDB collection.
        """
        return {
            DbCollection.JOBS: self.jobs,
            DbCollection.FLOWS: self.flows,
            DbCollection.AUX: self.auxiliary,
        }[collection]

    def compact(self) -> None:
        """Compact jobs and flows collections in MongoDB."""
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

    def get_job_info_by_pid(self, pid: int | str) -> JobInfo | None:
        """
        Retrieve job information by process ID (e.g., Slurm job ID).

        Args:
            pid (int): The process ID of the job in the queue system.

        Returns:
            JobInfo | None: Job information if found, None otherwise.
        """
        query = {"remote.process_id": str(pid)}
        jobs_info = self.get_jobs_info_query(query=query, limit=1)

        if jobs_info:
            return jobs_info[0]

        return None

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
        states: JobState | list[JobState] | None = None,
        locked: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
    ) -> int:
        """
        Count Jobs based on filters.

        Parameters
        ----------
        query
            A generic query. Will override all the other parameters.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.

        Returns
        -------
        int
            Number of Jobs matching the criteria.
        """
        if query is None:
            query = self._build_query_job(
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                states=states,
                locked=locked,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=workers,
            )
        return self.jobs.count_documents(query)

    def count_flows(
        self,
        query: dict | None = None,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
    ) -> int:
        """
        Count flows based on filter parameters.

        Parameters
        ----------
        query
            A generic query. Will override all the other parameters.
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flows.
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
            Number of Flows matching the criteria.
        """
        if not query:
            query = self._build_query_flow(
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                states=states,
                start_date=start_date,
                end_date=end_date,
                name=name,
            )
        return self.flows.count_documents(query)

    def count_jobs_states(self, states: list[JobState]) -> dict[JobState, int]:
        """
        Count the number of jobs in each of the given states.

        Parameters
        ----------
        states
            List of JobState to count.

        Returns
        -------
        dict[JobState, int]
            A dictionary with the count of jobs in each state.
        """
        pipeline = [
            {"$match": {"state": {"$in": [s.value for s in states]}}},
            {"$group": {"_id": "$state", "count": {"$sum": 1}}},
        ]
        result = self.jobs.aggregate(pipeline) or []
        out = {}
        for r in result:
            out[JobState(r["_id"])] = r["count"]

        for state in states:
            out[state] = out.get(state, 0)

        return out

    def count_flows_states(self, states: list[FlowState]) -> dict[FlowState, int]:
        """
        Count the number of flows in each of the given states.

        Parameters
        ----------
        states
            List of FlowState to count.

        Returns
        -------
        dict[FlowState, int]
            A dictionary with the count of flows in each state.
        """
        pipeline = [
            {"$match": {"state": {"$in": [s.value for s in states]}}},
            {"$group": {"_id": "$state", "count": {"$sum": 1}}},
        ]
        result = self.flows.aggregate(pipeline) or []
        out = {}
        for r in result:
            out[FlowState(r["_id"])] = r["count"]

        for state in states:
            out[state] = out.get(state, 0)

        return out

    def get_trends(
        self,
        states: Sequence[JobState | FlowState],
        interval: str = "days",
        num_intervals: int | None = None,
        interval_timezone: str = "UTC",
    ) -> dict[str, dict[JobState | FlowState, int]]:
        """
        Generates a pipeline to retrieve trends of job states over time for the given interval.

        Parameters
        ----------
        states
            A list of JobStates or FlowStates to be considered in the trend.
        interval
            One of 'hours', 'days', 'weeks', 'months', or 'years' to define the grouping period.
        num_intervals
            The number of intervals to consider. If not provided, it will be set to a default
            value based on the interval.
        interval_timezone
            The timezone to use for the date aggregation.

        Returns
        -------
        dict[str, dict[JobState | FlowState, int]]
            A dictionary with the date in the local timezone as key, and another dictionary as value.
            The inner dictionary contains the state as key and the number of jobs in that state as value.
        """

        tz = gettz(interval_timezone)
        utc_now = datetime.now(timezone.utc)
        tznow = utc_now.astimezone(tz)
        num_intervals = (
            num_intervals
            or {"hours": 12, "days": 7, "weeks": 4, "months": 12, "years": 3}[interval]
        )

        # Map the interval to a MongoDB date format
        date_format = {
            "hours": "%Y-%m-%d %H",
            "days": "%Y-%m-%d",
            "weeks": "%Y-%m-%d",  # use days and group by week in python
            "months": "%Y-%m",
            "years": "%Y",
        }[interval]

        start = get_past_time_rounded(
            interval=interval, num_intervals=num_intervals, reference=tznow
        )
        start.replace(tzinfo=tz)
        start_utc = start.astimezone(timezone.utc)

        date_to_string = {"format": date_format, "date": "$updated_on"}
        if interval_timezone != "UTC":
            date_to_string["timezone"] = get_utc_offset(interval_timezone)
        pipeline = [
            {"$match": {"updated_on": {"$gte": start_utc}}},
            {
                "$group": {
                    "_id": {"$dateToString": date_to_string},
                    **{
                        state.value.lower(): {
                            "$sum": {"$cond": [{"$eq": ["$state", state.value]}, 1, 0]}
                        }
                        for state in states
                    },
                }
            },
            {"$sort": {"_id": 1}},
        ]

        if isinstance(states[0], JobState):
            collection = self.jobs
            state_cls: type = JobState
        elif isinstance(states[0], FlowState):
            collection = self.flows
            state_cls = FlowState
        else:
            raise TypeError(f"Wrong type of state passed: {states}")

        results = list(collection.aggregate(pipeline))

        # MongoDB uses an approach in grouping by weeks that may lead to
        # more intervals than those specified, based on the number of
        # weeks. To avoid inconsistencies "weeks" are handled separately:
        # jobs are grouped by day in the query and regrouped by week
        # using the ISO calendar convention
        result_dict = {}
        if interval == "weeks":
            # first prepare the full dictionary with all the expected values
            for i in range(1, num_intervals + 1):
                # Calculate the expected date rounded to the interval
                expected_date = get_past_time_rounded(
                    interval=interval, num_intervals=i, reference=tznow
                )

                # Convert expected_date to ISO year-week (as a string)
                iso_year, iso_week, _ = expected_date.isocalendar()
                iso_id = f"{iso_year}-{iso_week:02d}"

                # Fill result_dict with zero counts
                result_dict[iso_id] = {state: 0 for state in states}

            # add the number for each day in the corresponding week
            for entry in results:
                raw_date = datetime.strptime(entry["_id"], "%Y-%m-%d")
                iso_year, iso_week, _ = raw_date.isocalendar()
                iso_id = f"{iso_year}-{iso_week:02d}"

                for state, count in entry.items():
                    if state != "_id":
                        result_dict[iso_id][state_cls(state.upper())] += count

        else:
            for r in results:
                result_dict[r["_id"]] = {
                    state_cls(s.upper()): n for s, n in r.items() if s != "_id"
                }

            for i in range(1, num_intervals + 1):
                expected_date_str = get_past_time_rounded(
                    interval=interval, num_intervals=i, reference=tznow
                ).strftime(date_format)
                if expected_date_str not in result_dict:
                    result_dict[expected_date_str] = {state: 0 for state in states}

        return result_dict

    def get_jobs_info_by_flow_uuid(
        self, flow_uuid, projection: list | dict | None = None
    ):
        query = {"job.hosts": flow_uuid}
        return list(self.jobs.find(query, projection=projection))

    def add_flow(
        self,
        flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
        worker: str,
        allow_external_references: bool = False,
        exec_config: ExecutionConfig | None = None,
        resources: dict | QResources | None = None,
        priority: int = 0,
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
                    priority=priority,
                )
            )

        flow_doc = get_initial_flow_doc_dict(flow, job_dicts)

        # inserting first the flow document and, iteratively, all the jobs
        # should not lead to inconsistencies in the states, even if one of
        # the jobs is checked out in the meanwhile. The opposite could lead
        # to errors.
        try:
            self.flows.insert_one(flow_doc)
            self.jobs.insert_many(job_dicts)
        except pymongo.errors.DuplicateKeyError as exc:
            # note that if the same Flow is inserted twice, the error will happen
            # during the insert_one(flow_doc), so no inconsistency will be left in
            # the DB. Since jobflow prevents the same job to belong to two different
            # Flows, the only apparently reasonable case where the flow uuid is
            # different but a job has a duplicated uuid is a random clash between
            # two generated uuid, which should have a negligible probability of
            # occurring. If a problem shows up a solution might be enclosing the
            # previous two operations in a transaction.
            raise ValueError(
                "A duplicate key error happened while inserting the flow. Check "
                "that you are not trying to submit the same flow multiple times."
            ) from exc

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
        priority: int = 0,
    ) -> None:
        from jobflow import Flow, Job

        decoder = MontyDecoder()

        def deserialize_partial_flow(in_dict: dict):
            """
            Recursively deserialize a Flow dictionary, avoiding the deserialization
            of all the elements that may require external packages.
            """
            if in_dict.get("@class") == "Flow":
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

        # get job parents. Job parents are identified only by their uuid.
        if response_type == DynamicResponseType.REPLACE:
            job_parents = job_doc["parents"]
        else:
            job_parents = [job_doc["uuid"]]

        # add new jobs to flow
        flow_dict = dict(flow_dict)
        flow_updates: dict[str, dict[str, Any]] = {
            "$addToSet": {"jobs": {"$each": new_flow.job_uuids}},
            "$push": {},
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
            parents = parents if parents else job_parents  # noqa: PLW2901
            job_dicts.append(
                get_initial_job_doc_dict(
                    job,
                    parents,
                    db_id,
                    worker=worker,
                    exec_config=exec_config,
                    resources=resources,
                    priority=priority,
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
            flow_out = self.get_flow_info_by_flow_uuid(flow_uuid, ["jobs"])
            if not flow_out:
                return None
            job_uuids = flow_out["jobs"]
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
                # This check needs to be present because if the worker is "local"
                # the download phase is skipped and the check is not done earlier.
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
                # update the time of the JobDoc, will be used in the checking
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

                # Files associated with the store may not have been downloaded.
                # First check if they exist and then try to get the store
                required_store_files = get_remote_store_filenames(
                    store, config_dict=self.project.remote_jobstore
                )
                for store_file in required_store_files:
                    if not (local_path / store_file).exists():
                        msg = (
                            "No explicit error raised during the remote execution, but the output "
                            f"store file {store_file} is missing in the downloaded folder {local_path}. "
                            "The file was probably not created in the remote folder during the "
                            "execution but is needed to proceed."
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

                remote_store = get_remote_store(
                    store, local_path, self.project.remote_jobstore
                )

                update_store(store, remote_store, job_doc["db_id"])

                self.checkin_job(
                    job_doc,
                    flow_lock.locked_document,
                    response=response,
                    doc_update=doc_update,
                )
                self.update_flow_state(host_flow_id)
                return True
            if flow_lock.unavailable_document:
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
        queue_out = None
        queue_err = None
        if response is None:
            # set queue_out and queue_err here in case of failure
            queue_out, queue_err = self._get_downloaded_queue_files(job_doc)
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
                    priority=job_doc["priority"],
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
                    priority=job_doc["priority"],
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
                    priority=job_doc["priority"],
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
            {
                "state": new_state,
                "stored_data": stored_data,
                "error": error,
                "remote.queue_out": queue_out,
                "remote.queue_err": queue_err,
            }
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
    def refresh_children(self, job_uuids: list[str]) -> list[str]:
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
        for job in jobs_mapping.values():
            allowed_states = [JobState.COMPLETED.value]
            on_missing_ref = (
                job.get("job", {}).get("config", {}).get("on_missing_references", None)
            )
            if on_missing_ref == jobflow.OnMissing.NONE.value:
                allowed_states.extend(
                    (JobState.FAILED.value, JobState.USER_STOPPED.value)
                )
            if job["state"] == JobState.WAITING.value and all(
                jobs_mapping[p]["state"] in allowed_states for p in job["parents"]
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

        criteria = {"uuid": flow_uuid} if job_uuid is None else {"jobs": job_uuid}

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
            pipeline.append({"$sort": dict(sort)})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.flows.aggregate(pipeline))

    def get_running_runner(self) -> dict | str:
        """Get the running runner information from the auxiliary collection."""
        rr_doc = self.auxiliary.find_one({"running_runner": {"$exists": True}})
        if rr_doc:
            return rr_doc["running_runner"]
        return "NO_DOCUMENT"

    def clean_running_runner(self, break_lock: bool = False) -> None:
        db_filter = {"running_runner": {"$exists": True}}
        with self.lock_auxiliary(
            filter=db_filter, break_lock=break_lock, get_locked_doc=True
        ) as lock:
            if not lock.locked_document:
                if lock.unavailable_document:
                    raise LockedDocumentError(
                        "Document for the running runner in the auxiliary collection is locked"
                    )
                raise MissingDocumentError(
                    "Runner document missing from auxiliary collection"
                )
            lock.update_on_release = {"$set": {"running_runner": None}}

    def update_flow_state(
        self,
        flow_uuid: str,
        updated_states: dict[str, dict[int, JobState | None]] | None = None,
    ) -> None:
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
            If the value is None the Job is considered deleted and the state
            of that Job will be ignored while determining the state of the
            whole Flow.
        """
        updated_states = updated_states or {}
        projection = ["uuid", "index", "parents", "state"]
        flow_jobs = self.get_jobs_info_by_flow_uuid(
            flow_uuid=flow_uuid, projection=projection
        )

        # update the full list of states and those of the leafs according
        # to the updated_states passed.
        # Ignore the Jobs for which the updated_states value is None.
        jobs_states = [
            updated_states.get(j["uuid"], {}).get(j["index"], JobState(j["state"]))
            for j in flow_jobs
            if updated_states.get(j["uuid"], {}).get(j["index"], JobState(j["state"]))
            is not None
        ]
        leafs = get_flow_leafs(flow_jobs)
        leaf_states = [
            updated_states.get(j["uuid"], {}).get(j["index"], JobState(j["state"]))
            for j in leafs
            if updated_states.get(j["uuid"], {}).get(j["index"], JobState(j["state"]))
            is not None
        ]
        flow_state = FlowState.from_jobs_states(
            jobs_states=jobs_states, leaf_states=leaf_states
        )

        # update flow state. If it is changed update the updated_on
        updated_cond = {
            "$cond": {
                "if": {"$eq": ["$state", flow_state.value]},
                "then": "$updated_on",
                "else": datetime.utcnow(),
            }
        }
        self.flows.find_one_and_update(
            {"uuid": flow_uuid},
            [{"$set": {"state": flow_state.value, "updated_on": updated_cond}}],
        )

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
        query: dict,
        max_step_attempts: int,
        delta_retry: tuple[int, ...],
        next_step_delay: int | None = None,
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
        next_step_delay
            An amount of seconds that sets the delay for the next step to
            start even in case there are no errors.
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
                cause = e.__cause__
                if cause:
                    # this is required for support of python 3.9. In 3.10 the API
                    # changed and format_exception(e) could be used instead.
                    # Do that if/when support for 3.9 is dropped.
                    trace = traceback.format_exception(
                        type(cause), cause, cause.__traceback__
                    )
                    error += "\ncaused by:\n" + "".join(trace)
                no_retry = e.no_retry
            except Exception:
                error = traceback.format_exc()
                warnings.warn(error, stacklevel=2)

            set_output = lock.update_on_release

            if lock.locked_document:
                if not error:
                    next_step_time_limit = None
                    if next_step_delay:
                        next_step_time_limit = datetime.utcnow() + timedelta(
                            seconds=next_step_delay
                        )
                    # When succeeded don't set remote.queue_out/remote.queue_err to
                    # None, otherwise it overwrites values that may be written if the
                    # completion fails.
                    succeeded_update = {
                        "$set": {
                            "remote.step_attempts": 0,
                            "remote.retry_time_limit": next_step_time_limit,
                            "remote.error": None,
                        }
                    }
                    update_on_release = deep_merge_dict(
                        succeeded_update, set_output or {}
                    )
                else:
                    step_attempts = doc["remote"]["step_attempts"]
                    no_retry = no_retry or step_attempts >= max_step_attempts
                    queue_out, queue_err = self._get_downloaded_queue_files(doc)
                    if no_retry:
                        update_on_release = {
                            "$set": {
                                "state": JobState.REMOTE_ERROR.value,
                                "previous_state": doc["state"],
                                "remote.error": error,
                                "remote.queue_out": queue_out,
                                "remote.queue_err": queue_err,
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
                                "remote.queue_out": queue_out,
                                "remote.queue_err": queue_err,
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

    @contextlib.contextmanager
    def lock_auxiliary(self, **lock_kwargs) -> Generator[MongoLock, None, None]:
        """
        Lock a document in the auxiliary collection.

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
        with MongoLock(collection=self.auxiliary, **lock_kwargs) as lock:
            yield lock

    def _get_downloaded_queue_files(
        self, job_doc: dict
    ) -> tuple[str | None, str | None]:
        local_path_str = get_local_data_path(
            project=self.project,
            worker=job_doc["worker"],
            job_id=job_doc["uuid"],
            index=job_doc["index"],
            run_dir=job_doc["run_dir"],
        )
        if not local_path_str:
            return None, None
        local_path = Path(local_path_str)
        queue_out_path = local_path / "queue.out"
        queue_err_path = local_path / "queue.err"
        queue_out = None
        queue_err = None
        length_limit = 3000
        if queue_out_path.exists():
            with queue_out_path.open(mode="rt") as f:
                queue_out = f.read()
                if len(queue_out) > length_limit:
                    queue_out = queue_out[:length_limit]
                    queue_out += " ...\nThe content was cut. Check the content of the actual file"
        if queue_err_path.exists():
            with queue_err_path.open(mode="rt") as f:
                queue_err = f.read()
                if len(queue_err) > length_limit:
                    queue_err = queue_err[:length_limit]
                    queue_err += " ...\nThe content was cut. Check the content of the actual file"

        return queue_out, queue_err

    def _delete_tmp_folder(self, job_doc: dict):
        worker = self.project.workers[job_doc["worker"]]
        if not worker.is_local:
            local_path = get_local_data_path(
                project=self.project,
                worker=worker,
                job_id=job_doc["uuid"],
                index=job_doc["index"],
                run_dir=job_doc["run_dir"],
            )
            if Path(local_path).exists():
                try:
                    shutil.rmtree(local_path)
                except Exception as e:
                    logger.warning(
                        f"Could not delete the temporary local folder {local_path}: {getattr(e, 'message', e)}"
                    )

    def ping_flow_doc(self, uuid: str) -> None:
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

    def _cancel_queue_process(self, job_doc: dict) -> None:
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
        with SharedHosts(self.project) as shared_hosts:
            worker = self.project.workers[job_doc["worker"]]
            host = shared_hosts.get_host(job_doc["worker"])

            queue_manager = QueueManager(worker.get_scheduler_io(), host)
            cancel_result = queue_manager.cancel(queue_process_id)
            if cancel_result.status != CancelStatus.SUCCESSFUL:
                raise RuntimeError(
                    f"Cancelling queue process {queue_process_id} failed. "
                    f"stdout: {cancel_result.stdout}. stderr: {cancel_result.stderr}"
                )

    def get_batch_processes(
        self, worker: str | None = None
    ) -> dict[str, dict[str, str]]:
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
        if worker:
            query = {f"batch_processes.{worker}": {"$exists": True}}
        else:
            query = {"batch_processes": {"$exists": True}}

        result = self.auxiliary.find_one(query)
        if result:
            return result["batch_processes"] or {}
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
            {"$set": {f"batch_processes.{worker}.{process_id}": process_uuid}},
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

    def delete_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        delete_output: bool = False,
        delete_files: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Delete a single job from the queue store and optionally from the job store.
        The Flow document will be updated accordingly but no consistency check
        is performed. The Flow may be left in an inconsistent state.
        For advanced users only.

        Parameters
        ----------
        job_id
            The uuid of the job to delete.
        db_id
            The db_id of the job to delete.
        job_index
            The index of the job. If None, the job with the largest index will be selected.
        delete_output : bool, default False
            If True, also delete the job output from the JobStore.
        delete_files
            If True also delete the files on the worker.
        wait
            In case the Flow or Job is locked, wait this time (in seconds) for the lock to be released.
        break_lock
            Forcibly break the lock on locked documents.

        Returns
        -------
        str
            The db_id of the deleted Job.
        """

        job_lock_kwargs = dict(projection=["uuid", "index", "db_id", "state"])
        # avoid deleting jobs in batch states. It would require additional
        # specific handling and it is an unlikely use case.
        with self.lock_job_flow(
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            acceptable_states=DELETABLE_STATES,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
            if flow_lock.locked_document is None:
                raise RuntimeError("No document found in flow lock")

            # Update FlowDoc
            flow_doc = FlowDoc.model_validate(flow_lock.locked_document)
            job_uuid, job_index = job_doc["uuid"], job_doc["index"]

            if len(flow_doc.ids) == 1:
                raise RuntimeError(
                    "It is not possible to delete the only Job of the Flow. Delete the entire Flow."
                )

            # Remove job from ids list
            flow_doc.ids = [
                id_tuple
                for id_tuple in flow_doc.ids
                if id_tuple[1] != job_uuid or id_tuple[2] != job_index
            ]

            # Remove job from jobs list and as parent of other jobs if no job
            # with that id remains in the flow
            if not any(job_uuid == id_tuple[1] for id_tuple in flow_doc.ids):
                # Here a flow_doc.jobs.remove could be enough. But due to a previous
                # bug the list of jobs could contain the same uuid more than once.
                # Make sure to remove all the instances.
                flow_doc.jobs = [jid for jid in flow_doc.jobs if jid != job_uuid]

                for parent_dict in flow_doc.parents.values():
                    for index_list in parent_dict.values():
                        if job_uuid in index_list:
                            index_list.remove(job_uuid)

            # Remove job from parents
            flow_doc.parents[job_uuid].pop(str(job_index), None)
            # if all the jobs with a given uuid have been removed, also remove
            # the entry from the parents
            if not flow_doc.parents[job_uuid]:
                flow_doc.parents.pop(job_uuid, None)

            # Update flow state if necessary
            updated_states = {job_uuid: {job_index: None}}  # None indicates job removal
            self.update_flow_state(
                flow_uuid=flow_doc.uuid, updated_states=updated_states
            )

            # Prepare flow update
            flow_update = {
                "$set": {
                    "jobs": flow_doc.jobs,
                    "ids": flow_doc.ids,
                    "parents": flow_doc.parents,
                }
            }
            flow_update["$set"]["updated_on"] = datetime.utcnow()

            # Set flow update to be applied on lock release
            flow_lock.update_on_release = flow_update

            job_lock.delete_on_release = True

            # Optionally delete from jobstore
            if delete_output:
                try:
                    if delete_output:
                        self.jobstore.remove_docs({"uuid": job_id, "index": job_index})
                except Exception:
                    warnings.warn(
                        f"Error while delete the output of job {job_id} {job_index}",
                        stacklevel=2,
                    )

            if delete_files:
                job_info = JobInfo.from_query_output(job_doc)
                self._safe_delete_files([job_info])

            return job_doc["db_id"]

    def delete_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        delete_output: bool = False,
        delete_files: bool = False,
        max_limit: int = 10,
    ) -> list[str]:
        """
        Delete selected jobs from the queue store and optionally from the job store.
        The Flow document will be updated accordingly but no consistency check
        is performed. The Flow may be left in an inconsistent state.
        For advanced users only.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong.
        states
            One or more states of the Jobs.
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
        workers
            One or more worker names.
        custom_query
            A generic query. Incompatible with all the other filtering options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be deleted are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        delete_output : bool, default False
            If True, also delete the Job output from the JobStore.
        delete_files
            If True also delete the files on the worker.
        max_limit
            The Jobs will be deleted only if the total number is lower than the
            specified limit. 0 means no limit.

        Returns
        -------
        list
            List of db_ids of the deleted Jobs.
        """
        # Open the SharedHosts so that hosts will be shared for all the Jobs
        with SharedHosts(self.project):
            return self._many_jobs_action(
                method=self.delete_job,
                action_description="deleting",
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                states=states,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=workers,
                custom_query=custom_query,
                raise_on_error=raise_on_error,
                wait=wait,
                delete_output=delete_output,
                delete_files=delete_files,
                max_limit=max_limit,
            )

    def backup_dump(
        self,
        dir_path: str | Path = ".",
        mongo_bin_path: str | None = None,
        compress: bool = False,
        python: bool = False,
    ) -> dict[str, int]:
        """
        Create a backup of the queue database using either mongodump or a python implementation.
        The mongodump version is faster and stores metadata, but requires the mongodump executable
        and may not support all the connection options defined in the project configuration.
        The python version is available if the mongodump executable is not available and may support
        more connection options.

        Parameters
        ----------
        dir_path
            The path of the folder where the output files will be saved. Follows the mongodump
            convention: a subfolder with the name of the DB will created inside this path.
        mongo_bin_path
            The path to a folder containing the mongodump executable, if not present in the PATH.
        compress
            If True, the output files will be compressed with gzip.
        python
            If True a python implementation will be used to create a backup. WARNING: In this case
            metadata of the collections will not be saved.

        Returns
        -------
        dict[str, int]
            A dictionary containing the collection names as keys and the number of documents
            saved for each collection as values.
        """
        dir_path = Path(dir_path)
        doc_count = {}
        standard_collection_names = ["jobs", "flows", "jf_auxiliary"]
        if python:
            for std_name, collection in zip(
                standard_collection_names, [self.jobs, self.flows, self.auxiliary]
            ):
                doc_count[std_name] = pymongo_dump(
                    collection=collection, output_path=dir_path, compress=compress
                )
        else:
            for std_name, collection_name in zip(
                standard_collection_names,
                [
                    self.jobs_collection,
                    self.flows_collection,
                    self.auxiliary_collection,
                ],
            ):
                doc_count[std_name] = mongodump_from_store(
                    store=self.queue_store,
                    collection=collection_name,
                    output_path=dir_path,
                    compress=compress,
                    mongo_bin_path=mongo_bin_path,
                )

        # move the files produced to match the standard names
        for std_name, collection_name in zip(
            standard_collection_names,
            [self.jobs_collection, self.flows_collection, self.auxiliary_collection],
        ):
            if collection_name != std_name:
                full_dir_path = dir_path / self.queue_store.database
                data_file_name = f"{collection_name}.bson"
                if compress:
                    data_file_name += ".gz"
                file_path = full_dir_path / data_file_name
                if file_path.exists():
                    new_file_name = f"{std_name}.bson"
                    if compress:
                        new_file_name += ".gz"
                    file_path.rename(full_dir_path / new_file_name)
                metadata_file_name = f"{collection_name}.metadata.json"
                if compress:
                    metadata_file_name += ".gz"
                file_path = full_dir_path / metadata_file_name
                if file_path.exists():
                    new_file_name = f"{std_name}.metadata.json"
                    if compress:
                        new_file_name += ".gz"
                    file_path.rename(full_dir_path / new_file_name)

        return doc_count

    def backup_restore(
        self,
        dir_path: str | Path = ".",
        mongo_bin_path: str | None = None,
        compress: bool | None = None,
        python: bool = False,
    ):
        """
        Restore the queue database from a backup.

        It will restore the content of the collections jobs, flows and auxiliary
        from the files '<name>.bson(.gz)' in the given directory. If `python` is not
        selected, it will use the 'mongorestore' command with the specified 'mongo_bin_path'.
        If `python` is selected it will use a pure python implementation.

        Will allow to restore the backup with pristine collections.

        Parameters
        ----------
        dir_path
            The directory where to find the backup files.
        mongo_bin_path
            The path to a folder containing the mongodump executable, if not present in the PATH.
        compress
            If True the backup files are compressed. If None it will be determined based
            on the file extension.
        python
            If True, a pure python implementation will be used instead of the 'mongorestore'
            command. WARNING: In this case metadata of the collections will not be restored.
        """
        dir_path = Path(dir_path)
        collection_names = ["jobs", "flows", "jf_auxiliary"]
        for name, db_name, collection in zip(
            collection_names,
            [self.jobs_collection, self.flows_collection],
            [self.jobs, self.flows],
        ):
            count = collection.count_documents({})
            if count > 0:
                raise RuntimeError(
                    f"The collection named {db_name} for {name} contains {count} documents."
                    "Choose an empty collection."
                )
        next_id_doc = self.auxiliary.find_one({"next_id": {"$exists": True}})
        if next_id_doc and next_id_doc["next_id"] != 1:
            raise RuntimeError(
                "next_id value is different from one in the auxiliary collection. "
                "Choose an empty collection."
            )
        self.auxiliary.delete_many({})
        for name, db_name, collection in zip(
            collection_names,
            [self.jobs_collection, self.flows_collection, self.auxiliary_collection],
            [self.jobs, self.flows, self.auxiliary],
        ):
            file_name = f"{name}.bson"
            # compress may be set automatically in the restore functions, but if
            # compress is set explicitly to True the name should match.
            if compress:
                file_name += ".gz"
            files_paths = list(dir_path.glob(f"{file_name}*"))
            if len(files_paths) != 1:
                raise RuntimeError(
                    f"{len(files_paths)} files matching the name {file_name} were found in {dir_path}"
                )
            if python:
                pymongo_restore(collection=collection, input_file=files_paths[0])
            else:
                mongorestore_to_store(
                    store=self.queue_store,
                    collection=db_name,
                    input_file=files_paths[0],
                    compress=compress,
                    mongo_bin_path=mongo_bin_path,
                )

    def upgrade_check_jobflow(self):
        """
        Check if the jobflow version in the database matches the current one.
        Returns an empty string if they match, an error message otherwise.

        Returns
        -------
        str
            An empty string if configurations match, an error message otherwise.
        """
        environment_doc = self.auxiliary.find_one(
            {"jobflow_remote_version": {"$exists": True}}
        )
        if environment_doc is None:
            return (
                "No information about jobflow version in the database.\n"
                "The database is likely from before version 0.1.5 of jobflow-remote.\n"
            )

        previous_jobflow_version = environment_doc["jobflow_version"]
        current_jobflow_version = jobflow.__version__
        if jobflow.__version__ != previous_jobflow_version:
            return (
                f"The previous jobflow version ({previous_jobflow_version}) "
                f"is not the same as the current one ({current_jobflow_version}).\n"
            )
        return ""

    def upgrade_full_check(self):
        """
        Check if the packages used to generate the database match the current ones.
        Returns an empty string if they match, an error message otherwise.

        Returns
        -------
        str
            An empty string if configurations match, an error message otherwise.
        """
        environment_doc = self.auxiliary.find_one(
            {"jobflow_remote_version": {"$exists": True}}
        )
        if environment_doc is None:
            return (
                "No information about environment (all packages versions) in the database.\n"
                "The database is likely from before version 0.1.5 of jobflow-remote.\n"
            )

        previous_package_versions = environment_doc["full_environment"]
        installed_packages = importlib.metadata.distributions()
        package_versions = {
            package.metadata["Name"]: package.version for package in installed_packages
        }
        missing_packages = set(previous_package_versions.keys()) - set(
            package_versions.keys()
        )
        common_packages = set(previous_package_versions.keys()) & set(
            package_versions.keys()
        )
        different_version_packages = [
            (package, previous_package_versions[package], package_versions[package])
            for package in common_packages
            if previous_package_versions[package] != package_versions[package]
        ]
        if missing_packages or different_version_packages:
            msg = "Packages differ between the previous environment and the current one.\n"
            if missing_packages:
                missing_str = "\n- ".join(sorted(missing_packages))
                msg += (
                    f"The following {len(missing_packages)} packages "
                    f"are missing in the current environment:\n "
                    f"{missing_str}\n"
                )
            if different_version_packages:
                different_str = "\n- ".join(
                    [
                        f"{pkg[0]}-{pkg[2]} (previous version: {pkg[1]})"
                        for pkg in sorted(
                            different_version_packages, key=lambda x: x[0]
                        )
                    ]
                )
                msg += (
                    f"The following {len(missing_packages)} packages "
                    f"have different versions:\n "
                    f"{different_str}\n"
                )
            return msg
        return ""

    def update_version_information(
        self, jobflow_remote_version: str | Version | None = None
    ):
        """
        Update the version information in the database.

        This method will update the version information of jobflow-remote and jobflow,
        as well as the versions of all packages in the environment.

        Parameters
        ----------
        jobflow_remote_version
            The version of jobflow-remote to use. If ``None`` the current version of
            jobflow-remote is used.
        """
        installed_packages = importlib.metadata.distributions()
        jobflow_remote_version = jobflow_remote_version or jobflow_remote.__version__
        self.auxiliary.find_one_and_update(
            filter={"jobflow_remote_version": {"$exists": True}},
            update={
                "$set": {
                    "jobflow_remote_version": str(jobflow_remote_version),
                    "jobflow_version": jobflow.__version__,
                    "full_environment": {
                        package.metadata["Name"]: package.version
                        for package in installed_packages
                    },
                }
            },
            upsert=True,
        )

    def get_current_db_version(self) -> Version:
        """Get the current database version"""
        version_doc = self.auxiliary.find_one(
            {"jobflow_remote_version": {"$exists": True}}
        )
        if not version_doc:
            return parse_version("0.1.0")  # Default for old DBs
        return parse_version(version_doc["jobflow_remote_version"])

    @cached_property
    def queue_supports_transactions(self) -> bool:
        """
        Check if the version of MongoDB defined in the queue Store supports transactions.
        It explicitly tries to open a session and use it to verify.
        Cached property.

        Returns
        -------
        bool
            True if transactions are supported.
        """
        # note: there could be cheaper methods to check if it is supported,
        # but this should be 100% sure to give the right answer and should be
        # called only on rare events. If this becomes a more common requirement
        # a benchmark of different options will be required
        try:
            with self.db.client.start_session() as session, session.start_transaction():
                self.auxiliary.find_one({}, projection=[], session=session)
                return True
        except Exception:
            return False


def get_flow_leafs(job_docs: list[dict]) -> list[dict]:
    """
    Get the leaf jobs from a list of serialized representation of JobDoc.

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
