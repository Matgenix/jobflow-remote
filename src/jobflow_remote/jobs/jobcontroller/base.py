from __future__ import annotations

import contextlib
import logging
import shutil
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from monty.os import makedirs_p
from qtoolkit.core.data_objects import CancelStatus, QResources

from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.jobs.data import (
    DbCollection,
    DynamicResponseType,
    FlowInfo,
    JobDoc,
    JobInfo,
)
from jobflow_remote.remote.data import get_local_data_path
from jobflow_remote.remote.queue import QueueManager
from jobflow_remote.utils.remote import SharedHosts, safe_remove_job_files

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence
    from datetime import datetime
    from enum import Enum
    from typing import Union

    import jobflow
    from jobflow import JobStore
    from monty.json import MSONable
    from packaging.version import Version

    from jobflow_remote.config.base import ExecutionConfig, Project
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.utils.db import MongoLock

    obj_type = Union[str, Enum, type[MSONable], list[Union[Enum, str, type[MSONable]]]]
    load_type = Union[bool, dict[str, Union[bool, obj_type]]]


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
        jobstore: JobStore,
        project: Project | None = None,
        optional_jobstores: dict[str, JobStore] | None = None,
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
        optional_jobstores
            A dictionary of optional JobStores as defined in the project.
        """
        self.jobstore = jobstore
        self.optional_jobstores = optional_jobstores or {}
        self.jobstore.connect()
        for opt_js in self.optional_jobstores.values():
            opt_js.connect()
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
        from jobflow_remote.jobs.jobcontroller.mongo import MongoJobController
        from jobflow_remote.jobs.jobcontroller.sql import SQLJobController

        jobstore = project.get_jobstore()
        optional_jobstores = {}
        if project.optional_jobstores:
            for js_name in project.optional_jobstores:
                optional_jobstores[js_name] = project.get_jobstore(name=js_name)

        queue_config_store = project.queue.store
        if (
            isinstance(queue_config_store, dict)
            and queue_config_store.get("type", "").lower() == "sqlite"
        ):
            # assume unix file path
            sqlite_fp = queue_config_store.get("filepath")
            if not sqlite_fp:
                sqlite_fp = Path(project.base_dir) / "queue_db" / "queue_sql.db"
                makedirs_p(sqlite_fp.parent)
            return SQLJobController(
                db_url=f"sqlite:///{sqlite_fp!s}",
                jobstore=jobstore,
                project=project,
                optional_jobstores=optional_jobstores,
            )

        queue_store = project.get_queue_store()
        flows_collection = project.queue.flows_collection
        auxiliary_collection = project.queue.auxiliary_collection

        return MongoJobController(
            queue_store=queue_store,
            jobstore=jobstore,
            flows_collection=flows_collection,
            auxiliary_collection=auxiliary_collection,
            project=project,
            optional_jobstores=optional_jobstores,
        )

    def close(self) -> None:
        """Close the connections to all the Stores in JobController."""
        raise NotImplementedError

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
        raise NotImplementedError

    def get_jobs_info(
        self,
        custom_query: dict | None = None,
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
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
        skip
            The number of documents to omit (from the start of the result set).

        Returns
        -------
        list
            A list of JobInfo objects for the Jobs matching the criteria.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def get_jobs_doc(
        self,
        custom_query: dict | None = None,
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
        custom_query
            A dictionary representing the filter.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
        raise NotImplementedError

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
            A dict and an optional list to be used as filter and sort,
            respectively, in a query for a single Job.
        """
        raise NotImplementedError

    @staticmethod
    def generate_flow_id_query(
        db_id: str | None = None,
        job_id: str | None = None,
        flow_id: str | None = None,
    ) -> dict:
        """
        Generate a query for a single Flow based on the ids.
        Only one among the input options should be defined.

        Parameters
        ----------
        db_id
            The db_id of one Job belonging to the Flow.
        job_id
            The uuid of one Job belonging to the Flow.
        flow_id
            The uuid of the Flow.

        Returns
        -------
        dict
            A dict to be used as filter in a query for a single Job.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

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
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

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

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def resume_jobs(
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
        Restart selected Jobs that were previously paused or stopped.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

    def resume_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Restart a single Jobs that was previously paused or stopped.
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
        raise NotImplementedError

    def _resume_job_locked(self, job_doc) -> JobState:
        """
        Helper method for the logic of resuming a Job.
        Assumes the input is the dictionary representation of a JobDoc and that
        the Flow and Job has been locked.

        Parameters
        ----------
        job_doc
            Dictionary representing the JobDoc with the required elements presents.

        Returns
        -------
        JobState
            The final state that should be set to a Job.
        """
        raise NotImplementedError

    def resume_flow(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        flow_id: str | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> int:
        """
        Resume a Flow by resuming all the STOPPED, USER_STOPPED and PAUSED Jobs
        in the Flow.

        Parameters
        ----------
        job_id
            The uuid of one of the Jobs of the Flow.
        db_id
            The db_id of one of the Jobs of the Flow.
        flow_id
            The uuid of the Flow.
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
        int
            The number of Jobs modified.
        """
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        skip
            The number of documents to omit (from the start of the result set).

        Returns
        -------
        list
            A list of FlowInfo.
        """
        raise NotImplementedError

    def get_flow_store(self, flow_id: str) -> str | None:
        """
        Fetch the name of the optional JobStore to store the outputs
        of a Flow, if defined.

        Parameters
        ----------
        flow_id
            The uuid of the Flow
        Returns
        -------
        str
            The name of one of the optional JobStores to be used.
            None if the default should be used.
        """
        raise NotImplementedError

    def set_flow_store(
        self,
        store: str | None,
        db_id: str | None = None,
        job_id: str | None = None,
        flow_id: str | None = None,
    ) -> None:
        """
        Set the name of the optional JobStore to store the outputs
        of a Flow. If None the default JobStore will be used.
        Can be changed only for READY Flows.

        Parameters
        ----------
        store
            The name of one of the optional JobStores. If None sets to the
            default JobStore.
        db_id
            The db_id of one Job belonging to the Flow.
        job_id
            The uuid of one Job belonging to the Flow.
        flow_id
            The uuid of the Flow.

        """

    def delete_flows(
        self,
        flow_ids: str | list[str] | None = None,
        max_limit: int = 10,
        delete_output: bool = False,
        delete_files: bool = False,
        cancel_processes: bool = True,
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
        cancel_processes
            If True will attempt to delete the processes for SUBMITTED and RUNNING jobs.
            Failure to cancel will not stop the deletion of the Flow.

        Returns
        -------
        int
            Number of deleted Flows.
        """
        raise NotImplementedError

    def delete_flow(
        self,
        flow_id: str,
        delete_output: bool = False,
        delete_files: bool = False,
        cancel_processes: bool = True,
    ) -> bool:
        """
        Delete a single Flow based on the uuid.

        Parameters
        ----------
        flow_id
            One Flow ids. Can be db_id or uuid.
        delete_output
            If True also delete the associated output in the JobStore.
        delete_files
            If True also delete the files on the worker.
        cancel_processes
            If True will attempt to delete the processes for SUBMITTED and RUNNING jobs.
            Failure to cancel will not stop the deletion of the Flow.

        Returns
        -------
        bool
            True if the flow has been deleted.
        """
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def compact(self) -> None:
        """Compact jobs and flows collections in MongoDB."""
        raise NotImplementedError

    def get_flow_info_by_flow_uuid(
        self, flow_uuid: str, projection: list | dict | None = None
    ):
        raise NotImplementedError

    def get_flow_info_by_job_uuid(
        self, job_uuid: str, projection: list | dict | None = None
    ):
        raise NotImplementedError

    def get_job_info_by_job_uuid(
        self,
        job_uuid: str,
        job_index: int | str = "last",
        projection: list | dict | None = None,
    ):
        raise NotImplementedError

    def get_job_info_by_pid(self, pid: int | str) -> JobInfo | None:
        """
        Retrieve job information by process ID (e.g., Slurm job ID).

        Args:
            pid (int): The process ID of the job in the queue system.

        Returns:
            JobInfo | None: Job information if found, None otherwise.
        """
        raise NotImplementedError

    def get_job_doc(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
    ) -> JobDoc | None:
        raise NotImplementedError

    def get_jobs(self, query, projection: list | dict | None = None):
        raise NotImplementedError

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
            A generic query.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
        raise NotImplementedError

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
        metadata: dict | None = None,
        locked: bool = False,
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
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        locked
            If True only locked Flows will be counted.

        Returns
        -------
        int
            Number of Flows matching the criteria.
        """
        raise NotImplementedError

    def count_jobs_states(
        self, states: list[JobState], worker: str | None = None
    ) -> dict[JobState, int]:
        """
        Count the number of jobs in each of the given states.

        Parameters
        ----------
        states
            List of JobState to count.
        worker
            Name of the worker

        Returns
        -------
        dict[JobState, int]
            A dictionary with the count of jobs in each state.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def get_jobs_info_by_flow_uuid(
        self, flow_uuid, projection: list | dict | None = None
    ):
        raise NotImplementedError

    def add_flow(
        self,
        flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
        worker: str,
        allow_external_references: bool = False,
        exec_config: ExecutionConfig | None = None,
        resources: dict | QResources | None = None,
        priority: int = 0,
        jobstore: str | None = None,
    ) -> list[str]:
        raise NotImplementedError

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
        stopped: bool = False,
    ) -> None:
        """
        Append a new Flow to an existing one as a child of a specific Job.

        Parameters
        ----------
        job_doc
            The dictionary representation of the JobDoc of the Job to which
             the new Flow will be appended.
        flow_dict
            The dictionary of the original Flow.
        new_flow_dict
            The dictionary of the new Flow.
        worker
            The default worker applied to the newly created Jobs, if not
            overridden by specific Job configurations.
        response_type
            Type or response.
        exec_config
            ExecConfig inherited from the generating Job, if not overridden
            by specific Job updates.
        resources
            Resources inherited from the generating Job, if not overridden
            by specific Job updates.
        priority
            Priority inherited from the generating Job.
        stopped
            If True the generated Jobs will be set in the STOPPED state.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def complete_job(
        self, job_doc: dict, local_path: Path | str, store: JobStore
    ) -> bool:
        raise NotImplementedError

    def checkin_job(
        self,
        job_doc: dict,
        flow_dict: dict,
        response: dict | None,
        error: str | None = None,
        doc_update: dict | None = None,
    ):
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def ping_running_runner(self) -> bool:
        """
        Ping the running_runner document, if exists and has been activated as a daemon.

        Returns
        -------
        bool
            True if the ping was successful.
        """
        raise NotImplementedError

    def update_flow_state(
        self,
        flow_uuid: str,
        updated_states: dict[str, dict[int, JobState | None]] | None = None,
    ) -> FlowState:
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

        Returns
        -------
        FlowState
            The state set for the Flow.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
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
            A generic query. Keys must not overlap with other specified query options.
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
        raise NotImplementedError

    def get_job_output(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        load: load_type = False,
    ) -> Any:
        """
        Get the output of a single Job based on db_id or uuid+index.
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
        load
            Which items to load from additional stores. Setting to ``True`` will load
            all items stored in additional stores. See the ``JobStore`` constructor for
            more details.

        Returns
        -------
        Any
            The output(s) for the job
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

    def upgrade_check_jobflow(self):
        """
        Check if the jobflow version in the database matches the current one.
        Returns an empty string if they match, an error message otherwise.

        Returns
        -------
        str
            An empty string if configurations match, an error message otherwise.
        """
        raise NotImplementedError

    def upgrade_full_check(self):
        """
        Check if the packages used to generate the database match the current ones.
        Returns an empty string if they match, an error message otherwise.

        Returns
        -------
        str
            An empty string if configurations match, an error message otherwise.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def get_current_db_version(self) -> Version:
        """Get the current database version"""
        raise NotImplementedError

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
        return False
