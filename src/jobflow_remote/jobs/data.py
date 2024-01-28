from collections import defaultdict
from datetime import datetime
from enum import Enum
from functools import cached_property
from typing import Optional, Union

from jobflow import Flow, Job
from monty.json import jsanitize
from pydantic import BaseModel, Field
from qtoolkit.core.data_objects import QResources, QState

from jobflow_remote.config.base import ExecutionConfig
from jobflow_remote.jobs.state import FlowState, JobState

IN_FILENAME = "jfremote_in.json"
OUT_FILENAME = "jfremote_out.json"


def get_initial_job_doc_dict(
    job: Job,
    parents: Optional[list[str]],
    db_id: str,
    worker: str,
    exec_config: Optional[ExecutionConfig],
    resources: Optional[Union[dict, QResources]],
) -> dict:
    """
    Generate an instance of JobDoc for initial insertion in the DB.

    Parameters
    ----------
    job:
        The Job of the JobDoc.
    parents
        The parents of the Job.
    db_id
        The db_id.
    worker
        The worker where the Job should be executed.
    exec_config
        The ExecutionConfig used for execution.
    resources
        The resources used to run the Job.
    Returns
    -------
    JobDoc
        A new JobDoc.
    """
    from monty.json import jsanitize

    # take the resources either from the job, if they are defined
    # (they can be defined dynamically by the update_config) or the
    # defined value
    job_resources = job.config.manager_config.get("resources") or resources
    job_exec_config = job.config.manager_config.get("exec_config") or exec_config
    worker = job.config.manager_config.get("worker") or worker

    job_doc = JobDoc(
        job=jsanitize(job, strict=True, enum_values=True),
        uuid=job.uuid,
        index=job.index,
        db_id=db_id,
        state=JobState.WAITING if parents else JobState.READY,
        parents=parents,
        worker=worker,
        exec_config=job_exec_config,
        resources=job_resources,
    )

    return job_doc.as_db_dict()


def get_initial_flow_doc_dict(flow: Flow, job_dicts: list[dict]) -> dict:
    """
    Generate a serialized FlowDoc for initial insertion in the DB.

    Parameters
    ----------
    flow
        The Flow used to generate the FlowDoc.
    job_dicts
        The dictionaries of the Jobs composing the Flow.
    Returns
    -------
    dict
        A serialized version of a new FlowDoc.
    """
    jobs = [j["uuid"] for j in job_dicts]
    ids = [(j["db_id"], j["uuid"], j["index"]) for j in job_dicts]
    parents = {j["uuid"]: {"1": j["parents"]} for j in job_dicts}

    flow_doc = FlowDoc(
        uuid=flow.uuid,
        jobs=jobs,
        state=FlowState.READY,
        name=flow.name,
        ids=ids,
        parents=parents,
    )

    return flow_doc.as_db_dict()


class RemoteInfo(BaseModel):
    """
    Model with data describing the remote state of a Job.
    """

    step_attempts: int = 0
    queue_state: Optional[QState] = None
    process_id: Optional[str] = None
    retry_time_limit: Optional[datetime] = None
    error: Optional[str] = None


class JobInfo(BaseModel):
    """
    Model with information extracted from a JobDoc.
    Mainly for visualization purposes.
    """

    uuid: str
    index: int
    db_id: str
    worker: str
    name: str
    state: JobState
    created_on: datetime
    updated_on: datetime
    remote: RemoteInfo = RemoteInfo()
    parents: Optional[list[str]] = None
    previous_state: Optional[JobState] = None
    error: Optional[str] = None
    lock_id: Optional[str] = None
    lock_time: Optional[datetime] = None
    run_dir: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    priority: int = 0
    metadata: Optional[dict] = None

    @property
    def is_locked(self) -> bool:
        return self.lock_id is not None

    @property
    def run_time(self) -> Optional[float]:
        """
        Calculate the run time based on start and end time.

        Returns
        -------
        float
            The run time in seconds
        """
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()

        return None

    @property
    def estimated_run_time(self) -> Optional[float]:
        """
        Estimate the current run time based on the start time and the current time.

        Returns
        -------
        float
            The estimated run time in seconds.
        """
        if self.start_time:
            return (
                datetime.now(tz=self.start_time.tzinfo) - self.start_time
            ).total_seconds()

        return None

    @classmethod
    def from_query_output(cls, d) -> "JobInfo":
        """
        Generate an instance from the output of a query to the JobDoc collection.

        Parameters
        ----------
        d
            The dictionary with the queried data.
        Returns
        -------
        JobInfo
            The instance of JobInfo based on the data
        """
        job = d.pop("job")
        for k in ["name", "metadata"]:
            d[k] = job[k]
        return cls.model_validate(d)


def _projection_db_info() -> list[str]:
    """
    Generate a list of fields used for projection, depending on the JobInfo model.

    Returns
    -------
    list
        The list of fields to use in a query.
    """
    projection = list(JobInfo.model_fields.keys())
    projection.remove("name")
    projection.append("job.name")
    projection.append("job.metadata")
    return projection


# generate the list only once.
projection_job_info = _projection_db_info()


class JobDoc(BaseModel):
    """
    Model for the standard representation of a Job in the queue database.
    """

    # TODO consider defining this as a dict and provide a get_job() method to
    # get the real Job. This would avoid (de)serializing jobs if this document
    # is used often to interact with the DB.
    job: Job
    uuid: str
    index: int
    db_id: str
    worker: str
    state: JobState
    remote: RemoteInfo = RemoteInfo()
    # only the uuid as list of parents for a JobDoc (i.e. uuid+index) is
    # enough to determine the parents, since once a job with a uuid is
    # among the parents, all the index will still be parents.
    # Note that for just the uuid this condition is not true: JobDocs with
    # the same uuid but different indexes may have different parents
    parents: Optional[list[str]] = None
    previous_state: Optional[JobState] = None
    error: Optional[str] = None  # TODO is there a better way to serialize it?
    lock_id: Optional[str] = None
    lock_time: Optional[datetime] = None
    run_dir: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    priority: int = 0
    # store: Optional[JobStore] = None
    exec_config: Optional[Union[ExecutionConfig, str]] = None
    resources: Optional[Union[QResources, dict]] = None

    stored_data: Optional[dict] = None
    # history: Optional[list[str]] = None

    def as_db_dict(self) -> dict:
        """
        Generate a dict representation suitable to be inserted in the database.

        Returns
        -------
        dict
            The dict representing the JobDoc.
        """
        d = jsanitize(
            self.model_dump(mode="python"),
            strict=True,
            allow_bson=True,
            enum_values=True,
        )
        # required since the resources are not serialized otherwise
        if isinstance(self.resources, QResources):
            d["resources"] = self.resources.as_dict()
        return d


class FlowDoc(BaseModel):
    """
    Model for the standard representation of a Flow in the queue database.
    """

    uuid: str
    jobs: list[str]
    state: FlowState
    name: str
    lock_id: Optional[str] = None
    lock_time: Optional[datetime] = None
    created_on: datetime = Field(default_factory=datetime.utcnow)
    updated_on: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)
    # parents need to include both the uuid and the index.
    # When dynamically replacing a Job with a Flow some new Jobs will
    # be parents of the job with index=i+1, but will not be parents of
    # the job with index i.
    # index is stored as string, since mongodb needs string keys
    # This dictionary include {job uuid: {job index: [parent's uuids]}}
    parents: dict[str, dict[str, list[str]]] = Field(default_factory=dict)
    # ids correspond to db_id, uuid, index for each JobDoc
    ids: list[tuple[str, str, int]] = Field(default_factory=list)

    def as_db_dict(self) -> dict:
        """
        Generate a dict representation suitable to be inserted in the database.

        Returns
        -------
        dict
            The dict representing the FlowDoc.
        """
        d = jsanitize(
            self.model_dump(mode="python"),
            strict=True,
            allow_bson=True,
            enum_values=True,
        )
        return d

    @cached_property
    def int_index_parents(self):
        d = defaultdict(dict)
        for child_id, index_parents in self.parents.items():
            for index, parents in index_parents.items():
                d[child_id][int(index)] = parents
        return dict(d)

    @cached_property
    def children(self) -> dict[str, list[tuple[str, int]]]:
        d = defaultdict(list)
        for job_id, index_parents in self.parents.items():
            for index, parents in index_parents.items():
                for parent_id in parents:
                    d[parent_id].append((job_id, int(index)))

        return dict(d)

    def descendants(self, job_uuid: str) -> list[tuple[str, int]]:
        descendants = set()

        def add_descendants(uuid):
            children = self.children.get(uuid)
            if children:
                descendants.update(children)
                for child in children:
                    add_descendants(child[0])

        add_descendants(job_uuid)

        return list(descendants)

    @cached_property
    def ids_mapping(self) -> dict[str, dict[int, str]]:
        d: dict = defaultdict(dict)

        for db_id, job_id, index in self.ids:
            d[job_id][int(index)] = db_id

        return dict(d)


class RemoteError(RuntimeError):
    """
    An exception signaling errors during the update of the remote states.
    """

    def __init__(self, msg, no_retry=False):
        self.msg = msg
        self.no_retry = no_retry


class FlowInfo(BaseModel):
    """
    Model with information extracted from a FlowDoc.
    Mainly for visualization purposes.
    """

    db_ids: list[str]
    job_ids: list[str]
    job_indexes: list[int]
    flow_id: str
    state: FlowState
    name: str
    created_on: datetime
    updated_on: datetime
    workers: list[str]
    job_states: list[JobState]
    job_names: list[str]
    parents: list[list[str]]
    hosts: list[list[str]]

    @classmethod
    def from_query_dict(cls, d) -> "FlowInfo":
        created_on = d["created_on"]
        updated_on = d["updated_on"]
        flow_id = d["uuid"]
        jobs_data = d.get("jobs_list") or []

        workers = []
        job_states = []
        job_names = []
        parents = []
        job_hosts = []

        if jobs_data:
            db_ids = []
            job_ids = []
            job_indexes = []
            for job_doc in jobs_data:
                db_ids.append(job_doc["db_id"])
                job_ids.append(job_doc["uuid"])
                job_indexes.append(job_doc["index"])
                job_names.append(job_doc["job"]["name"])
                state = job_doc["state"]
                job_states.append(JobState(state))
                workers.append(job_doc["worker"])
                parents.append(job_doc["parents"] or [])
                job_hosts.append(job_doc["job"]["hosts"] or [])
        else:
            db_ids, job_ids, job_indexes = list(  # type:ignore[assignment]
                zip(*d["ids"])
            )
            # parents could be determined in this case as well from the Flow document.
            # However, to match the correct order it would require lopping over them.
            # To keep the generation faster add this only if a use case shows up.

        state = FlowState(d["state"])

        return cls(
            db_ids=db_ids,
            job_ids=job_ids,
            job_indexes=job_indexes,
            flow_id=flow_id,
            state=state,
            name=d["name"],
            created_on=created_on,
            updated_on=updated_on,
            workers=workers,
            job_states=job_states,
            job_names=job_names,
            parents=parents,
            hosts=job_hosts,
        )

    @cached_property
    def ids_mapping(self) -> dict[str, dict[int, str]]:
        d: dict = defaultdict(dict)

        for db_id, job_id, index in zip(self.db_ids, self.job_ids, self.job_indexes):
            d[job_id][int(index)] = db_id

        return dict(d)

    def iter_job_prop(self):
        n_jobs = len(self.job_ids)
        for i in range(n_jobs):
            d = {
                "db_id": self.db_ids[i],
                "uuid": self.job_ids[i],
                "index": self.job_indexes[i],
            }
            if self.job_names:
                d["name"] = self.job_names[i]
                d["state"] = self.job_states[i]
                d["parents"] = self.parents[i]
                d["hosts"] = self.hosts[i]
            yield d


class DynamicResponseType(Enum):
    """
    Types of dynamic responses in jobflow.
    """

    REPLACE = "replace"
    DETOUR = "detour"
    ADDITION = "addition"


def get_reset_job_base_dict() -> dict:
    """
    Generate a dictionary with the basic properties to update in case of reset.

    Returns
    -------
    dict
        Data to be reset.
    """
    d = {
        "remote.step_attempts": 0,
        "remote.retry_time_limit": None,
        "previous_state": None,
        "remote.queue_state": None,
        "remote.error": None,
        "error": None,
        "updated_on": datetime.utcnow(),
        "start_time": None,
        "end_time": None,
    }
    return d
