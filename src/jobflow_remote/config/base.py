import abc
import logging
import traceback
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal, Optional, Union

from jobflow import JobStore
from maggma.stores import MongoStore
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from qtoolkit.io import BaseSchedulerIO, scheduler_mapping

from jobflow_remote.remote.host import BaseHost, LocalHost, RemoteHost
from jobflow_remote.utils.data import store_from_dict

DEFAULT_JOBSTORE = {"docs_store": {"type": "MemoryStore"}}


class RunnerOptions(BaseModel):
    """
    Options to tune the execution of the Runner
    """

    delay_checkout: int = Field(
        30,
        description="Delay between subsequent execution of the checkout from database (seconds)",
    )
    delay_check_run_status: int = Field(
        30,
        description="Delay between subsequent execution of the checking the status of "
        "jobs that are submitted to the scheduler (seconds)",
    )
    delay_advance_status: int = Field(
        30,
        description="Delay between subsequent advancement of the job's remote state (seconds)",
    )
    delay_refresh_limited: int = Field(
        600,
        description="Delay between subsequent refresh from the DB of the number of submitted "
        "and running jobs (seconds). Only use if a worker with max_jobs is present",
    )
    delay_update_batch: int = Field(
        60,
        description="Delay between subsequent refresh from the DB of the number of submitted "
        "and running jobs (seconds). Only use if a worker with max_jobs is present",
    )
    lock_timeout: Optional[int] = Field(
        86400,
        description="Time to consider the lock on a document expired and can be overridden (seconds)",
    )
    delete_tmp_folder: bool = Field(
        True,
        description="Whether to delete the local temporary folder after a job has completed",
    )
    max_step_attempts: int = Field(
        3,
        description="Maximum number of attempt performed before failing an "
        "advancement of a remote state",
    )
    delta_retry: tuple[int, ...] = Field(
        (30, 300, 1200),
        description="List of increasing delay between subsequent attempts when the "
        "advancement of a remote step fails",
    )

    def get_delta_retry(self, step_attempts: int) -> int:
        """
        The time to wait before retrying a failed advancement of the remote state,
        based on the number of attempts.

        If exceeding the size of the list delta_retry, the last value is returned.

        Parameters
        ----------
        step_attempts
            The number of attempts advancing a remote state.
        Returns
        -------
            The delay in seconds.
        """
        ind = min(step_attempts, len(self.delta_retry)) - 1
        return self.delta_retry[ind]

    model_config = ConfigDict(extra="forbid")


class LogLevel(str, Enum):
    """
    Enumeration of logging level.
    """

    ERROR = "error"
    WARN = "warn"
    INFO = "info"
    DEBUG = "debug"

    def to_logging(self) -> int:
        """
        Helper converter to python logging values.

        Returns
        -------
        The int corresponding to python logging value
        """
        return {
            LogLevel.ERROR: logging.ERROR,
            LogLevel.WARN: logging.WARN,
            LogLevel.INFO: logging.INFO,
            LogLevel.DEBUG: logging.DEBUG,
        }[self]


class BatchConfig(BaseModel):
    jobs_handle_dir: Path = Field(
        description="Absolute path to a folder that will be used to store information to share with the jobs being executed"
    )
    work_dir: Path = Field(
        description="Absolute path to a folder where the batch jobs will be executed"
    )
    max_jobs: Optional[int] = Field(
        None, description="Maximum number of jobs executed in a single run in the queue"
    )
    max_wait: Optional[int] = Field(
        60,
        description="Maximum time to wait before stopping if no new jobs are available to run (seconds)",
    )
    max_time: Optional[int] = Field(
        None,
        description="Maximum time after which a job will not submit more jobs (seconds). To help avoid hitting the walltime",
    )
    model_config = ConfigDict(extra="forbid")


class WorkerBase(BaseModel):
    """
    Base class defining the common field for the different types of Worker.
    """

    type: str = Field(
        description="The discriminator field to determine the worker type"
    )

    scheduler_type: str = Field(
        description="Type of the scheduler. Depending on the values supported by QToolKit"
    )
    work_dir: Path = Field(
        description="Absolute path of the directory of the worker where subfolders for "
        "executing the calculation will be created"
    )
    resources: Optional[dict] = Field(
        None,
        description="A dictionary defining the default resources requested to the "
        "scheduler. Used to fill in the QToolKit template",
    )
    pre_run: Optional[str] = Field(
        None,
        description="String with commands that will be executed before the execution of the Job",
    )
    post_run: Optional[str] = Field(
        None,
        description="String with commands that will be executed after the execution of the Job",
    )
    timeout_execute: int = Field(
        60,
        description="Timeout for the execution of the commands in the worker "
        "(e.g. submitting a job)",
    )
    max_jobs: Optional[int] = Field(
        None,
        description="The maximum number of jobs that can be submitted to the queue.",
        ge=0,
    )
    batch: Optional[BatchConfig] = Field(
        None,
        description="Options for batch execution. If define the worker will be considered a batch worker",
    )
    model_config = ConfigDict(extra="forbid")

    @field_validator("scheduler_type")
    def check_scheduler_type(cls, scheduler_type: str) -> str:
        """
        Validator to set the default of scheduler_type
        """
        if scheduler_type not in scheduler_mapping:
            raise ValueError(f"Unknown scheduler type {scheduler_type}")
        return scheduler_type

    @field_validator("work_dir")
    def check_work_dir(cls, v) -> Path:
        if not v.is_absolute():
            raise ValueError("`work_dir` must be an absolute path")
        return v

    def get_scheduler_io(self) -> BaseSchedulerIO:
        """
        Get the BaseSchedulerIO from QToolKit depending on scheduler_type.

        Returns
        -------
        The instance of the scheduler_type.
        """
        if self.scheduler_type not in scheduler_mapping:
            raise ConfigError(f"Unknown scheduler type {self.scheduler_type}")
        return scheduler_mapping[self.scheduler_type]()

    @abc.abstractmethod
    def get_host(self) -> BaseHost:
        """
        Return the Host object used in the Worker.
        """

    @property
    @abc.abstractmethod
    def cli_info(self) -> dict:
        """
        Short information about the worker to be displayed in the command line
        interface.

        Returns
        -------
        A dictionary with the Worker short information.
        """

    @property
    def is_local(self) -> bool:
        return self.type == "local"


class LocalWorker(WorkerBase):
    """
    Worker representing the local host.

    Executes command directly.
    """

    type: Literal["local"] = Field(
        "local", description="The discriminator field to determine the worker type"
    )

    def get_host(self) -> BaseHost:
        """
        Return the LocalHost.

        Returns
        -------
        The LocalHost.
        """
        return LocalHost(timeout_execute=self.timeout_execute)

    @property
    def cli_info(self) -> dict:
        """
        Short information about the worker to be displayed in the command line
        interface.

        Returns
        -------
        A dictionary with the Worker short information.
        """
        return dict(
            scheduler_type=self.scheduler_type,
            work_dir=self.work_dir,
        )


class RemoteWorker(WorkerBase):
    """
    Worker representing a remote host reached through an SSH connection.

    Uses a Fabric Connection. Check Fabric documentation for more datails on the
    options defininf a Connection.
    """

    type: Literal["remote"] = Field(
        "remote", description="The discriminator field to determine the worker type"
    )
    host: str = Field(description="The host to which to connect")
    user: Optional[str] = Field(None, description="Login username")
    port: Optional[int] = Field(None, description="Port number")
    password: Optional[str] = Field(None, description="Login password")
    key_filename: Optional[Union[str, list[str]]] = Field(
        None,
        description="The filename, or list of filenames, of optional private key(s) "
        "and/or certs to try for authentication",
    )
    passphrase: Optional[str] = Field(
        None, description="Passphrase used for decrypting private keys"
    )
    gateway: Optional[str] = Field(
        None, description="A shell command string to use as a proxy or gateway"
    )
    forward_agent: Optional[bool] = Field(
        None, description="Whether to enable SSH agent forwarding"
    )
    connect_timeout: Optional[int] = Field(
        None, description="Connection timeout, in seconds"
    )
    connect_kwargs: Optional[dict] = Field(
        None,
        description="Other keyword arguments passed to paramiko.client.SSHClient.connect",
    )
    inline_ssh_env: Optional[bool] = Field(
        None,
        description="Whether to send environment variables 'inline' as prefixes in "
        "front of command strings",
    )
    keepalive: Optional[int] = Field(
        60, description="Keepalive value in seconds passed to paramiko's transport"
    )
    shell_cmd: Optional[str] = Field(
        "bash",
        description="The shell command used to execute the command remotely. If None "
        "the command is executed directly",
    )
    login_shell: bool = Field(
        True, description="Whether to use a login shell when executing the command"
    )

    def get_host(self) -> BaseHost:
        """
        Return the RemoteHost.

        Returns
        -------
        The RemoteHost.
        """
        connect_kwargs = dict(self.connect_kwargs) if self.connect_kwargs else {}
        if self.password:
            connect_kwargs["password"] = self.password
        if self.key_filename:
            connect_kwargs["key_filename"] = self.key_filename
        if self.passphrase:
            connect_kwargs["passphrase"] = self.passphrase
        return RemoteHost(
            host=self.host,
            user=self.user,
            port=self.port,
            gateway=self.gateway,
            forward_agent=self.forward_agent,
            connect_timeout=self.connect_timeout,
            connect_kwargs=connect_kwargs,
            inline_ssh_env=self.inline_ssh_env,
            timeout_execute=self.timeout_execute,
            keepalive=self.keepalive,
            shell_cmd=self.shell_cmd,
            login_shell=self.login_shell,
        )

    @property
    def cli_info(self) -> dict:
        """
        Short information about the worker to be displayed in the command line
        interface.

        Returns
        -------
        A dictionary with the Worker short information.
        """
        return dict(
            host=self.host,
            scheduler_type=self.scheduler_type,
            work_dir=self.work_dir,
        )


WorkerConfig = Annotated[Union[LocalWorker, RemoteWorker], Field(discriminator="type")]


class ExecutionConfig(BaseModel):
    """
    Configuration to be set before and after the execution of a Job.
    """

    modules: Optional[list[str]] = Field(
        None, description="list of modules to be loaded"
    )
    export: Optional[dict[str, Any]] = Field(
        None, description="dictionary with variable to be exported"
    )
    pre_run: Optional[str] = Field(
        None, description="Other commands to be executed before the execution of a job"
    )
    post_run: Optional[str] = Field(
        None, description="Commands to be executed after the execution of a job"
    )
    model_config = ConfigDict(extra="forbid")


class QueueConfig(BaseModel):
    store: dict = Field(
        default_factory=dict,
        description="Dictionary describing a maggma Store used for the queue data. "
        "Can contain the monty serialized dictionary or a dictionary with a 'type' "
        "specifying the Store subclass. Should be subclass of a MongoStore, as it "
        "requires to perform MongoDB actions. The collection is used to store the "
        "jobs",
        validate_default=True,
    )
    flows_collection: str = Field(
        "flows",
        description="The name of the collection containing information about the flows. "
        "Taken from the same database as the one defined in the store",
    )
    auxiliary_collection: str = Field(
        "jf_auxiliary",
        description="The name of the collection containing auxiliary information. "
        "Taken from the same database as the one defined in the store",
    )
    db_id_prefix: Optional[str] = Field(
        None,
        description="a string defining the prefix added to the integer ID associated "
        "to each Job in the database",
    )

    @field_validator("store")
    def check_store(cls, store: dict) -> dict:
        """
        Check that the queue configuration could be converted to a Store.
        """
        if store:
            try:
                deserialized_store = store_from_dict(store)
            except Exception as e:
                raise ValueError(
                    f"error while converting queue to a maggma store. Error: {traceback.format_exc()}"
                ) from e
            if not isinstance(deserialized_store, MongoStore):
                raise ValueError(
                    "The queue store should be a subclass of a "
                    f"MongoStore: {deserialized_store.__class__} instead"
                )
        return store

    model_config = ConfigDict(extra="forbid")


class Project(BaseModel):
    """
    The configurations of a Project.
    """

    name: str = Field(description="The name of the project")
    base_dir: Optional[str] = Field(
        None,
        description="The base directory containing the project related files. Default "
        "is a folder with the project name inside the projects folder",
        validate_default=True,
    )
    tmp_dir: Optional[str] = Field(
        None,
        description="Folder where remote files are copied. Default a 'tmp' folder in base_dir",
        validate_default=True,
    )
    log_dir: Optional[str] = Field(
        None,
        description="Folder containing all the logs. Default a 'log' folder in base_dir",
        validate_default=True,
    )
    daemon_dir: Optional[str] = Field(
        None,
        description="Folder containing daemon related files. Default to a 'daemon' "
        "folder in base_dir",
        validate_default=True,
    )
    log_level: LogLevel = Field(LogLevel.INFO, description="The level set for logging")
    runner: RunnerOptions = Field(
        default_factory=RunnerOptions, description="The options for the Runner"
    )
    workers: dict[str, WorkerConfig] = Field(
        default_factory=dict,
        description="A dictionary with the worker name as keys and the worker "
        "configuration as values",
    )
    queue: QueueConfig = Field(
        description="The configuration of the Store used to store the states of"
        "the Jobs and the Flows",
    )
    exec_config: dict[str, ExecutionConfig] = Field(
        default_factory=dict,
        description="A dictionary with the ExecutionConfig name as keys and the "
        "ExecutionConfig configuration as values",
    )
    jobstore: dict = Field(
        default_factory=lambda: dict(DEFAULT_JOBSTORE),
        description="The JobStore used for the input. Can contain the monty "
        "serialized dictionary or the Store int the Jobflow format",
        validate_default=True,
    )
    metadata: Optional[dict] = Field(
        None, description="A dictionary with metadata associated to the project"
    )

    def get_jobstore(self) -> Optional[JobStore]:
        """
        Generate an instance of the JobStore based on the configuration

        Returns
        -------
        A JobStore
        """
        if not self.jobstore:
            return None
        elif self.jobstore.get("@class") == "JobStore":
            return JobStore.from_dict(self.jobstore)
        else:
            return JobStore.from_dict_spec(self.jobstore)

    def get_queue_store(self):
        """
        Generate an instance of a maggma Store based on the queue configuration.

        Returns
        -------
        A maggma Store
        """
        return store_from_dict(self.queue.store)

    def get_job_controller(self):
        from jobflow_remote.jobs.jobcontroller import JobController

        return JobController.from_project(self)

    @field_validator("base_dir")
    def check_base_dir(cls, base_dir: str, info: ValidationInfo) -> str:
        """
        Validator to set the default of base_dir based on the project name
        """
        if not base_dir:
            from jobflow_remote import SETTINGS

            return str(Path(SETTINGS.projects_folder, info.data["name"]))
        return base_dir

    @field_validator("tmp_dir")
    def check_tmp_dir(cls, tmp_dir: str, info: ValidationInfo) -> str:
        """
        Validator to set the default of tmp_dir based on the base_dir
        """
        if not tmp_dir:
            return str(Path(info.data["base_dir"], "tmp"))
        return tmp_dir

    @field_validator("log_dir")
    def check_log_dir(cls, log_dir: str, info: ValidationInfo) -> str:
        """
        Validator to set the default of log_dir based on the base_dir
        """
        if not log_dir:
            return str(Path(info.data["base_dir"], "log"))
        return log_dir

    @field_validator("daemon_dir")
    def check_daemon_dir(cls, daemon_dir: str, info: ValidationInfo) -> str:
        """
        Validator to set the default of daemon_dir based on the base_dir
        """
        if not daemon_dir:
            return str(Path(info.data["base_dir"], "daemon"))
        return daemon_dir

    @field_validator("jobstore")
    def check_jobstore(cls, jobstore: dict) -> dict:
        """
        Check that the jobstore configuration could be converted to a JobStore.
        """
        if jobstore:
            try:
                if jobstore.get("@class") == "JobStore":
                    JobStore.from_dict(jobstore)
                else:
                    JobStore.from_dict_spec(jobstore)
            except Exception as e:
                raise ValueError(
                    f"error while converting jobstore to JobStore. Error: {traceback.format_exc()}"
                ) from e
        return jobstore

    model_config = ConfigDict(extra="forbid")


class ConfigError(Exception):
    """
    A generic Exception related to the configuration
    """


class ProjectUndefined(ConfigError):
    """
    Exception raised if the Project has not been defined or could not be determined.
    """
