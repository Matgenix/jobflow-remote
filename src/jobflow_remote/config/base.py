from __future__ import annotations

import abc
import logging
import traceback
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

from jobflow import JobStore
from pydantic import BaseModel, Extra, Field, validator
from qtoolkit.io import BaseSchedulerIO, scheduler_mapping

from jobflow_remote.fireworks.launchpad import RemoteLaunchPad
from jobflow_remote.remote.host import BaseHost, LocalHost, RemoteHost
from jobflow_remote.utils.data import store_from_dict

DEFAULT_JOBSTORE = {"docs_store": {"type": "MemoryStore"}}


class RunnerOptions(BaseModel):
    delay_checkout: int = 30
    delay_check_run_status: int = 30
    delay_advance_status: int = 30
    lock_timeout: int | None = 7200
    delete_tmp_folder: bool = True
    max_step_attempts: int = 3
    delta_retry: tuple[int, ...] = (30, 300, 1200)

    def get_delta_retry(self, step_attempts: int):
        ind = min(step_attempts, len(self.delta_retry)) - 1
        return self.delta_retry[ind]

    class Config:
        extra = Extra.forbid


class LogLevel(str, Enum):
    ERROR = "error"
    WARN = "warn"
    INFO = "info"
    DEBUG = "debug"

    def to_logging(self) -> int:
        return {
            LogLevel.ERROR: logging.ERROR,
            LogLevel.WARN: logging.WARN,
            LogLevel.INFO: logging.INFO,
            LogLevel.DEBUG: logging.DEBUG,
        }[self]


class WorkerBase(BaseModel):

    scheduler_type: str
    work_dir: str
    resources: dict | None = None
    pre_run: str | None = None
    post_run: str | None = None
    timeout_execute: int = 60

    class Config:
        extra = Extra.forbid

    @validator("scheduler_type", always=True)
    def check_scheduler_type(cls, scheduler_type: str, values: dict) -> str:
        """
        Validator to set the default of scheduler_type
        """
        if scheduler_type not in scheduler_mapping:
            raise ValueError(f"Unknown scheduler type {scheduler_type}")
        return scheduler_type

    def get_scheduler_io(self) -> BaseSchedulerIO:
        if self.scheduler_type not in scheduler_mapping:
            raise ConfigError(f"Unknown scheduler type {self.scheduler_type}")
        return scheduler_mapping[self.scheduler_type]()

    @abc.abstractmethod
    def get_host(self) -> BaseHost:
        pass


class LocalWorker(WorkerBase):

    type: Literal["local"] = "local"

    def get_host(self) -> BaseHost:
        return LocalHost(timeout_execute=self.timeout_execute)


class RemoteWorker(WorkerBase):

    type: Literal["remote"] = "remote"
    host: str
    user: str = None
    port: int = None
    gateway: str = None
    forward_agent: bool = None
    connect_timeout: int = None
    connect_kwargs: dict = None
    inline_ssh_env: bool = None

    def get_host(self) -> BaseHost:
        return RemoteHost(
            host=self.host,
            user=self.user,
            port=self.port,
            gateway=self.gateway,
            forward_agent=self.forward_agent,
            connect_timeout=self.connect_timeout,
            connect_kwargs=self.connect_kwargs,
            inline_ssh_env=self.inline_ssh_env,
            timeout_execute=self.timeout_execute,
        )


WorkerConfig = Annotated[LocalWorker | RemoteWorker, Field(discriminator="type")]


class ExecutionConfig(BaseModel):
    exec_config_id: str | None = None
    modules: list[str] | None = None
    export: dict[str, str] | None = None
    pre_run: str | None
    post_run: str | None

    class Config:
        extra = Extra.forbid


class Project(BaseModel):
    name: str
    base_dir: str | None = None
    tmp_dir: str | None = None
    log_dir: str | None = None
    daemon_dir: str | None = None
    log_level: LogLevel = LogLevel.INFO
    runner: RunnerOptions = Field(default_factory=RunnerOptions)
    workers: dict[str, WorkerConfig] = Field(default_factory=dict)
    queue: dict = Field(default_factory=dict)
    exec_config: list[ExecutionConfig] = Field(default_factory=list)
    jobstore: dict = Field(default_factory=lambda: dict(DEFAULT_JOBSTORE))
    metadata: dict | None = None

    def get_exec_config_dict(self) -> dict[str, ExecutionConfig]:
        return {ec.exec_config_id: ec for ec in self.exec_config}

    def get_exec_config_ids(self) -> list[str]:
        return [ec.exec_config_id for ec in self.exec_config]

    def get_jobstore(self) -> JobStore | None:
        if not self.jobstore:
            return None
        elif self.jobstore.get("@class") == "JobStore":
            return JobStore.from_dict(self.jobstore)
        else:
            return JobStore.from_dict_spec(self.jobstore)

    def get_queue_store(self):
        return store_from_dict(self.queue)

    def get_launchpad(self) -> RemoteLaunchPad:
        return RemoteLaunchPad(self.get_queue_store())

    @validator("base_dir", always=True)
    def check_base_dir(cls, base_dir: str, values: dict) -> str:
        """
        Validator to set the default of base_dir based on the project name
        """
        if not base_dir:
            from jobflow_remote import SETTINGS

            return str(Path(SETTINGS.projects_folder, values["name"]))
        return base_dir

    @validator("tmp_dir", always=True)
    def check_tmp_dir(cls, tmp_dir: str, values: dict) -> str:
        """
        Validator to set the default of tmp_dir based on the base_dir
        """
        if not tmp_dir:
            return str(Path(values["base_dir"], "tmp"))
        return tmp_dir

    @validator("log_dir", always=True)
    def check_log_dir(cls, log_dir: str, values: dict) -> str:
        """
        Validator to set the default of log_dir based on the base_dir
        """
        if not log_dir:
            return str(Path(values["base_dir"], "log"))
        return log_dir

    @validator("daemon_dir", always=True)
    def check_daemon_dir(cls, daemon_dir: str, values: dict) -> str:
        """
        Validator to set the default of daemon_dir based on the base_dir
        """
        if not daemon_dir:
            return str(Path(values["base_dir"], "daemon"))
        return daemon_dir

    @validator("exec_config", always=True)
    def check_exec_config(
        cls, exec_config: list[ExecutionConfig], values: dict
    ) -> list[ExecutionConfig]:
        ecids: list[ExecutionConfig] = []
        for ec in exec_config:
            if ec.exec_config_id in ecids:
                raise ValueError(f"Repeated Host with id {ec.exec_config_id}")

        return exec_config

    @validator("jobstore", always=True)
    def check_jobstore(cls, jobstore: dict, values: dict) -> dict:
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

    @validator("queue", always=True)
    def check_queue(cls, queue: dict, values: dict) -> dict:
        if queue:
            try:
                store_from_dict(queue)
            except Exception as e:
                raise ValueError(
                    f"error while converting queue to a maggma store. Error: {traceback.format_exc()}"
                ) from e
        return queue

    class Config:
        extra = Extra.forbid


class ConfigError(Exception):
    pass
