from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import Annotated, Literal

from jobflow import JobStore
from pydantic import BaseModel, Extra, Field, validator
from qtoolkit.io import BaseSchedulerIO, scheduler_mapping

from jobflow_remote.fireworks.launchpad import RemoteLaunchPad
from jobflow_remote.remote.host import BaseHost, LocalHost, RemoteHost

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


class Machine(BaseModel):

    machine_id: str
    scheduler_type: str
    host_id: str
    work_dir: str
    resources: dict | None = None
    pre_run: str | None = None
    post_run: str | None = None
    queue_exec_timeout: int | None = 30

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


class LaunchPadConfig(BaseModel):
    host: str | None = "localhost"
    port: int | None = None
    name: str | None = None
    username: str | None = None
    password: str | None = None
    logdir: str | None = None
    strm_lvl: str = "CRITICAL"
    user_indices: list[str] | None = None
    wf_user_indices: list[str] | None = None
    authsource: str | None = None
    uri_mode: bool = False
    mongoclient_kwargs: dict | None = None

    class Config:
        extra = Extra.forbid


class RemoteHostConfig(BaseModel):
    host_type: Literal["remote"] = "remote"
    host_id: str
    host: str
    user: str = None
    port: int = None
    gateway: str = None
    forward_agent: bool = None
    connect_timeout: int = None
    connect_kwargs: dict = None
    inline_ssh_env: bool = None
    timeout_execute: int = 60

    class Config:
        extra = Extra.forbid

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


class LocalHostConfig(BaseModel):
    host_type: Literal["local"] = "local"
    host_id: str
    timeout_execute: int = 60

    class Config:
        extra = Extra.forbid

    def get_host(self) -> BaseHost:
        return LocalHost(timeout_execute=self.timeout_execute)


HostConfig = Annotated[
    LocalHostConfig | RemoteHostConfig, Field(discriminator="host_type")
]


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
    log_level: int = logging.INFO
    runner: RunnerOptions = Field(default_factory=RunnerOptions)
    hosts: list[HostConfig] = Field(default_factory=list)
    machines: list[Machine] = Field(default_factory=list)
    run_db: LaunchPadConfig = Field(default_factory=LaunchPadConfig)
    exec_config: list[ExecutionConfig] = Field(default_factory=list)
    jobstore: dict = Field(default_factory=lambda: dict(DEFAULT_JOBSTORE))

    def get_machines_dict(self) -> dict[str, Machine]:
        return {m.machine_id: m for m in self.machines}

    def get_machines_ids(self) -> list[str]:
        return [m.machine_id for m in self.machines]

    def get_hosts_config_dict(self) -> dict[str, LocalHostConfig | RemoteHostConfig]:
        return {h.host_id: h for h in self.hosts}

    def get_hosts_ids(self) -> list[str]:
        return [h.host_id for h in self.hosts]

    def get_hosts_dict(self) -> dict[str, BaseHost]:
        return {h.host_id: h.get_host() for h in self.hosts}

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

    def get_launchpad(self) -> RemoteLaunchPad:
        return RemoteLaunchPad(**self.run_db.dict())

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

    @validator("machines", always=True)
    def check_machines(cls, machines: list[Machine], values: dict) -> list[Machine]:
        if "hosts" not in values:
            raise ValueError("hosts should be defined to define a Machine")

        hosts_ids = [h.host_id for h in values["hosts"]]
        mids: list[Machine] = []
        for m in machines:
            if m.machine_id in mids:
                raise ValueError(f"Repeated Machine with id {m.machine_id}")
            if m.host_id not in hosts_ids:
                raise ValueError(
                    f"Host with id {m.host_id} defined in Machine {m.machine_id} is not defined"
                )
        return machines

    @validator("hosts", always=True)
    def check_hosts(cls, hosts: list[HostConfig], values: dict) -> list[HostConfig]:
        hids: list[HostConfig] = []
        for h in hosts:
            if h.host_id in hids:
                raise ValueError(f"Repeated Host with id {h.host_id}")

        return hosts

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

    class Config:
        extra = Extra.forbid


class ConfigError(Exception):
    pass
