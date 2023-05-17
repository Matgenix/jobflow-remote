from __future__ import annotations

import logging

# from pydantic.dataclasses import dataclass
from dataclasses import dataclass, field
from uuid import uuid4

from monty.json import MSONable
from qtoolkit.core.data_objects import QResources
from qtoolkit.io import BaseSchedulerIO

from jobflow_remote.remote.host import BaseHost


@dataclass
class ProjectOptions(MSONable):
    max_step_attempts: int = 3
    delta_retry: tuple[int, ...] = (30, 300, 1200)

    def get_delta_retry(self, step_attempts: int):
        ind = min(step_attempts, len(self.delta_retry)) - 1
        return self.delta_retry[ind]


@dataclass
class RunnerOptions(MSONable):
    delay_checkout: int = 30
    delay_check_run_status: int = 30
    delay_advance_status: int = 30
    lock_timeout: int | None = 7200
    delete_tmp_folder: bool = True


@dataclass
class Project(MSONable):

    project_id: str
    name: str
    folder: str | None = None
    folder_tmp: str | None = None
    log_level: int = logging.INFO
    options: ProjectOptions = field(default_factory=ProjectOptions)
    runner_options: RunnerOptions = field(default_factory=RunnerOptions)

    @classmethod
    def from_uuid_id(cls, **kwargs):
        project_id = str(uuid4())
        return cls(project_id=project_id, **kwargs)


@dataclass
class ProjectsData(MSONable):

    projects: dict[str, Project] = field(default_factory=dict)
    default_project_name: str = None
    hosts: dict[str, BaseHost] = field(default_factory=dict)

    @property
    def default_project(self):
        return self.projects[self.default_project_name]


@dataclass
class Machine(MSONable):

    machine_id: str
    scheduler_io: BaseSchedulerIO
    host_id: str
    work_dir: str
    default_qtk_options: dict | QResources | None = None
    pre_run: str | None = None
    post_run: str | None = None
    queue_exec_timeout: int | None = 30


@dataclass
class LaunchPadConfig(MSONable):
    host: str | None = None
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


class ConfigError(Exception):
    pass
