from __future__ import annotations

import glob
import logging
import os
import shutil
import traceback
from collections import namedtuple
from pathlib import Path

import tomlkit
from jobflow import JobStore
from maggma.stores import MongoStore
from monty.json import jsanitize
from monty.os import makedirs_p
from monty.serialization import dumpfn, loadfn

from jobflow_remote.config.base import (
    ConfigError,
    ExecutionConfig,
    Project,
    ProjectUndefined,
    WorkerBase,
)
from jobflow_remote.utils.data import deep_merge_dict

logger = logging.getLogger(__name__)

ProjectData = namedtuple("ProjectData", ["filepath", "project", "ext"])

WorkerData = namedtuple("WorkerData", ["name", "worker"])


class ConfigManager:
    projects_ext = ["json", "yaml", "toml"]

    def __init__(self, exclude_unset=False, exclude_none=False):
        from jobflow_remote import SETTINGS

        self.exclude_unset = exclude_unset
        self.exclude_none = exclude_none
        self.projects_folder = Path(SETTINGS.projects_folder)
        makedirs_p(self.projects_folder)
        self.projects_data = self.load_projects_data()

    @property
    def projects(self):
        return {name: pd.project for name, pd in self.projects_data.items()}

    def load_projects_data(self) -> dict[str, ProjectData]:
        projects_data: dict[str, ProjectData] = {}
        for ext in self.projects_ext:
            for filepath in glob.glob(str(self.projects_folder / f"*.{ext}")):
                try:
                    if ext in ["json", "yaml"]:
                        d = loadfn(filepath)
                    else:
                        with open(filepath) as f:
                            d = tomlkit.parse(f.read())
                    project = Project.parse_obj(d)
                except Exception:
                    logger.warning(
                        f"File {filepath} could not be parsed as a Project. Error: {traceback.format_exc()}"
                    )
                    continue
                if project.name in projects_data:
                    msg = f"Two projects with the same name '{project.name}' have been defined: {filepath}, {projects_data[project.name].filepath}"
                    raise ConfigError(msg)
                projects_data[project.name] = ProjectData(filepath, project, ext)

        return projects_data

    def select_project_name(self, project_name: str | None = None) -> str:
        from jobflow_remote import SETTINGS

        project_name = project_name or SETTINGS.project
        if not project_name:
            if len(self.projects_data) == 1:
                project_name = next(iter(self.projects_data.keys()))
            else:
                raise ProjectUndefined("A project name should be defined")

        return project_name

    def get_project_data(self, project_name: str | None = None) -> ProjectData:

        project_name = self.select_project_name(project_name)

        if project_name not in self.projects_data:
            raise ConfigError(f"The selected project {project_name} does not exist")

        return self.projects_data[project_name]

    def get_project(self, project_name: str | None = None) -> Project:
        return self.get_project_data(project_name).project

    def dump_project(self, project_data: ProjectData):
        exclude_none = True if project_data.ext == "toml" else self.exclude_none
        d = jsanitize(
            project_data.project.dict(
                exclude_none=exclude_none, exclude_unset=self.exclude_unset
            ),
            enum_values=True,
        )
        if project_data.ext in ["json", "yaml"]:
            dumpfn(d, project_data.filepath)
        elif project_data.ext == "toml":
            with open(project_data.filepath, "w") as f:
                tomlkit.dump(d, f)

    def create_project(self, project: Project, ext="yaml"):
        if project.name in self.projects_data:
            raise ConfigError(f"Project with name {project.name} already exists")

        makedirs_p(project.base_dir)
        makedirs_p(project.tmp_dir)
        makedirs_p(project.log_dir)
        filepath = self.projects_folder / f"{project.name}.{ext}"
        if filepath.exists():
            raise ConfigError(
                f"Project with name {project.name} does not exist, but file {str(filepath)} does"
            )
        project_data = ProjectData(filepath, project, ext)
        self.dump_project(project_data)
        self.projects_data[project.name] = project_data

    def remove_project(self, project_name: str, remove_folders: bool = True):
        if project_name not in self.projects_data:
            return
        project_data = self.projects_data.pop(project_name)
        if remove_folders:
            shutil.rmtree(project_data.project.base_dir, ignore_errors=True)
        os.remove(project_data.filepath)

    def update_project(self, config: dict, project_name: str):
        project_data = self.projects_data.pop(project_name)
        proj_dict = project_data.project.dict()
        new_project = Project.parse_obj(deep_merge_dict(proj_dict, config))
        project_data = ProjectData(project_data.filepath, new_project, project_data.ext)
        self.dump_project(project_data)
        self.projects_data[project_data.project.name] = project_data

    def set_worker(
        self,
        name: str,
        worker: WorkerBase,
        project_name: str | None = None,
        replace: bool = False,
    ):
        project_data = self.get_project_data(project_name)
        if not replace and name in project_data.project.workers:
            raise ConfigError(f"Worker with name {name} is already defined")

        project_data.project.workers[name] = worker
        self.dump_project(project_data)

    def remove_worker(self, worker_name: str, project_name: str | None = None):
        project_data = self.get_project_data(project_name)
        project_data.project.workers.pop(worker_name)
        self.dump_project(project_data)

    def load_worker(
        self, worker_name: str, project_name: str | None = None
    ) -> WorkerBase:
        project = self.get_project(project_name)
        if worker_name not in project.workers:
            raise ConfigError(f"Worker with name {worker_name} is not defined")
        return project.workers[worker_name]

    def set_queue_db(self, store: MongoStore, project_name: str | None = None):
        project_data = self.get_project_data(project_name)
        project_data.project.queue = store.as_dict()

        self.dump_project(project_data)

    def set_jobstore(self, jobstore: JobStore, project_name: str | None = None):
        project_data = self.get_project_data(project_name)
        project_data.project.jobstore = jobstore.as_dict()
        self.dump_project(project_data)

    def set_exec_config(
        self,
        exec_config_name: str,
        exec_config: ExecutionConfig,
        project_name: str | None = None,
        replace: bool = False,
    ):
        project_data = self.get_project_data(project_name)
        if not replace and exec_config_name in project_data.project.exec_config:
            raise ConfigError(f"Host with name {exec_config_name} is already defined")
        project_data.project.exec_config[exec_config_name] = exec_config
        self.dump_project(project_data)

    def remove_exec_config(
        self, exec_config_name: str, project_name: str | None = None
    ):
        project_data = self.get_project_data(project_name)
        project_data.project.exec_config.pop(exec_config_name, None)
        self.dump_project(project_data)

    def load_exec_config(
        self, exec_config_name: str, project_name: str | None = None
    ) -> ExecutionConfig:
        project = self.get_project(project_name)
        if exec_config_name not in project.exec_config:
            raise ConfigError(
                f"ExecutionConfig with id {exec_config_name} is not defined"
            )
        return project.exec_config[exec_config_name]
