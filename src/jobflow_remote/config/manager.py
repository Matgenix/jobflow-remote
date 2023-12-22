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
    """
    A manager for the projects configuration files.

    Provides tool to parse project information from the selected projects folder as
    well as methods to update the properties of each project.
    """

    projects_ext = ["json", "yaml", "toml"]

    def __init__(
        self,
        exclude_unset: bool = False,
        exclude_none: bool = False,
        warn: bool = False,
    ):
        """

        Parameters
        ----------
        exclude_unset
            when dumping projects determine whether fields which were not explicitly
            set when creating the model should be excluded from the dictionary
        exclude_none
            when dumping projects determine whether fields which are equal to None
            should be excluded from the dictionary
        warn
            if True print warnings related to the parsing of the files in the
            projects folder
        """
        from jobflow_remote import SETTINGS

        self.exclude_unset = exclude_unset
        self.exclude_none = exclude_none
        self.warn = warn
        self.projects_folder = Path(SETTINGS.projects_folder)
        makedirs_p(self.projects_folder)
        self.projects_data = self.load_projects_data()

    @property
    def projects(self) -> dict[str, Project]:
        """
        Returns
        -------
        dict
            Dictionary with project name as key and Project as value.
        """
        return {name: pd.project for name, pd in self.projects_data.items()}

    def load_projects_data(self) -> dict[str, ProjectData]:
        """
        Load projects from the selected projects folder.

        Returns
        -------
        dict
            Dictionary with project name as key and ProjectData as value.
        """

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
                    if self.warn:
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
        """
        Determine the project name to be used based on the passed value
        and on the general settings.

        Parameters
        ----------
        project_name
            The name of the project or None to use the value from the settings
        Returns
        -------
        str
            The name of the selected project.
        """
        from jobflow_remote import SETTINGS

        project_name = project_name or SETTINGS.project
        if not project_name:
            if len(self.projects_data) == 1:
                project_name = next(iter(self.projects_data.keys()))
            else:
                raise ProjectUndefined("A project name should be defined")

        return project_name

    def get_project_data(self, project_name: str | None = None) -> ProjectData:
        """
        Get the ProjectData object based from the project name.

        Parameters
        ----------
        project_name
            The name of the project or None to use the value from the settings
        Returns
        -------
        ProjectData
            The selected ProjectData
        """
        project_name = self.select_project_name(project_name)

        if project_name not in self.projects_data:
            raise ConfigError(
                f"The selected project {project_name} does not exist "
                "or could not be parsed correctly"
            )

        return self.projects_data[project_name]

    def get_project(self, project_name: str | None = None) -> Project:
        """
        Get the Project object based from the project name.

        Parameters
        ----------
        project_name
            The name of the project or None to use the value from the settings
        Returns
        -------
        Project
            The selected Project
        """
        return self.get_project_data(project_name).project

    def dump_project(self, project_data: ProjectData):
        """
        Dump the project to filepath specified in the ProjectData.

        Parameters
        ----------
        project_data
            The project data to be dumped
        """
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
        """
        Create a new Project in the project folder by dumping the project to file.

        Parameters
        ----------
        project
            The data of the project to be created.
        ext
            The extension of the file to which the project will be dumped (yaml, json
             or toml)
        """
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
        """
        Remove a project from the projects folder.

        Parameters
        ----------
        project_name
            Name of the project to be removed.
        remove_folders
            Optionally remove the folders related to the project (e.g. tmp, log).
        """
        if project_name not in self.projects_data:
            return
        project_data = self.projects_data.pop(project_name)
        if remove_folders:
            shutil.rmtree(project_data.project.base_dir, ignore_errors=True)
        os.remove(project_data.filepath)

    def update_project(self, config: dict, project_name: str):
        """
        Update the project values.
        The passed dict with values will be recursively merged in the current project.

        Parameters
        ----------
        config
            Dictionary with the project values to be updated.
        project_name
            Name of the project to be updated
        """
        project_data = self.projects_data.pop(project_name)
        proj_dict = project_data.project.dict()
        new_project = Project.parse_obj(deep_merge_dict(proj_dict, config))
        project_data = ProjectData(project_data.filepath, new_project, project_data.ext)
        self.dump_project(project_data)
        self.projects_data[project_data.project.name] = project_data

    def project_names_from_files(self) -> list[str]:
        """
        Parses all the prasable files and only checks for the "name" attribute to
        return a list of potential project file names.

        Useful in case some projects cannot be properly parsed, but the full list
        needs to be returned.

        Returns
        -------
        list
            List of project names.
        """
        project_names = []
        for ext in self.projects_ext:
            for filepath in glob.glob(str(self.projects_folder / f"*.{ext}")):
                try:
                    if ext in ["json", "yaml"]:
                        d = loadfn(filepath)
                    else:
                        with open(filepath) as f:
                            d = tomlkit.parse(f.read())
                    if "name" in d:
                        project_names.append(d["name"])
                except Exception:
                    continue

        return project_names

    def set_worker(
        self,
        name: str,
        worker: WorkerBase,
        project_name: str | None = None,
        replace: bool = False,
    ):
        """
        Set a worker in the selected project.
        Can add a new worker or replace an existing one.

        Parameters
        ----------
        name
            Name of the worker to be added or replaced.
        worker
            Worker to be set.
        project_name
            Name of the project where the Worker is set, or None to use the one
            from the settings.
        replace
            Raise an exception if False and a Worker with the chosen name already
            exists.
        """
        project_data = self.get_project_data(project_name)
        if not replace and name in project_data.project.workers:
            raise ConfigError(f"Worker with name {name} is already defined")

        project_data.project.workers[name] = worker
        self.dump_project(project_data)

    def remove_worker(self, worker_name: str, project_name: str | None = None):
        """
        Remove a worker from the selected project.

        Parameters
        ----------
        worker_name
            Name of the worker to be removed
        project_name
            Name of the project from which the Worker should be removed, or None to
            use the one from the settings.
        """
        project_data = self.get_project_data(project_name)
        project_data.project.workers.pop(worker_name)
        self.dump_project(project_data)

    def get_worker(
        self, worker_name: str, project_name: str | None = None
    ) -> WorkerBase:
        """
        Return the worker object based on the name.

        Parameters
        ----------
        worker_name
            Name of the worker to retrieve.
        project_name
            Name of the project from which the Worker should be retrieved, or None to
            use the one from the settings.
        Returns
        -------
        WorkerBase
            The selected Worker.
        """
        project = self.get_project(project_name)
        if worker_name not in project.workers:
            raise ConfigError(f"Worker with name {worker_name} is not defined")
        return project.workers[worker_name]

    def set_queue_db(self, store: MongoStore, project_name: str | None = None):
        """
        Set the project specific store used for managing the queue.

        Parameters
        ----------
        store
            A maggma Store
        project_name
            Name of the project where the Store is set, or None to use the one
            from the settings.
        """
        project_data = self.get_project_data(project_name)
        project_data.project.queue = store.as_dict()

        self.dump_project(project_data)

    def set_jobstore(self, jobstore: JobStore, project_name: str | None = None):
        """
        Set the project specific store used for jobflow.

        Parameters
        ----------
        jobstore
            A maggma Store
        project_name
            Name of the project where the Store is set, or None to use the one
            from the settings.
        """
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
        """
        Set an ExecutionConfig in the selected project.
        Can add a new ExecutionConfig or replace an existing one.

        Parameters
        ----------
        exec_config_name
            Name of the ExecutionConfig to be added or replaced.
        exec_config
            The ExecutionConfig.
        project_name
            Name of the project where the ExecutionConfig is set, or None to use
             the one from the settings.
        replace
            Raise an exception if False and an ExecutionConfig with the chosen
            name already exists.
        """
        project_data = self.get_project_data(project_name)
        if not replace and exec_config_name in project_data.project.exec_config:
            raise ConfigError(f"Host with name {exec_config_name} is already defined")
        project_data.project.exec_config[exec_config_name] = exec_config
        self.dump_project(project_data)

    def remove_exec_config(
        self, exec_config_name: str, project_name: str | None = None
    ):
        """
        Remove an ExecutionConfig from the selected project.

        Parameters
        ----------
        exec_config_name
            Name of the ExecutionConfig to be removed
        project_name
            Name of the project from which the ExecutionConfig should be removed, or
            None to use the one from the settings.
        """
        project_data = self.get_project_data(project_name)
        project_data.project.exec_config.pop(exec_config_name, None)
        self.dump_project(project_data)

    def get_exec_config(
        self, exec_config_name: str, project_name: str | None = None
    ) -> ExecutionConfig:
        """
        Return the ExecutionConfig object based on the name.

        Parameters
        ----------
        exec_config_name
            Name of the ExecutionConfig.
        project_name
            Name of the project from which the ExecutionConfig should be retrieved,
            or None to use the one from the settings.
        Returns
        -------
        ExecutionConfig
            The selected ExecutionConfig
        """
        project = self.get_project(project_name)
        if exec_config_name not in project.exec_config:
            raise ConfigError(
                f"ExecutionConfig with id {exec_config_name} is not defined"
            )
        return project.exec_config[exec_config_name]
