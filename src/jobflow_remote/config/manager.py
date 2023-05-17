from __future__ import annotations

import shutil
from pathlib import Path

from monty.os import makedirs_p
from monty.serialization import dumpfn, loadfn

from jobflow_remote import SETTINGS
from jobflow_remote.config.entities import ConfigError, Machine, Project, ProjectsData
from jobflow_remote.fireworks.launchpad import RemoteLaunchPad
from jobflow_remote.remote.host.base import BaseHost


class ConfigManager:
    projects_filename = "projects.json"
    machines_filename = "machines.json"
    jobflow_settings_filename = "jobflow.json"
    launchpad_filename = "launchpad.json"
    log_folder = "logs"

    def __init__(self):
        self.projects_folder = Path(SETTINGS.projects_folder)
        makedirs_p(self.projects_folder)
        self.projects_data = self._load_projects_data()

    @property
    def projects_config_filepath(self) -> Path:
        return self.projects_folder / self.projects_filename

    @property
    def projects(self) -> dict[str, Project]:
        return dict(self.projects_data.projects)

    @property
    def hosts(self) -> dict[str, BaseHost]:
        return dict(self.projects_data.hosts)

    def _base_get_project_path(
        self, subpath: str | Path, project: str | Project | None = None
    ):
        if isinstance(project, Project):
            project = project.name
        project = self.load_project(project)
        return Path(project.folder) / subpath

    def get_machines_config_filepath(
        self, project: str | Project | None = None
    ) -> Path:
        return self._base_get_project_path(self.machines_filename, project)

    def get_jobflow_settings_filepath(
        self, project: str | Project | None = None
    ) -> Path:
        return self._base_get_project_path(self.jobflow_settings_filename, project)

    def get_launchpad_filepath(self, project: str | Project | None = None) -> Path:
        return self._base_get_project_path(self.launchpad_filename, project)

    def get_logs_folder_path(self, project: str | Project | None = None) -> Path:
        return self._base_get_project_path(self.log_folder, project)

    def _load_projects_data(self) -> ProjectsData:
        filepath = self.projects_config_filepath
        if not Path(filepath).exists():
            pd = ProjectsData()
        else:
            pd = loadfn(filepath)

        return pd

    def load_project(self, project_name: str | None = None) -> Project:
        if not project_name:
            return self.load_current_project()

        pd = self.projects_data
        try:
            return pd.projects[project_name]
        except ValueError:
            raise ConfigError(
                f"No project with name {project_name} present in the configuration"
            )

    def load_project_from_id(self, project_id: str) -> Project:
        pd = self.projects_data
        for p in pd.projects.values():
            if p.project_id == project_id:
                return p

        raise ConfigError(
            f"No project with id {project_id} present in the configuration"
        )

    def load_default_project(self) -> Project:
        if not self.projects_data.default_project_name:
            raise ConfigError("default project has not been defined")

        try:
            return self.projects_data.projects[self.projects_data.default_project_name]
        except ValueError:
            raise ConfigError(
                f"Could not find the project {self.projects_data.default_project_name}"
            )

    def load_current_project(self) -> Project:
        project_name = (
            SETTINGS.current_project or self.projects_data.default_project_name
        )
        if not project_name:
            raise ConfigError(
                "current project and default project have not been defined"
            )

        try:
            return self.projects_data.projects[project_name]
        except ValueError:
            raise ConfigError(f"Could not find the project {project_name}")

    def dump_projects_data(self, projects_data: ProjectsData | None = None):
        projects_data = projects_data or self.projects_data
        makedirs_p(self.projects_folder)
        dumpfn(projects_data, self.projects_config_filepath, indent=2)
        self.projects_data = projects_data

    def create_project(self, project: Project):
        if project.name in self.projects_data.projects:
            raise ConfigError(f"Project with name {project.name} already exists")
        self.projects_data.projects[project.name] = project
        if not project.folder:
            project_folder = str(self.projects_folder / project.name)
            project.folder = project_folder
        if not project.folder_tmp:
            tmp_folder = str(Path(project.folder) / "tmp_files")
            project.folder_tmp = tmp_folder

        makedirs_p(project.folder)
        makedirs_p(project.folder_tmp)
        makedirs_p(self.get_logs_folder_path(project.name))
        self.dump_projects_data(self.projects_data)

    def remove_project(self, project_name: str):
        if project_name not in self.projects_data.projects:
            return
        project = self.projects_data.projects.pop(project_name)
        shutil.rmtree(project.folder, ignore_errors=True)
        shutil.rmtree(project.folder_tmp, ignore_errors=True)

    def set_default_project(self, project: str | Project):
        if isinstance(project, Project):
            project = project.name

        if project not in self.projects_data.projects:
            raise ConfigError(
                f"Cannot set current project as no project named {project} has been defined"
            )

        self.projects_data.default_project_name = project

        self.dump_projects_data()

    def load_machines_data(self, project_name: str | None = None) -> dict[str, Machine]:
        filepath = self.get_machines_config_filepath(project_name)
        if not filepath.exists():
            return {}

        return loadfn(filepath)

    def dump_machines_data(
        self, machines_data: dict, project_name: str | None = None
    ) -> None:
        filepath = self.get_machines_config_filepath(project_name)
        dumpfn(machines_data, filepath, indent=2)

    def dump_jobflow_settings_data(
        self, settings: dict, project_name: str | None = None
    ) -> None:
        filepath = self.get_jobflow_settings_filepath(project_name)
        dumpfn(settings, filepath, indent=2)

    def dump_launchpad_data(
        self, config: dict, project_name: str | None = None
    ) -> None:
        filepath = self.get_launchpad_filepath(project_name)
        dumpfn(config, filepath, indent=2)

    def set_machine(
        self, machine: Machine, project_name: str | None = None, replace: bool = False
    ):
        machines_data = self.load_machines_data(project_name)
        if not replace and machine.machine_id in machines_data:
            raise ConfigError(
                f"Machine with id {machine.machine_id} is already defined"
            )
        machines_data[machine.machine_id] = machine
        if machine.host_id not in self.hosts:
            raise ValueError(f"host {machine.host_id} is not defined")
        self.dump_machines_data(machines_data)

    def remove_machine(self, machine_id: str, project_name: str | None = None):
        machines_data = self.load_machines_data(project_name)
        machines_data.pop(machine_id)
        self.dump_machines_data(machines_data)

    def load_machine(self, machine_id: str, project_name: str | None = None) -> Machine:
        machines_data = self.load_machines_data(project_name)
        if machine_id not in machines_data:
            raise ConfigError(f"Machine with id {machine_id} is not defined")
        return machines_data[machine_id]

    def set_host(self, host_id: str, host: BaseHost, replace: bool = False):
        if not replace and host_id in self.projects_data.hosts:
            raise ConfigError(f"Host with id {host_id} is already defined")
        self.projects_data.hosts[host_id] = host
        self.dump_projects_data()

    def remove_host(self, host_id: str):
        for project_name in self.projects.keys():
            for machine_id, machine in self.load_machines_data(project_name).items():
                if machine.host_id == host_id:
                    raise ValueError(
                        f"Host is used in the {machine_id} machine. Will not be removed."
                    )
        self.projects_data.hosts.pop(host_id)
        self.dump_projects_data()

    def load_host(self, host_id: str) -> BaseHost:
        if host_id not in self.projects_data.hosts:
            raise ConfigError(f"Host with id {host_id} is not defined")

    def set_jobflow_settings(
        self, settings: dict, project_name: str | None = None, update: bool = False
    ):
        project = self.load_project(project_name)
        filepath = self.get_jobflow_settings_filepath(project)
        settings["CONFIG_FILE"] = str(filepath)
        if update and filepath.exists():
            old = loadfn(filepath)
            old.update(settings)
            settings = old

        self.dump_jobflow_settings_data(settings, project.name)

    def load_jobflow_settings(self, project_name: str):
        filepath = self.get_jobflow_settings_filepath(project_name)
        if not filepath.exists():
            return {}
        else:
            return loadfn(filepath)

    def activate_jobflow_settings(self, project_name: str | None = None):
        project_settings = self.load_jobflow_settings(project_name)
        from jobflow import SETTINGS

        for k, v in project_settings.items():
            setattr(SETTINGS, k, v)

    def activate_project(self, project_name: str):
        self.activate_jobflow_settings(project_name)

    def set_launchpad_config(
        self, config: dict, project_name: str | None = None, update: bool = False
    ):
        project = self.load_project(project_name)
        filepath = self.get_launchpad_filepath(project)
        if update and filepath.exists():
            old = loadfn(filepath)
            old.update(config)
            config = old

        self.dump_launchpad_data(config, project.name)

    def load_launchpad_config(self, project_name: str | None = None):
        filepath = self.get_launchpad_filepath(project_name)
        if not filepath.exists():
            return {}
        else:
            return loadfn(filepath, cls=None)

    def load_launchpad(self, project_name: str | None = None):
        # from fireworks import LaunchPad
        config = self.load_launchpad_config(project_name)
        return RemoteLaunchPad(**config)
