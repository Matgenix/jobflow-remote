from __future__ import annotations

from typing import Callable

from jobflow import Flow, Job, JobStore
from qtoolkit.core.data_objects import QResources

from jobflow_remote.config import ConfigManager
from jobflow_remote.config.base import ExecutionConfig


def set_run_config(
    flow_or_job: Flow | Job,
    name_filter: str = None,
    function_filter: Callable = None,
    exec_config: str | ExecutionConfig | None = None,
    resources: dict | QResources | None = None,
):
    if not exec_config and not resources:
        return
    config: dict = {"manager_config": {}}
    if exec_config:
        config["manager_config"]["exec_config"] = exec_config
    if resources:
        config["manager_config"]["resources"] = resources

    flow_or_job.update_config(
        config=config, name_filter=name_filter, function_filter=function_filter
    )


def load_job_store(project: str | None = None) -> JobStore:
    """
    Load the JobStore for the current project.

    Parameters
    ----------
    project

    Returns
    -------

    """
    cm = ConfigManager()
    p = cm.get_project(project)
    job_store = p.get_jobstore()

    return job_store
