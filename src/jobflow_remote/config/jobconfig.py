from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from jobflow_remote.config import ConfigManager

if TYPE_CHECKING:
    from jobflow import Flow, Job, JobStore
    from qtoolkit.core.data_objects import QResources

    from jobflow_remote.config.base import ExecutionConfig


def set_run_config(
    flow_or_job: Flow | Job,
    name_filter: str = None,
    function_filter: Callable = None,
    exec_config: str | ExecutionConfig | None = None,
    resources: dict | QResources | None = None,
    priority: int | None = None,
    worker: str | None = None,
    dynamic: bool = True,
) -> Flow | Job:
    """
    Modify in place a Flow or a Job by setting the properties in the
    "manager_config" entry in the JobConfig associated to each Job
    matching the filter.

    The values left as None will not be set and their value may be
    filled by those set in the submit_flow.
    If you want/need to leave resources or exec_config empty pass
    an empty QResources (or an empty dict) or an empty ExecutionConfig.

    Uses the Flow/Job update_config() method,
    so follows the same conventions, also setting the options in
    the config_updates of the Job, to allow setting the same properties
    also in dynamically generated Jobs, if the dynamic option is True.

    Note that calling this function will override all previously set
    values for matching Jobs, even if not specified here.

    Parameters
    ----------
    flow_or_job
        A Flow or a Job to be modified
    name_filter
        A filter for the job name. Only jobs with a matching name will be updated.
        Includes partial matches, e.g. "ad" will match a job with the name "adder".
    function_filter
        A filter for the job function. Only jobs with a matching function will be
        updated.
    exec_config
        The execution configuration to be added to the selected Jobs.
    resources
        The resources to be set for the selected Jobs.
    priority
        The priority of the Job.
    worker
        The worker where the selected Jobs will be executed.
    dynamic
        The updates will be propagated to Jobs/Flows dynamically generated at
        runtime.

    Returns
    -------
    Flow or Job
        The modified object.
    """
    if not exec_config and not resources and not worker:
        return flow_or_job
    config: dict = {"manager_config": {}}
    if exec_config is not None:
        config["manager_config"]["exec_config"] = exec_config
    if resources is not None:
        config["manager_config"]["resources"] = resources
    if worker is not None:
        config["manager_config"]["worker"] = worker
    if priority is not None:
        config["manager_config"]["priority"] = priority

    flow_or_job.update_config(
        config=config,
        name_filter=name_filter,
        function_filter=function_filter,
        dynamic=dynamic,
    )

    return flow_or_job


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
    return p.get_jobstore()
