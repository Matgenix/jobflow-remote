from __future__ import annotations

import jobflow
from qtoolkit.core.data_objects import QResources

from jobflow_remote.config.base import ConfigError, ExecutionConfig
from jobflow_remote.config.manager import ConfigManager


def submit_flow(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    worker: str | None = None,
    project: str | None = None,
    exec_config: str | ExecutionConfig | None = None,
    resources: dict | QResources | None = None,
    allow_external_references: bool = False,
) -> list[int]:
    """
    Submit a flow for calculation to the selected Worker.

    This will not start the calculation but just add to the database of the
    calculation to be executed.

    Parameters
    ----------
    flow
        A flow or job.
    worker
        The name of the Worker where the calculation will be submitted. If None, use the
        first configured worker for this project.
    project
        the name of the project to which the Flow should be submitted. If None the
        current project will be used.
    exec_config: str or ExecutionConfig
        the options to set before the execution of the job in the submission script.
        In addition to those defined in the Worker.
    resources: Dict or QResources
        information passed to qtoolkit to require the resources for the submission
        to the queue.
    allow_external_references
        If False all the references to other outputs should be from other Jobs
        of the Flow.

    Returns
    -------
    List of int
        The list of db_ids of the submitted Jobs.
    """
    config_manager = ConfigManager()

    proj_obj = config_manager.get_project(project)
    if worker is None:
        if not proj_obj.workers:
            raise ConfigError("No workers configured for this project.")
        worker = next(iter(proj_obj.workers.keys()))

    # try to load the worker and exec_config to check that the values are well defined
    config_manager.get_worker(worker_name=worker, project_name=project)
    if isinstance(exec_config, str):
        config_manager.get_exec_config(
            exec_config_name=exec_config, project_name=project
        )

    jc = proj_obj.get_job_controller()

    return jc.add_flow(
        flow=flow,
        worker=worker,
        exec_config=exec_config,
        resources=resources,
        allow_external_references=allow_external_references,
    )
