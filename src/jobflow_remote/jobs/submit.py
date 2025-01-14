from __future__ import annotations

from typing import TYPE_CHECKING

from jobflow.core.flow import get_flow

from jobflow_remote.config.base import ConfigError, ExecutionConfig
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.remote.data import check_additional_stores

if TYPE_CHECKING:
    import jobflow
    from qtoolkit.core.data_objects import QResources


def submit_flow(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    worker: str | None = None,
    project: str | None = None,
    exec_config: str | ExecutionConfig | None = None,
    resources: dict | QResources | None = None,
    priority: int = 0,
    allow_external_references: bool = False,
) -> list[str]:
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
    exec_config
        the options to set before the execution of the job in the submission script.
        In addition to those defined in the Worker.
    resources
        Information passed to qtoolkit to require the resources for the submission
        to the queue.
    priority
        The priority assigned to the Jobs.
    allow_external_references
        If False all the references to other outputs should be from other Jobs
        of the Flow.

    Returns
    -------
    List of str
        The list of db_ids of the submitted Jobs.
    """
    config_manager = ConfigManager()

    proj_obj = config_manager.get_project(project)
    if worker is None:
        if not proj_obj.workers:
            raise ConfigError("No workers configured for this project.")
        worker = next(iter(proj_obj.workers))

    # try to load the worker and exec_config to check that the values are well defined
    config_manager.get_worker(worker_name=worker, project_name=project)
    if isinstance(exec_config, str):
        config_manager.get_exec_config(
            exec_config_name=exec_config, project_name=project
        )

    flow = get_flow(flow, allow_external_references=allow_external_references)

    # check that all the additional stores are properly defined
    jobstore = proj_obj.get_jobstore()
    for job, _ in flow.iterflow():
        missing_stores = check_additional_stores(job, jobstore)
        if missing_stores:
            raise ConfigError(
                f"Additional stores {missing_stores!r} are not configured for this project."
            )

    jc = proj_obj.get_job_controller()

    return jc.add_flow(
        flow=flow,
        worker=worker,
        exec_config=exec_config,
        resources=resources,
        priority=priority,
        allow_external_references=allow_external_references,
    )
