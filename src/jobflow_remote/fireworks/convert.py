from __future__ import annotations

import typing

from fireworks import Firework, Workflow
from qtoolkit.core.data_objects import QResources

from jobflow_remote.config.base import ExecutionConfig
from jobflow_remote.fireworks.tasks import RemoteJobFiretask

if typing.TYPE_CHECKING:
    from typing import Sequence

    import jobflow

__all__ = ["flow_to_workflow", "job_to_firework"]


def flow_to_workflow(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    machine: str,
    store: jobflow.JobStore | None = None,
    exec_config: str | ExecutionConfig = None,
    resources: dict | QResources | None = None,
    metadata: dict | None = None,
    **kwargs,
) -> Workflow:
    """
    Convert a :obj:`Flow` or a :obj:`Job` to a FireWorks :obj:`Workflow` object.

    Each firework spec is updated with the contents of the
    :obj:`Job.config.manager_config` dictionary. Accordingly, a :obj:`.JobConfig` object
    can be used to configure FireWork options such as metadata and the fireworker.

    Parameters
    ----------
    flow
        A flow or job.
    machine
        The id of the Machine where the calculation will be submitted
    store
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this could be different on the computer that submits the
        workflow and the computer which runs the workflow. The value of ``JOB_STORE`` on
        the computer that runs the workflow will be used.
    exec_config: ExecutionConfig
        the options to set before the execution of the job in the submission script.
        In addition to those defined in the Machine.
    resources: Dict or QResources
        information passed to qtoolkit to require the resources for the submission
        to the queue.
    metadata: Dict
        metadata passed to the workflow. The flow uuid will be added with the key
        "flow_id".
    **kwargs
        Keyword arguments passed to Workflow init method.

    Returns
    -------
    Workflow
        The job or flow as a workflow.
    """
    from fireworks.core.firework import Firework, Workflow
    from jobflow.core.flow import get_flow

    parent_mapping: dict[str, Firework] = {}
    fireworks = []

    flow = get_flow(flow)

    for job, parents in flow.iterflow():
        fw = job_to_firework(
            job,
            machine=machine,
            store=store,
            parents=parents,
            parent_mapping=parent_mapping,
            exec_config=exec_config,
            resources=resources,
        )
        fireworks.append(fw)

    metadata = metadata or {}
    metadata["flow_id"] = flow.uuid

    return Workflow(fireworks, name=flow.name, metadata=metadata, **kwargs)


def job_to_firework(
    job: jobflow.Job,
    machine: str,
    store: jobflow.JobStore | None = None,
    parents: Sequence[str] | None = None,
    parent_mapping: dict[str, Firework] | None = None,
    exec_config: str | ExecutionConfig = None,
    resources: dict | QResources | None = None,
    **kwargs,
) -> Firework:
    """
    Convert a :obj:`Job` to a :obj:`.Firework`.

    The firework spec is updated with the contents of the
    :obj:`Job.config.manager_config` dictionary. Accordingly, a :obj:`.JobConfig` object
    can be used to configure FireWork options such as metadata and the fireworker.

    Parameters
    ----------
    job
        A job.
    store
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this could be different on the computer that submits the
        workflow and the computer which runs the workflow. The value of ``JOB_STORE`` on
        the computer that runs the workflow will be used.
    parents
        The parent uuids of the job.
    parent_mapping
        A dictionary mapping job uuids to Firework objects, as ``{uuid: Firework}``.
    **kwargs
        Keyword arguments passed to the Firework constructor.

    Returns
    -------
    Firework
        A firework that will run the job.
    """
    from fireworks.core.firework import Firework
    from jobflow.core.reference import OnMissing

    if (parents is None) is not (parent_mapping is None):
        raise ValueError("Both or neither of parents and parent_mapping must be set.")

    if isinstance(exec_config, ExecutionConfig):
        exec_config = exec_config.dict()

    manager_config = dict(job.config.manager_config)
    resources_from_manager = manager_config.pop("resources", None)
    exec_config_manager = manager_config.pop("exec_config", None)
    resources = resources_from_manager or resources
    exec_config = exec_config_manager or exec_config

    if isinstance(exec_config, ExecutionConfig):
        exec_config = exec_config.dict()

    task = RemoteJobFiretask(
        job=job,
        store=store,
        machine=machine,
        resources=resources,
        exec_config=exec_config,
    )

    job_parents = None
    if parents is not None and parent_mapping is not None:
        job_parents = (
            [parent_mapping[parent] for parent in parents] if parents else None
        )

    spec = {"_add_launchpad_and_fw_id": True}  # this allows the job to know the fw_id
    if job.config.on_missing_references != OnMissing.ERROR:
        spec["_allow_fizzled_parents"] = True
    spec.update(manager_config)
    spec.update(job.metadata)  # add metadata to spec

    fw = Firework([task], spec=spec, name=job.name, parents=job_parents, **kwargs)

    if parent_mapping is not None:
        parent_mapping[job.uuid] = fw

    return fw
