from __future__ import annotations

import jobflow
from qtoolkit.core.data_objects import QResources

from jobflow_remote.config.base import ExecutionConfig
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.convert import flow_to_workflow


def submit_flow(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    machine: str,
    store: str | jobflow.JobStore | None = None,
    project: str | None = None,
    exec_config: str | ExecutionConfig | None = None,
    resources: dict | QResources | None = None,
):
    """
    Submit a flow for calculation to the selected Machine.

    This will not start the calculation but just add to the database of the
    calculation to be executed.

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
    project
        the name of the project to which the Flow should be submitted. If None the
        current project will be used.
    exec_config: str or ExecutionConfig
        the options to set before the execution of the job in the submission script.
        In addition to those defined in the Machine.
    resources: Dict or QResources
        information passed to qtoolkit to require the resources for the submission
        to the queue.
    """
    config_manager = ConfigManager()

    proj_obj = config_manager.get_project(project)

    # try to load the machine and exec_config to check that the values are well defined
    config_manager.load_machine(machine_id=machine, project_name=project)
    if isinstance(exec_config, str):
        config_manager.load_exec_config(
            exec_config_id=exec_config, project_name=project
        )

    wf = flow_to_workflow(
        flow, machine=machine, store=store, exec_config=exec_config, resources=resources
    )

    rlpad = proj_obj.get_launchpad()
    rlpad.add_wf(wf)
