from __future__ import annotations

import jobflow
from qtoolkit.core.data_objects import QResources

from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.convert import flow_to_workflow


def submit_flow(
    flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
    machine: str,
    store: jobflow.JobStore | None = None,
    project: str | None = None,
    exports: dict | None = None,
    qtk_options: dict | QResources | None = None,
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
    exports
        pairs of key-values that will be exported in the submission script
    qtk_options
        information passed to qtoolkit to require the resources for the submission
        to the queue.
    """
    wf = flow_to_workflow(
        flow, machine=machine, store=store, exports=exports, qtk_options=qtk_options
    )

    config_manager = ConfigManager()

    # try to load the machine to check that the project and the machine are well defined
    machine = config_manager.load_machine(machine_id=machine, project_name=project)

    rlpad = config_manager.load_launchpad(project)
    rlpad.add_wf(wf)
