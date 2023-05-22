from __future__ import annotations

import glob
import os

from fireworks import FiretaskBase, FWAction, explicit_serialize
from monty.shutil import decompress_file


@explicit_serialize
class RemoteJobFiretask(FiretaskBase):
    """
    A firetask that will run any job, tailored for the execution on a remote resource.

    Other Parameters
    ----------------
    job : Dict
        A serialized job.
    store : JobStore
        A job store. Alternatively, if set to None, :obj:`JobflowSettings.JOB_STORE`
        will be used. Note, this will use the configuration defined on the local
        machine, even if the Task is executed on a remote one. An actual store
        should be set before the Task is executed remotely.
    machine: Str
        The id of the Machine where the calculation will be submitted
    exec_config: ExecutionConfig
        the options to set before the execution of the job in the submission script.
        In addition to those defined in the Machine.
    resources: Dict or QResources
        information passed to qtoolkit to require the resources for the submission
        to the queue.
    """

    required_params = ["job", "store", "machine"]
    optional_params = ["exec_config", "resources"]

    def run_task(self, fw_spec):
        """Run the job and handle any dynamic firework submissions."""
        from jobflow import initialize_logger
        from jobflow.core.job import Job
        from jobflow.core.store import JobStore
        from maggma.stores.mongolike import JSONStore

        job: Job = self.get("job")
        original_store = self.get("store")

        docs_store = JSONStore("remote_job_data.json", read_only=False)
        additional_stores = {}
        for k in original_store.additional_stores.keys():
            additional_stores[k] = JSONStore(
                f"additional_store_{k}.json", read_only=False
            )
        store = JobStore(
            docs_store=docs_store,
            additional_stores=additional_stores,
            save=original_store.save,
            load=original_store.load,
        )
        store.connect()

        if hasattr(self, "fw_id"):
            job.metadata.update({"fw_id": self.fw_id})

        initialize_logger()

        response = job.run(store=store)

        # some jobs may have compressed the FW files while being executed,
        # try to decompress them if that is the case.
        self.decompress_files()

        detours = None
        additions = None
        # in case of dynamic Flow set the same parameters as the current Job
        kwargs_dynamic = {
            "machine": self.get("machine"),
            "store": original_store,
            "exports": self.get("exports"),
            "qtk_options": self.get("qtk_options"),
        }
        from jobflow_remote.fireworks.convert import flow_to_workflow

        if response.replace is not None:
            # create a workflow from the new additions; be sure to use original store
            detours = [flow_to_workflow(flow=response.replace, **kwargs_dynamic)]

        if response.addition is not None:
            additions = [flow_to_workflow(flow=response.addition, **kwargs_dynamic)]

        if response.detour is not None:
            detour_wf = flow_to_workflow(flow=response.detour, **kwargs_dynamic)
            if detours is not None:
                detours.append(detour_wf)
            else:
                detours = [detour_wf]

        fwa = FWAction(
            stored_data=response.stored_data,
            detours=detours,
            additions=additions,
            defuse_workflow=response.stop_jobflow,
            defuse_children=response.stop_children,
        )
        return fwa

    def decompress_files(self):
        file_names = ["FW.json", "FW_offline.json"]

        for fn in file_names:
            if os.path.isfile(fn):
                continue
            for f in glob.glob(fn + ".*"):
                decompress_file(f)
