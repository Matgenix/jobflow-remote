from __future__ import annotations

import glob
import os

from fireworks import FiretaskBase, FWAction, explicit_serialize
from jobflow import JobStore
from monty.shutil import decompress_file

from jobflow_remote.remote.data import (
    default_orjson_serializer,
    get_remote_store_filenames,
)


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
    original_store: JobStore
        The original JobStore. Used to set the value to following Jobs in case of
        a dynamical Flow.
    """

    required_params = ["job", "store", "machine"]
    optional_params = ["exec_config", "resources", "original_store"]

    def run_task(self, fw_spec):
        """Run the job and handle any dynamic firework submissions."""
        from jobflow import initialize_logger
        from jobflow.core.job import Job

        job: Job = self.get("job")
        store = self.get("store")

        # needs to be set here again since it does not get properly serialized.
        # it is possible to serialize the default function before serializing, but
        # avoided that to avoid that any refactoring of the default_orjson_serializer
        # breaks the deserialization of old Fireworks
        store.docs_store.serialization_default = default_orjson_serializer
        for additional_store in store.additional_stores.values():
            additional_store.serialization_default = default_orjson_serializer

        store.connect()

        if hasattr(self, "fw_id"):
            job.metadata.update({"db_id": self.fw_id})

        initialize_logger()

        try:
            response = job.run(store=store)
        finally:
            # some jobs may have compressed the FW files while being executed,
            # try to decompress them if that is the case.
            self.decompress_files(store)

        detours = None
        additions = None
        # in case of dynamic Flow set the same parameters as the current Job
        kwargs_dynamic = {
            "machine": self.get("machine"),
            "store": self.get("original_store"),
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

    def decompress_files(self, store: JobStore):
        file_names = ["FW.json", "FW_offline.json"]
        file_names.extend(get_remote_store_filenames(store))

        for fn in file_names:
            # If the file is already present do not decompress it, even if
            # a compressed version is present.
            if os.path.isfile(fn):
                continue
            for f in glob.glob(fn + ".*"):
                decompress_file(f)
