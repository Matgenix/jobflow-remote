from __future__ import annotations

import logging
import shutil
import signal
import time
import traceback
import uuid
import warnings
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from pathlib import Path

from fireworks import FWorker
from jobflow import SETTINGS
from monty.os import makedirs_p
from monty.serialization import loadfn
from qtoolkit.core.data_objects import QState, SubmissionStatus

from jobflow_remote.config.entities import (
    ConfigError,
    Machine,
    Project,
    ProjectOptions,
    ProjectsData,
    RunnerOptions,
)
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.launcher import rapidfire_checkout
from jobflow_remote.fireworks.launchpad import RemoteLaunchPad
from jobflow_remote.fireworks.tasks import RemoteJobFiretask
from jobflow_remote.remote.data import get_job_path, get_remote_files, get_remote_store
from jobflow_remote.remote.host import BaseHost
from jobflow_remote.remote.queue import QueueManager
from jobflow_remote.run.state import RemoteState
from jobflow_remote.utils.data import deep_merge_dict
from jobflow_remote.utils.db import MongoLock
from jobflow_remote.utils.log import initialize_runner_logger

logger = logging.getLogger(__name__)

# lpad = LaunchPad.auto_load()
# fworker = FWorker()

# c = RemoteConfig(host="slurmtest", root_dir="/data")
# rh = RemoteHost(c)
# queue = QueueManager(SlurmIO(get_job_executable="scontrol"), host=rh)
#
# launch_base_dir = "/data/run_jobflow"


JobFWData = namedtuple("JobFWData", ["fw", "task", "job", "store", "machine", "host"])


class Runner:
    def __init__(self, project_name: str | None = None, log_level: int | None = None):
        self.stop_signal = False
        self.runner_id: str = str(uuid.uuid4())
        self.config_manager: ConfigManager = ConfigManager()
        self.project_name = project_name
        self.project: Project = self.config_manager.load_project(project_name)
        self.rlpad: RemoteLaunchPad = self.config_manager.load_launchpad(project_name)
        self.fworker: FWorker = FWorker()
        self.machines: dict[str, Machine] = {}
        self.queue_managers: dict = {}
        log_level = log_level if log_level is not None else self.project.log_level
        initialize_runner_logger(
            log_folder=self.config_manager.get_logs_folder_path(project_name),
            level=log_level,
        )

    @property
    def projects_data(self) -> ProjectsData:
        return self.config_manager.projects_data

    @property
    def hosts(self) -> dict[str, BaseHost]:
        return self.projects_data.hosts

    @property
    def runner_options(self) -> RunnerOptions:
        return self.project.runner_options

    @property
    def project_options(self) -> ProjectOptions:
        return self.project.options

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal: {signum}")
        self.stop_signal = True

    def get_machine(self, machine_id: str) -> Machine:
        if machine_id not in self.machines:
            self.machines[machine_id] = self.config_manager.load_machine(
                machine_id, project_name=self.project_name
            )
        return self.machines[machine_id]

    def get_queue_manager(self, machine_id: str) -> QueueManager:
        if machine_id not in self.queue_managers:
            machine = self.get_machine(machine_id)
            self.queue_managers[machine_id] = QueueManager(
                machine.scheduler_io, self.hosts[machine.host_id]
            )
        return self.queue_managers[machine_id]

    def get_fw_data(self, fw_id: int) -> JobFWData:
        fw = self.rlpad.lpad.get_fw_by_id(fw_id)
        task = fw.tasks[0]
        if len(fw.tasks) != 1 and not isinstance(task, RemoteJobFiretask):
            raise RuntimeError(f"jobflow-remote cannot handle task {task}")
        job = task.get("job")
        store = task.get("store")
        if store is None:
            store = SETTINGS.JOB_STORE
            task["store"] = store
        machine = self.get_machine(task["machine"])
        host = self.hosts[machine.host_id]

        return JobFWData(fw, task, job, store, machine, host)

    def run(self):
        signal.signal(signal.SIGTERM, self.handle_signal)
        last_checkout_time = 0
        last_check_run_status_time = 0
        wait_advance_status = False
        last_advance_status = 0

        try:
            while True:
                if self.stop_signal:
                    logger.info("stopping due to sigterm")
                    break
                now = time.time()
                if last_checkout_time + self.runner_options.delay_checkout < now:
                    self.checkout()
                    last_checkout_time = time.time()
                elif (
                    last_check_run_status_time
                    + self.runner_options.delay_check_run_status
                    < now
                ):
                    self.check_run_status()
                    last_check_run_status_time = time.time()
                elif (
                    not wait_advance_status
                    or last_advance_status + self.runner_options.delay_advance_status
                    < now
                ):
                    states = [
                        RemoteState.CHECKED_OUT.value,
                        RemoteState.UPLOADED.value,
                        RemoteState.TERMINATED.value,
                        RemoteState.DOWNLOADED.value,
                    ]
                    collection = self.rlpad.remote_runs
                    updated = self.lock_and_update(states, collection)
                    wait_advance_status = not updated
                    if not updated:
                        last_advance_status = time.time()

                time.sleep(1)
        finally:
            self.cleanup()

    def lock_and_update(
        self,
        states,
        collection,
        job_id=None,
        additional_filter=None,
        update=None,
        timeout=None,
        **kwargs,
    ):
        if not isinstance(states, (list, tuple)):
            states = tuple(states)

        states_methods = {
            RemoteState.CHECKED_OUT: self.upload,
            RemoteState.UPLOADED: self.submit,
            RemoteState.TERMINATED: self.download,
            RemoteState.DOWNLOADED: self.complete_launch,
        }

        db_filter = {
            "state": {"$in": states},
            "retry_time_limit": {"$not": {"$gt": datetime.utcnow()}},
        }
        if job_id is not None:
            db_filter["job_id"] = job_id
        if additional_filter:
            db_filter = deep_merge_dict(db_filter, additional_filter)

        with MongoLock(
            collection=collection,
            filter=db_filter,
            update=update,
            timeout=timeout,
            lock_id=self.runner_id,
            **kwargs,
        ) as lock:
            doc = lock.locked_document
            if not doc:
                return False
            error = None

            state = RemoteState(doc["state"])

            function = states_methods[state]

            fail_now = False
            try:
                succeeded, fail_now, set_output = function(doc)
            except ConfigError:
                error = traceback.format_exc()
                warnings.warn(error)
                succeeded = False
                fail_now = True
            except Exception:
                error = traceback.format_exc()
                warnings.warn(error)
                succeeded = False

            if succeeded:
                # new_state = states_evolution[state]
                succeeded_update = {
                    "$set": {
                        "state": state.next.value,
                        "step_attempts": 0,
                        "retry_time_limit": None,
                        "error": None,
                    }
                }
                lock.update_on_release = deep_merge_dict(
                    succeeded_update, set_output or {}
                )
            else:
                step_attempts = doc["step_attempts"]
                fail_now = (
                    fail_now or step_attempts >= self.project_options.max_step_attempts
                )
                if fail_now:
                    lock.update_on_release = {
                        "$set": {
                            "state": RemoteState.FAILED.value,
                            "failed_state": state,
                            "error": error,
                        }
                    }
                else:
                    step_attempts += 1
                    delta = self.project_options.get_delta_retry(step_attempts)
                    retry_time_limit = datetime.utcnow() + timedelta(seconds=delta)
                    lock.update_on_release = {
                        "$set": {
                            "step_attempts": step_attempts,
                            "retry_time_limit": retry_time_limit,
                            "error": error,
                        }
                    }

        return True

    def upload(self, doc):
        fw_id = doc["fw_id"]
        logger.debug(f"upload fw_id: {fw_id}")
        fw_job_data = self.get_fw_data(fw_id)

        job = fw_job_data.job
        store = fw_job_data.store
        store.connect()
        try:
            job.resolve_args(store=store, inplace=True)
        finally:
            try:
                store.close()
            except Exception:
                logging.error(f"error while closing the store {store}", exc_info=True)

        files = get_remote_files(fw_job_data.fw, doc["launch_id"])
        remote_path = get_job_path(job.uuid, fw_job_data.machine.work_dir)

        created = fw_job_data.host.mkdir(remote_path)
        if not created:
            logger.error(
                f"Could not create remote directory {remote_path} for fw_id {fw_id}"
            )
            return False, False, None

        for fname, fcontent in files.items():
            path_file = Path(remote_path, fname)
            fw_job_data.host.write_text_file(path_file, fcontent)

        return True, False, None

    def submit(self, doc):
        fw_id = doc["fw_id"]
        logger.debug(f"submit fw_id: {doc['fw_id']}")
        fw_job_data = self.get_fw_data(fw_id)
        job = fw_job_data.job

        get_job_path(
            doc["job_id"],
        )
        remote_path = get_job_path(job.uuid, fw_job_data.machine.work_dir)

        fw_job_data.machine.pre_run
        fw_job_data.machine.post_run

        script_commands = ["rlaunch singleshot --offline"]

        machine = fw_job_data.machine
        queue_manager = self.get_queue_manager(machine.machine_id)
        qtk_options = fw_job_data.task.get("qtk_options") or machine.default_qtk_options
        exports = fw_job_data.task.get("exports")
        submit_result = queue_manager.submit(
            commands=script_commands,
            pre_run=machine.pre_run,
            post_run=machine.post_run,
            options=qtk_options,
            exports=exports,
            work_dir=remote_path,
            create_submit_dir=False,
        )

        if submit_result.status == SubmissionStatus.FAILED:
            return False, False, None
        elif submit_result.status == SubmissionStatus.JOB_ID_UNKNOWN:
            raise RuntimeError("job id unknown")
        elif submit_result.status == SubmissionStatus.SUCCESSFUL:

            set_output = {"$set": {"process_id": str(submit_result.job_id)}}

            return True, False, set_output

        raise RuntimeError(f"unhandled submission status {submit_result.status}")

    def download(self, doc):
        fw_id = doc["fw_id"]
        logger.debug(f"download fw_id: {doc['fw_id']}")
        fw_job_data = self.get_fw_data(fw_id)
        job = fw_job_data.job

        remote_path = get_job_path(job.uuid, fw_job_data.machine.work_dir)
        loca_base_dir = Path(self.project.folder_tmp, "download")
        local_path = get_job_path(job.uuid, loca_base_dir)

        makedirs_p(local_path)

        store = fw_job_data.store

        # TODO check if the file exists
        fnames = ["FW_offline.json", "remote_job_data.json"]
        for k in store.additional_stores.keys():
            fnames.append(f"additional_store_{k}.json")

        for fname in fnames:
            # in principle fabric should work by just passing the destination folder,
            # but it fails
            remote_file_path = str(Path(remote_path, fname))
            try:
                fw_job_data.host.get(remote_file_path, str(Path(local_path, fname)))
            except FileNotFoundError:
                # if files are missing it should not retry
                logger.error(
                    f"file {remote_file_path} for job {job.uuid} does not exist"
                )
                return False, True, None

        return True, False, None

    def complete_launch(self, doc):
        fw_id = doc["fw_id"]
        logger.debug(f"complete launch fw_id: {doc['fw_id']}")
        fw_job_data = self.get_fw_data(fw_id)

        loca_base_dir = Path(self.project.folder_tmp, "download")
        local_path = get_job_path(fw_job_data.job.uuid, loca_base_dir)

        remote_data = loadfn(Path(local_path, "FW_offline.json"), cls=None)

        store = fw_job_data.store
        save = {
            k: "output" if v is True else v for k, v in fw_job_data.job._kwargs.items()
        }

        # TODO add ping data?
        remote_store = get_remote_store(store, local_path)
        fw_id, completed = self.rlpad.recover_remote(
            remote_status=remote_data,
            store=store,
            remote_store=remote_store,
            save=save,
            launch_id=doc["launch_id"],
            terminated=True,
        )

        # remove local folder with downloaded files if successfully completed
        if completed and self.runner_options.delete_tmp_folder:
            shutil.rmtree(local_path, ignore_errors=True)

        return completed, False, None

    def check_run_status(self):
        logger.debug("check_run_status")
        # check for jobs that could have changed state
        machines_ids_docs = defaultdict(dict)
        db_filter = {"state": {"$in": [RemoteState.SUBMITTED.value]}}
        projection = [
            "fw_id",
            "launch_id",
            "job_id",
            "process_id",
            "state",
            "machine_id",
        ]
        for doc in self.rlpad.remote_runs.find(db_filter, projection):
            machines_ids_docs[doc["machine_id"]][doc["process_id"]] = doc

        for machine_id, ids_docs in machines_ids_docs.items():

            if not ids_docs:
                continue
            try:
                ids_list = list(ids_docs.keys())
                queue = self.get_queue_manager(machine_id)
                qjobs = queue.get_jobs_list(ids_list)
            except Exception:
                logger.warning(
                    f"error trying to get jobs list for machine: {machine_id}",
                    exc_info=True,
                )
                continue

            qjobs_dict = {qjob.job_id: qjob for qjob in qjobs}

            for doc_id, doc in ids_docs.items():
                # TODO if failed should maybe be handled differently?
                qjob = qjobs_dict.get(doc_id)
                qstate = qjob.state if qjob else None
                collection = self.rlpad.remote_runs
                if qstate in [None, QState.DONE, QState.FAILED]:
                    lock_filter = {"state": doc["state"], "job_id": doc["job_id"]}
                    with MongoLock(collection=collection, filter=lock_filter) as lock:
                        if lock.locked_document:
                            lock.update_on_release = {
                                "$set": {
                                    "state": RemoteState.TERMINATED.value,
                                    "queue_state": qstate,
                                }
                            }
                            logger.debug(
                                f"terminated remote job with id {doc['process_id']}"
                            )

    def checkout(self):
        logger.debug("checkout rapidfire")
        n = rapidfire_checkout(self.rlpad, self.fworker)
        logger.debug(f"checked out {n} jobs")

    def cleanup(self):
        for host_id, host in self.hosts.items():
            try:
                host.close()
            except Exception:
                logging.exception(f"error while closing host {host_id}")
