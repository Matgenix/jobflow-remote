from __future__ import annotations

import json
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

from fireworks import Firework, FWorker
from monty.os import makedirs_p
from monty.serialization import loadfn
from qtoolkit.core.data_objects import QState, SubmissionStatus

from jobflow_remote.config.base import (
    ConfigError,
    ExecutionConfig,
    Machine,
    Project,
    RunnerOptions,
)
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.fireworks.launcher import rapidfire_checkout
from jobflow_remote.fireworks.launchpad import (
    FW_UUID_PATH,
    REMOTE_DOC_PATH,
    RemoteLaunchPad,
    get_job_doc,
    get_remote_doc,
)
from jobflow_remote.fireworks.tasks import RemoteJobFiretask
from jobflow_remote.jobs.state import RemoteState
from jobflow_remote.remote.data import (
    get_job_path,
    get_remote_files,
    get_remote_store,
    get_remote_store_filenames,
)
from jobflow_remote.remote.host import BaseHost
from jobflow_remote.remote.queue import QueueManager, set_name_out
from jobflow_remote.utils.data import deep_merge_dict
from jobflow_remote.utils.db import MongoLock
from jobflow_remote.utils.log import initialize_runner_logger

logger = logging.getLogger(__name__)


JobFWData = namedtuple(
    "JobFWData", ["fw", "task", "job", "store", "machine", "host", "original_store"]
)


class Runner:
    def __init__(
        self,
        project_name: str | None = None,
        log_level: int | None = None,
        runner_id: str | None = None,
    ):
        self.stop_signal = False
        self.runner_id: str = runner_id or str(uuid.uuid4())
        self.config_manager: ConfigManager = ConfigManager()
        self.project_name = project_name
        self.project: Project = self.config_manager.get_project(project_name)
        self.rlpad: RemoteLaunchPad = self.project.get_launchpad()
        self.fworker: FWorker = FWorker()
        self.machines: dict[str, Machine] = self.project.get_machines_dict()
        self.hosts: dict[str, BaseHost] = self.project.get_hosts_dict()
        self.queue_managers: dict = {}
        log_level = log_level if log_level is not None else self.project.log_level
        initialize_runner_logger(
            log_folder=self.project.log_dir,
            level=log_level,
        )

    @property
    def runner_options(self) -> RunnerOptions:
        return self.project.runner

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal: {signum}")
        self.stop_signal = True

    def get_machine(self, machine_id: str) -> Machine:
        if machine_id not in self.machines:
            raise ConfigError(
                f"No machine {machine_id} is defined in project {self.project_name}"
            )
        return self.machines[machine_id]

    def get_queue_manager(self, machine_id: str) -> QueueManager:
        if machine_id not in self.queue_managers:
            machine = self.get_machine(machine_id)
            self.queue_managers[machine_id] = QueueManager(
                machine.get_scheduler_io(), self.hosts[machine.host_id]
            )
        return self.queue_managers[machine_id]

    def get_fw_data(self, fw_doc: dict) -> JobFWData:
        # remove the launches to be able to create the FW instance without
        # accessing the DB again
        fw_doc_no_launches = dict(fw_doc)
        fw_doc_no_launches["launches"] = []
        fw_doc_no_launches["archived_launches"] = []
        fw = Firework.from_dict(fw_doc_no_launches)
        task = fw.tasks[0]
        if len(fw.tasks) != 1 and not isinstance(task, RemoteJobFiretask):
            raise RuntimeError(f"jobflow-remote cannot handle task {task}")
        job = task.get("job")
        store = task.get("store")
        if store is None:
            store = self.project.get_jobstore()
            task["store"] = store
        machine = self.get_machine(task["machine"])
        host = self.hosts[machine.host_id]

        return JobFWData(fw, task, job, store, machine, host, task.get("store"))

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
                    updated = self.lock_and_update(states)
                    wait_advance_status = not updated
                    if not updated:
                        last_advance_status = time.time()

                time.sleep(1)
        finally:
            self.cleanup()

    def lock_and_update(
        self,
        states,
        # collection,
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
            f"{REMOTE_DOC_PATH}.state": {"$in": states},
            f"{REMOTE_DOC_PATH}.retry_time_limit": {"$not": {"$gt": datetime.utcnow()}},
        }
        if job_id is not None:
            db_filter[FW_UUID_PATH] = job_id
        if additional_filter:
            db_filter = deep_merge_dict(db_filter, additional_filter)

        collection = self.rlpad.fireworks
        with MongoLock(
            collection=collection,
            filter=db_filter,
            update=update,
            timeout=timeout,
            lock_id=self.runner_id,
            lock_subdoc=REMOTE_DOC_PATH,
            **kwargs,
        ) as lock:
            doc = lock.locked_document
            if not doc:
                return False
            remote_doc = get_remote_doc(doc)
            if not remote_doc:
                return False

            state = RemoteState(remote_doc["state"])

            function = states_methods[state]

            fail_now = False
            try:
                error, fail_now, set_output = function(doc)
            except ConfigError:
                error = traceback.format_exc()
                warnings.warn(error)
                fail_now = True
            except Exception:
                error = traceback.format_exc()
                warnings.warn(error)

            if not error:
                # the state.next.value is correct as SUBMITTED is not dealt with here.
                succeeded_update = {
                    "$set": {
                        f"{REMOTE_DOC_PATH}.state": state.next.value,
                        f"{REMOTE_DOC_PATH}.step_attempts": 0,
                        f"{REMOTE_DOC_PATH}.retry_time_limit": None,
                        f"{REMOTE_DOC_PATH}.error": None,
                    }
                }
                lock.update_on_release = deep_merge_dict(
                    succeeded_update, set_output or {}
                )
            else:
                step_attempts = remote_doc["step_attempts"]
                fail_now = (
                    fail_now or step_attempts >= self.runner_options.max_step_attempts
                )
                if fail_now:
                    lock.update_on_release = {
                        "$set": {
                            f"{REMOTE_DOC_PATH}.state": RemoteState.FAILED.value,
                            f"{REMOTE_DOC_PATH}.previous_state": state.value,
                            f"{REMOTE_DOC_PATH}.error": error,
                        }
                    }
                else:
                    step_attempts += 1
                    delta = self.runner_options.get_delta_retry(step_attempts)
                    retry_time_limit = datetime.utcnow() + timedelta(seconds=delta)
                    lock.update_on_release = {
                        "$set": {
                            f"{REMOTE_DOC_PATH}.step_attempts": step_attempts,
                            f"{REMOTE_DOC_PATH}.retry_time_limit": retry_time_limit,
                            f"{REMOTE_DOC_PATH}.error": error,
                        }
                    }

            if "$set" in lock.update_on_release:
                lock.update_on_release["$set"][
                    "updated_on"
                ] = datetime.utcnow().isoformat()
                self.ping_wf_doc(doc["fw_id"])

        return True

    def upload(self, doc):
        fw_id = doc["fw_id"]
        remote_doc = get_remote_doc(doc)
        logger.debug(f"upload fw_id: {fw_id}")
        fw_job_data = self.get_fw_data(doc)

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

        remote_path = get_job_path(job.uuid, fw_job_data.machine.work_dir)

        # Set the value of the original store for dynamical workflow. Usually it
        # will be None don't add the serializer, at this stage the default_orjson
        # serializer could undergo refactoring and this could break deserialization
        # of older FWs. It is set in the FireTask at runtime.
        fw = fw_job_data.fw
        remote_store = get_remote_store(
            store=store, launch_dir=remote_path, add_orjson_serializer=False
        )
        fw.tasks[0]["store"] = remote_store
        fw.tasks[0]["original_store"] = fw_job_data.original_store

        files = get_remote_files(fw, remote_doc["launch_id"])
        self.rlpad.lpad.change_launch_dir(remote_doc["launch_id"], remote_path)

        created = fw_job_data.host.mkdir(remote_path)
        if not created:
            err_msg = (
                f"Could not create remote directory {remote_path} for fw_id {fw_id}"
            )
            logger.error(err_msg)
            return err_msg, False, None

        for fname, fcontent in files.items():
            path_file = Path(remote_path, fname)
            fw_job_data.host.write_text_file(path_file, fcontent)

        set_output = {"$set": {f"{REMOTE_DOC_PATH}.run_dir": remote_path}}

        return None, False, set_output

    def submit(self, doc):
        logger.debug(f"submit fw_id: {doc['fw_id']}")
        remote_doc = get_remote_doc(doc)
        fw_job_data = self.get_fw_data(doc)

        remote_path = remote_doc["run_dir"]

        script_commands = ["rlaunch singleshot --offline"]

        machine = fw_job_data.machine
        queue_manager = self.get_queue_manager(machine.machine_id)
        resources = fw_job_data.task.get("resources") or machine.resources or {}
        set_name_out(resources, fw_job_data.job.name)
        exec_config = fw_job_data.task.get("exec_config")
        if isinstance(exec_config, str):
            exec_config = self.config_manager.load_exec_config(
                exec_config_id=exec_config, project_name=self.project_name
            )
        elif isinstance(exec_config, dict):
            exec_config = ExecutionConfig.parse_obj(exec_config)

        exec_config = exec_config or ExecutionConfig()

        pre_run = machine.pre_run or ""
        if exec_config.pre_run:
            pre_run += "\n" + exec_config.pre_run
        post_run = machine.post_run or ""
        if exec_config.post_run:
            post_run += "\n" + exec_config.post_run

        submit_result = queue_manager.submit(
            commands=script_commands,
            pre_run=pre_run,
            post_run=post_run,
            options=resources,
            export=exec_config.export,
            modules=exec_config.modules,
            work_dir=remote_path,
            create_submit_dir=False,
        )

        if submit_result.status == SubmissionStatus.FAILED:
            err_msg = f"submission failed. {repr(submit_result)}"
            return err_msg, False, None
        elif submit_result.status == SubmissionStatus.JOB_ID_UNKNOWN:
            err_msg = f"submission succeeded but ID not known. Job may be running but status cannot be checked. {repr(submit_result)}"
            return err_msg, True, None
        elif submit_result.status == SubmissionStatus.SUCCESSFUL:

            set_output = {
                "$set": {f"{REMOTE_DOC_PATH}.process_id": str(submit_result.job_id)}
            }

            return None, False, set_output

        raise RuntimeError(f"unhandled submission status {submit_result.status}")

    def download(self, doc):
        remote_doc = get_remote_doc(doc)
        logger.debug(f"download fw_id: {doc['fw_id']}")
        fw_job_data = self.get_fw_data(doc)
        job = fw_job_data.job

        remote_path = remote_doc["run_dir"]
        loca_base_dir = Path(self.project.tmp_dir, "download")
        local_path = get_job_path(job.uuid, loca_base_dir)

        makedirs_p(local_path)

        store = fw_job_data.store

        fnames = ["FW_offline.json"]
        fnames.extend(get_remote_store_filenames(store))

        for fname in fnames:
            # in principle fabric should work by just passing the destination folder,
            # but it fails
            remote_file_path = str(Path(remote_path, fname))
            try:
                fw_job_data.host.get(remote_file_path, str(Path(local_path, fname)))
            except FileNotFoundError:
                # if files are missing it should not retry
                err_msg = f"file {remote_file_path} for job {job.uuid} does not exist"
                logger.error(err_msg)
                return err_msg, True, None

        return None, False, None

    def complete_launch(self, doc):
        remote_doc = get_remote_doc(doc)
        logger.debug(f"complete launch fw_id: {doc['fw_id']}")
        fw_job_data = self.get_fw_data(doc)

        loca_base_dir = Path(self.project.tmp_dir, "download")
        local_path = get_job_path(fw_job_data.job.uuid, loca_base_dir)

        try:
            remote_data = loadfn(Path(local_path, "FW_offline.json"), cls=None)

            store = fw_job_data.store
            save = {
                k: "output" if v is True else v
                for k, v in fw_job_data.job._kwargs.items()
            }

            # TODO add ping data?
            remote_store = get_remote_store(store, local_path)
            remote_store.connect()
            fw_id, completed = self.rlpad.recover_remote(
                remote_status=remote_data,
                store=store,
                remote_store=remote_store,
                save=save,
                launch_id=remote_doc["launch_id"],
                terminated=True,
            )
        except json.JSONDecodeError:
            # if an empty file is copied this error can appear, do not retry
            err_msg = traceback.format_exc()
            return err_msg, True, None

        # remove local folder with downloaded files if successfully completed
        if completed and self.runner_options.delete_tmp_folder:
            shutil.rmtree(local_path, ignore_errors=True)

        if not completed:
            err_msg = "the parsed output does not contain the required information to complete the job"
            return err_msg, True, None

        return None, False, None

    def check_run_status(self):
        logger.debug("check_run_status")
        # check for jobs that could have changed state
        machines_ids_docs = defaultdict(dict)
        db_filter = {
            f"{REMOTE_DOC_PATH}.state": {
                "$in": [RemoteState.SUBMITTED.value, RemoteState.RUNNING.value]
            }
        }
        projection = [
            "fw_id",
            f"{REMOTE_DOC_PATH}.launch_id",
            FW_UUID_PATH,
            f"{REMOTE_DOC_PATH}.process_id",
            f"{REMOTE_DOC_PATH}.state",
            "spec._tasks.machine",
        ]
        for doc in self.rlpad.fireworks.find(db_filter, projection):
            machine_id = doc["spec"]["_tasks"][0]["machine"]
            remote_doc = get_remote_doc(doc)
            machines_ids_docs[machine_id][remote_doc["process_id"]] = (doc, remote_doc)

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

            for doc_id, (doc, remote_doc) in ids_docs.items():
                # TODO if failed should maybe be handled differently?
                qjob = qjobs_dict.get(doc_id)
                qstate = qjob.state if qjob else None
                collection = self.rlpad.fireworks
                if (
                    qstate == QState.RUNNING
                    and remote_doc["state"] == RemoteState.SUBMITTED.value
                ):
                    lock_filter = {
                        f"{REMOTE_DOC_PATH}.state": remote_doc["state"],
                        FW_UUID_PATH: get_job_doc(doc)["uuid"],
                    }
                    with MongoLock(
                        collection=collection,
                        filter=lock_filter,
                        lock_subdoc=REMOTE_DOC_PATH,
                    ) as lock:
                        if lock.locked_document:
                            lock.update_on_release = {
                                "$set": {
                                    f"{REMOTE_DOC_PATH}.state": RemoteState.RUNNING.value,
                                    f"{REMOTE_DOC_PATH}.queue_state": qstate.value,
                                    "updated_on": datetime.utcnow().isoformat(),
                                }
                            }
                            self.ping_wf_doc(doc["fw_id"])
                            logger.debug(
                                f"remote job with id {remote_doc['process_id']} is running"
                            )
                elif qstate in [None, QState.DONE, QState.FAILED]:
                    lock_filter = {
                        f"{REMOTE_DOC_PATH}.state": remote_doc["state"],
                        FW_UUID_PATH: get_job_doc(doc)["uuid"],
                    }
                    with MongoLock(
                        collection=collection,
                        filter=lock_filter,
                        lock_subdoc=REMOTE_DOC_PATH,
                    ) as lock:
                        if lock.locked_document:
                            lock.update_on_release = {
                                "$set": {
                                    f"{REMOTE_DOC_PATH}.state": RemoteState.TERMINATED.value,
                                    f"{REMOTE_DOC_PATH}.queue_state": qstate.value
                                    if qstate
                                    else None,
                                    "updated_on": datetime.utcnow().isoformat(),
                                }
                            }
                            self.ping_wf_doc(doc["fw_id"])
                            logger.debug(
                                f"terminated remote job with id {remote_doc['process_id']}"
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

    def ping_wf_doc(self, db_id: int):
        # in the WF document the date is a real Date
        self.rlpad.workflows.find_one_and_update(
            {"nodes": db_id}, {"$set": {"updated_on": datetime.utcnow()}}
        )
