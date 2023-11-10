from __future__ import annotations

import json
import logging
import shutil
import signal
import time
import traceback
import uuid
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import Path

from fireworks import FWorker
from monty.os import makedirs_p
from qtoolkit.core.data_objects import QState, SubmissionStatus

from jobflow_remote import JobController
from jobflow_remote.config.base import (
    ConfigError,
    ExecutionConfig,
    LogLevel,
    Project,
    RunnerOptions,
    WorkerBase,
)
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.jobs.data import IN_FILENAME, OUT_FILENAME, JobDoc, RemoteError
from jobflow_remote.jobs.state import JobState
from jobflow_remote.remote.data import (
    get_job_path,
    get_remote_in_file,
    get_remote_store,
    get_remote_store_filenames,
)
from jobflow_remote.remote.host import BaseHost
from jobflow_remote.remote.queue import ERR_FNAME, OUT_FNAME, QueueManager, set_name_out
from jobflow_remote.utils.log import initialize_runner_logger

logger = logging.getLogger(__name__)


JobFWData = namedtuple(
    "JobFWData",
    ["fw", "task", "job", "store", "worker_name", "worker", "host", "original_store"],
)


class Runner:
    def __init__(
        self,
        project_name: str | None = None,
        log_level: LogLevel | None = None,
        runner_id: str | None = None,
    ):
        self.stop_signal = False
        self.runner_id: str = runner_id or str(uuid.uuid4())
        self.config_manager: ConfigManager = ConfigManager()
        self.project_name = project_name
        self.project: Project = self.config_manager.get_project(project_name)
        self.job_controller: JobController = JobController.from_project(self.project)
        self.fworker: FWorker = FWorker()
        self.workers: dict[str, WorkerBase] = self.project.workers
        # Build the dictionary of hosts. The reference is the worker name.
        # If two hosts match, use the same instance
        self.hosts: dict[str, BaseHost] = {}
        for wname, w in self.workers.items():
            new_host = w.get_host()
            for host in self.hosts.values():
                if new_host == host:
                    self.hosts[wname] = host
                    break
            else:
                self.hosts[wname] = new_host
        self.queue_managers: dict = {}
        log_level = log_level if log_level is not None else self.project.log_level
        initialize_runner_logger(
            log_folder=self.project.log_dir,
            level=log_level.to_logging(),
        )
        # TODO it could be better to create a pool of stores that are connected
        # How to deal with cases where the connection gets closed?
        # how to deal with file based stores?
        self.jobstore = self.project.get_jobstore()

    @property
    def runner_options(self) -> RunnerOptions:
        return self.project.runner

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal: {signum}")
        self.stop_signal = True

    def get_worker(self, worker_name: str) -> WorkerBase:
        if worker_name not in self.workers:
            raise ConfigError(
                f"No worker {worker_name} is defined in project {self.project_name}"
            )
        return self.workers[worker_name]

    def get_host(self, worker_name: str):
        host = self.hosts[worker_name]
        if not host.is_connected:
            host.connect()
        return host

    def get_queue_manager(self, worker_name: str) -> QueueManager:
        if worker_name not in self.queue_managers:
            worker = self.get_worker(worker_name)
            self.queue_managers[worker_name] = QueueManager(
                worker.get_scheduler_io(), self.get_host(worker_name)
            )
        return self.queue_managers[worker_name]

    def get_store(self, job_doc: JobDoc):
        return job_doc.store or self.jobstore

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
                    updated = self.advance_state()
                    wait_advance_status = not updated
                    if not updated:
                        last_advance_status = time.time()

                time.sleep(1)
        finally:
            self.cleanup()

    def advance_state(self):
        states = [
            JobState.CHECKED_OUT.value,
            JobState.UPLOADED.value,
            JobState.TERMINATED.value,
            JobState.DOWNLOADED.value,
        ]

        states_methods = {
            JobState.CHECKED_OUT: self.upload,
            JobState.UPLOADED: self.submit,
            JobState.TERMINATED: self.download,
            JobState.DOWNLOADED: self.complete_job,
        }

        with self.job_controller.lock_job_for_update(
            states=states,
            max_step_attempts=self.runner_options.max_step_attempts,
            delta_retry=self.runner_options.delta_retry,
        ) as lock:
            doc = lock.locked_document
            if not doc:
                return False

            state = JobState(doc["state"])

            states_methods[state](lock)
            return True

    def upload(self, lock):
        doc = lock.locked_document
        db_id = doc["db_id"]
        logger.debug(f"upload db_id: {db_id}")

        job_doc = JobDoc(**doc)
        job = job_doc.job

        worker = self.get_worker(job_doc.worker)
        host = self.get_host(job_doc.worker)
        store = self.get_store(job_doc)
        # TODO would it be better/feasible to keep a pool of the required
        # Stores already connected, to avoid opening and closing them?
        store.connect()
        try:
            job.resolve_args(store=store, inplace=True)
        finally:
            try:
                store.close()
            except Exception:
                logging.error(f"error while closing the store {store}", exc_info=True)

        remote_path = get_job_path(job.uuid, worker.work_dir)

        # Set the value of the original store for dynamical workflow. Usually it
        # will be None don't add the serializer, at this stage the default_orjson
        # serializer could undergo refactoring and this could break deserialization
        # of older FWs. It is set in the FireTask at runtime.
        remote_store = get_remote_store(
            store=store, launch_dir=remote_path, add_orjson_serializer=False
        )

        created = host.mkdir(remote_path)
        if not created:
            err_msg = (
                f"Could not create remote directory {remote_path} for db_id {db_id}"
            )
            logger.error(err_msg)
            raise RemoteError(err_msg, no_retry=False)

        serialized_input = get_remote_in_file(job, remote_store, job_doc.store)

        path_file = Path(remote_path, IN_FILENAME)
        host.put(serialized_input, str(path_file))

        set_output = {
            "$set": {"run_dir": remote_path, "state": JobState.UPLOADED.value}
        }
        lock.update_on_release = set_output

    def submit(self, lock):
        doc = lock.locked_document
        logger.debug(f"submit db_id: {doc['db_id']}")

        job_doc = JobDoc(**doc)
        job = job_doc.job

        worker = self.get_worker(job_doc.worker)

        remote_path = Path(job_doc.run_dir)

        script_commands = [f"jf execution run {remote_path}"]

        queue_manager = self.get_queue_manager(job_doc.worker)
        resources = job_doc.resources or worker.resources or {}
        qout_fpath = remote_path / OUT_FNAME
        qerr_fpath = remote_path / ERR_FNAME
        set_name_out(resources, job.name, out_fpath=qout_fpath, err_fpath=qerr_fpath)

        exec_config = job_doc.exec_config
        if isinstance(exec_config, str):
            exec_config = self.config_manager.get_exec_config(
                exec_config_name=exec_config, project_name=self.project_name
            )
        elif isinstance(exec_config, dict):
            exec_config = ExecutionConfig.parse_obj(job_doc.exec_config)

        # define an empty default if it is not set
        exec_config = exec_config or ExecutionConfig()

        pre_run = worker.pre_run or ""
        if exec_config.pre_run:
            pre_run += "\n" + exec_config.pre_run
        post_run = worker.post_run or ""
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
            raise RemoteError(err_msg, False)
        elif submit_result.status == SubmissionStatus.JOB_ID_UNKNOWN:
            err_msg = f"submission succeeded but ID not known. Job may be running but status cannot be checked. {repr(submit_result)}"
            raise RemoteError(err_msg, True)
        elif submit_result.status == SubmissionStatus.SUCCESSFUL:
            lock.update_on_release = {
                "$set": {
                    "remote.process_id": str(submit_result.job_id),
                    "state": JobState.SUBMITTED.value,
                }
            }
        else:
            raise RemoteError(
                f"unhandled submission status {submit_result.status}", True
            )

    def download(self, lock):
        doc = lock.locked_document
        logger.debug(f"download db_id: {doc['db_id']}")

        job_doc = JobDoc(**doc)
        job = job_doc.job

        # If the worker is local do not copy the files in the temporary folder
        # TODO it could be possible to go directly from
        #  SUBMITTED/RUNNING to DOWNLOADED instead
        worker = self.get_worker(job_doc.worker)
        if worker.type != "local":
            host = self.get_host(job_doc.worker)
            store = self.get_store(job_doc)

            remote_path = job_doc.run_dir
            local_base_dir = Path(self.project.tmp_dir, "download")
            local_path = get_job_path(job.uuid, local_base_dir)

            makedirs_p(local_path)

            fnames = [OUT_FILENAME]
            fnames.extend(get_remote_store_filenames(store))

            for fname in fnames:
                # in principle fabric should work by just passing the
                # destination folder, but it fails
                remote_file_path = str(Path(remote_path, fname))
                try:
                    host.get(remote_file_path, str(Path(local_path, fname)))
                except FileNotFoundError:
                    # if files are missing it should not retry
                    err_msg = (
                        f"file {remote_file_path} for job {job.uuid} does not exist"
                    )
                    logger.error(err_msg)
                    raise RemoteError(err_msg, True)

        lock.update_on_release = {"$set": {"state": JobState.DOWNLOADED.value}}

    def complete_job(self, lock):
        doc = lock.locked_document
        logger.debug(f"complete job db_id: {doc['db_id']}")

        # if the worker is local the files were not copied to the temporary
        # folder, but the files could be directly updated
        worker = self.get_worker(doc["worker"])
        worker_is_local = worker.type == "local"
        if worker_is_local:
            local_path = doc["run_dir"]
        else:
            local_base_dir = Path(self.project.tmp_dir, "download")
            local_path = get_job_path(doc["uuid"], local_base_dir)

        try:
            job_doc = JobDoc(**doc)
            store = self.get_store(job_doc)
            completed = self.job_controller.complete_job(job_doc, local_path, store)

        except json.JSONDecodeError:
            # if an empty file is copied this error can appear, do not retry
            err_msg = traceback.format_exc()
            raise RemoteError(err_msg, True)

        # remove local folder with downloaded files if successfully completed
        if completed and self.runner_options.delete_tmp_folder and not worker_is_local:
            shutil.rmtree(local_path, ignore_errors=True)

        if not completed:
            err_msg = "the parsed output does not contain the required information to complete the job"
            raise RemoteError(err_msg, True)

    def check_run_status(self):
        logger.debug("check_run_status")
        # check for jobs that could have changed state
        workers_ids_docs = defaultdict(dict)
        db_filter = {
            "state": {"$in": [JobState.SUBMITTED.value, JobState.RUNNING.value]},
            "lock_id": None,
            "remote.retry_time_limit": {"$not": {"$gt": datetime.utcnow()}},
        }
        projection = [
            "db_id",
            "uuid",
            "index",
            "remote",
            "worker",
            "state",
        ]
        for doc in self.job_controller.get_jobs(db_filter, projection):
            worker_name = doc["worker"]
            remote_doc = doc["remote"]
            workers_ids_docs[worker_name][remote_doc["process_id"]] = doc

        for worker_name, ids_docs in workers_ids_docs.items():
            error = None
            if not ids_docs:
                continue

            qjobs_dict = {}
            try:
                ids_list = list(ids_docs.keys())
                queue = self.get_queue_manager(worker_name)
                qjobs = queue.get_jobs_list(ids_list)
                qjobs_dict = {qjob.job_id: qjob for qjob in qjobs}
            except Exception:
                logger.warning(
                    f"error trying to get jobs list for worker: {worker_name}",
                    exc_info=True,
                )
                error = traceback.format_exc()

            for doc_id, doc in ids_docs.items():
                # TODO if failed should maybe be handled differently?
                remote_doc = doc["remote"]
                qjob = qjobs_dict.get(doc_id)
                qstate = qjob.state if qjob else None
                next_state = None
                start_time = None
                if (
                    qstate == QState.RUNNING
                    and doc["state"] == JobState.SUBMITTED.value
                ):
                    next_state = JobState.RUNNING
                    start_time = datetime.utcnow()
                    logger.debug(
                        f"remote job with id {remote_doc['process_id']} is running"
                    )
                elif qstate in [None, QState.DONE, QState.FAILED]:
                    next_state = JobState.TERMINATED
                    logger.debug(
                        f"terminated remote job with id {remote_doc['process_id']}"
                    )
                elif not error and remote_doc["step_attempts"] > 0:
                    # reset the step attempts if succeeding in case there was
                    # an error earlier. Setting the state to the same as the
                    # current triggers the update that cleans the state
                    next_state = JobState(remote_doc["state"])

                # the document needs to be updated only in case of error or if a
                # next state has been set.
                # Only update if the state did not change in the meanwhile
                if next_state or error:
                    lock_filter = {"uuid": doc["uuid"], "index": doc["index"]}
                    with self.job_controller.lock_job_for_update(
                        states=doc["state"],
                        additional_filter=lock_filter,
                        max_step_attempts=self.runner_options.max_step_attempts,
                        delta_retry=self.runner_options.delta_retry,
                    ) as lock:
                        if lock.locked_document:
                            if error:
                                raise RemoteError(error, False)
                            set_output = {
                                "$set": {
                                    "remote.queue_state": qstate.value
                                    if qstate
                                    else None,
                                    "state": next_state.value,
                                }
                            }
                            if start_time:
                                set_output["$set"]["start_time"] = start_time
                            lock.update_on_release = set_output

    def checkout(self):
        logger.debug("checkout jobs")
        n_checked_out = 0
        while True:
            reserved = self.job_controller.checkout_job()
            if not reserved:
                break

            n_checked_out += 1

        logger.debug(f"checked out {n_checked_out} jobs")

    def cleanup(self):
        for worker_name, host in self.hosts.items():
            try:
                host.close()
            except Exception:
                logging.exception(
                    f"error while closing connection to worker {worker_name}"
                )

        try:
            self.jobstore.close()
        except Exception:
            logging.exception("error while closing connection to jobstore")

        self.job_controller.close()
