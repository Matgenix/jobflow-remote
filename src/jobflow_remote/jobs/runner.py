"""The Runner orchestrating the Jobs execution"""

from __future__ import annotations

import json
import logging
import shutil
import signal
import time
import traceback
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from jobflow.utils import suuid
from monty.json import MontyDecoder
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
from jobflow_remote.jobs.batch import RemoteBatchManager
from jobflow_remote.jobs.data import IN_FILENAME, OUT_FILENAME, RemoteError
from jobflow_remote.jobs.state import JobState
from jobflow_remote.remote.data import (
    get_job_path,
    get_remote_in_file,
    get_remote_store,
    get_remote_store_filenames,
    resolve_job_dict_args,
)
from jobflow_remote.remote.host import BaseHost
from jobflow_remote.remote.queue import ERR_FNAME, OUT_FNAME, QueueManager, set_name_out
from jobflow_remote.utils.log import initialize_runner_logger
from jobflow_remote.utils.schedule import SafeScheduler

if TYPE_CHECKING:
    from jobflow_remote.utils.db import MongoLock

logger = logging.getLogger(__name__)


class Runner:
    """
    Object orchestrating the execution of all the Jobs.

    Advances the status of the Jobs, handles the communication with the workers
    and updates the queue and output databases.

    The main entry point is the `run` method. It is supposed to be executed
    by a daemon, but can also be run directly for testing purposes.
    It allows to run all the steps required to advance the Job's states or even
    a subset of them, to parallelize the different tasks.

    The runner instantiates a pool of workers and hosts given in the project
    definition. A single connection will be opened if multiple workers share
    the same host.

    The Runner schedules the execution of the specific tasks at regular intervals
    and relies on objects like QueueManager, BaseHost and JobController to
    interact with workers and databases.
    """

    def __init__(
        self,
        project_name: str | None = None,
        log_level: LogLevel | None = None,
        runner_id: str | None = None,
    ):
        """
        Parameters
        ----------
        project_name
            Name of the project. Used to retrieve all the configurations required
            to execute the runner.
        log_level
            Logging level of the Runner.
        runner_id
            A unique identifier for the Runner process. Used to identify the
            runner process in logging and in the DB locks.
            If None a uuid will be generated.
        """
        self.stop_signal = False
        self.runner_id: str = runner_id or str(uuid.uuid4())
        self.config_manager: ConfigManager = ConfigManager()
        self.project_name = project_name
        self.project: Project = self.config_manager.get_project(project_name)
        self.job_controller: JobController = JobController.from_project(self.project)
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
        self.limited_workers = {
            name: {"max": w.max_jobs, "current": 0}
            for name, w in self.workers.items()
            if w.max_jobs and not w.batch
        }
        self.batch_workers = {}
        for wname, w in self.workers.items():
            if w.batch is not None:
                self.batch_workers[wname] = RemoteBatchManager(
                    self.hosts[wname], w.batch.jobs_handle_dir
                )
        self.queue_managers: dict = {}
        log_level = log_level if log_level is not None else self.project.log_level
        initialize_runner_logger(
            log_folder=self.project.log_dir,
            level=log_level.to_logging(),
            runner_id=self.runner_id,
        )
        # TODO it could be better to create a pool of stores that are connected
        # How to deal with cases where the connection gets closed?
        # how to deal with file based stores?
        self.jobstore = self.project.get_jobstore()

    @property
    def runner_options(self) -> RunnerOptions:
        """
        The Runner options defined in the project.
        """
        return self.project.runner

    def handle_signal(self, signum, frame):
        """
        Handle the SIGTERM signal in the Runner.
        Sets a variable that will stop the Runner loop.
        """
        logger.info(f"Received signal: {signum}")
        self.stop_signal = True

    def get_worker(self, worker_name: str) -> WorkerBase:
        """
        Get the worker from the pool of workers instantiated by the Runner.

        Parameters
        ----------
        worker_name
            The name of the worker.

        Returns
        -------
            An instance of the corresponding worker.
        """
        if worker_name not in self.workers:
            raise ConfigError(
                f"No worker {worker_name} is defined in project {self.project_name}"
            )
        return self.workers[worker_name]

    def get_host(self, worker_name: str) -> BaseHost:
        """
        Get the host associated to a worker from the pool of hosts instantiated
        by the Runner.

        Parameters
        ----------
        worker_name
            The name of the worker.
        Returns
        -------
            An instance of the Host associated to the worker.
        """
        host = self.hosts[worker_name]
        if not host.is_connected:
            host.connect()
        return host

    def get_queue_manager(self, worker_name: str) -> QueueManager:
        """
        Get an instance of the queue manager associated to a worker, based on its host.

        Parameters
        ----------
        worker_name
            The name of the worker.
        Returns
        -------
             An instance of the QueueManager associated to the worker.
        """
        if worker_name not in self.queue_managers:
            worker = self.get_worker(worker_name)
            self.queue_managers[worker_name] = QueueManager(
                worker.get_scheduler_io(), self.get_host(worker_name)
            )
        return self.queue_managers[worker_name]

    def run(
        self,
        transfer: bool = True,
        complete: bool = True,
        queue: bool = True,
        checkout: bool = True,
        ticks: int | None = None,
    ):
        """
        Start the runner.

        Which actions are being performed can be tuned by the arguments.

        Parameters
        ----------
        transfer
            If True actions related to file transfer are performed by the runner.
        complete
            If True Job completion is performed by the runner.
        queue
            If True interactions with the queue manager are handled by the Runner.
        checkout
            If True the checkout of Jobs is performed by the Runner.
        ticks
            If provided, the Runner will run for this number of ticks before exiting.
        """
        signal.signal(signal.SIGTERM, self.handle_signal)

        states = []
        if transfer:
            states.append(JobState.CHECKED_OUT.value)
            states.append(JobState.TERMINATED.value)
        if complete:
            states.append(JobState.DOWNLOADED.value)
        if queue:
            states.append(JobState.UPLOADED.value)

        logger.info(
            f"Runner run options: transfer: {transfer} complete: {complete} queue: {queue} checkout: {checkout}"
        )

        scheduler = SafeScheduler(seconds_after_failure=120)

        # run a first call for each case, since schedule will wait for the delay
        # to make the first execution.
        if checkout:
            self.checkout()
            scheduler.every(self.runner_options.delay_checkout).seconds.do(
                self.checkout
            )

        if transfer or queue or complete:
            self.advance_state(states)
            scheduler.every(self.runner_options.delay_advance_status).seconds.do(
                self.advance_state, states=states
            )

        if queue:
            self.check_run_status()
            scheduler.every(self.runner_options.delay_check_run_status).seconds.do(
                self.check_run_status
            )
            # Limited workers will only affect the process interacting with the queue
            # manager. When a job is submitted or terminated the count in the
            # limited_workers can be directly updated, since by construction only one
            # process will take care of the queue state.
            # The refresh can be run on a relatively high delay since it should only
            # account for actions from the user (e.g. rerun, cancel), that can alter
            # the number of submitted/running jobs.
            if self.limited_workers:
                self.refresh_num_current_jobs()
                scheduler.every(self.runner_options.delay_refresh_limited).seconds.do(
                    self.refresh_num_current_jobs
                )
            if self.batch_workers:
                self.update_batch_jobs()
                scheduler.every(self.runner_options.delay_update_batch).seconds.do(
                    self.update_batch_jobs
                )

        if complete:
            self.advance_state(states)
            scheduler.every(self.runner_options.delay_advance_status).seconds.do(
                self.advance_state, states=states
            )

        try:
            ticks_remaining: int | bool = True
            if ticks is not None:
                ticks_remaining = ticks

            while ticks_remaining:
                if self.stop_signal:
                    logger.info("stopping due to sigterm")
                    break
                scheduler.run_pending()
                time.sleep(1)

                if ticks is not None:
                    ticks_remaining -= 1

        finally:
            self.cleanup()

    def _get_limited_worker_query(self, states: list[str]) -> dict | None:
        """
        Generate the query to be used for fetching Jobs for workers with limited
        number of Jobs allowed.

        Parameters
        ----------
        states
            The states to be used in the query.
        Returns
        -------
            A dictionary with the query.
        """
        states = [s for s in states if s != JobState.UPLOADED.value]

        available_workers = [w for w in self.workers if w not in self.limited_workers]
        for worker, status in self.limited_workers.items():
            if status["current"] < status["max"]:
                available_workers.append(worker)

        states_query = {"state": {"$in": states}}
        uploaded_query = {
            "state": JobState.UPLOADED.value,
            "worker": {"$in": available_workers},
        }

        if states and available_workers:
            query = {"$or": [states_query, uploaded_query]}
            return query
        elif states:
            return states_query
        elif available_workers:
            return uploaded_query

        return None

    def advance_state(self, states: list[str]):
        """
        Acquire the lock and advance the state of a single job.

        Parameters
        ----------
        states
            The state of the Jobs that can be queried.
        """
        states_methods = {
            JobState.CHECKED_OUT: self.upload,
            JobState.UPLOADED: self.submit,
            JobState.TERMINATED: self.download,
            JobState.DOWNLOADED: self.complete_job,
        }

        while True:
            # handle the case of workers with limited number of jobs
            if self.limited_workers and JobState.UPLOADED.value in states:
                query = self._get_limited_worker_query(states=states)
                if not query:
                    return
            else:
                query = {"state": {"$in": states}}

            with self.job_controller.lock_job_for_update(
                query=query,
                max_step_attempts=self.runner_options.max_step_attempts,
                delta_retry=self.runner_options.delta_retry,
            ) as lock:
                doc = lock.locked_document
                if not doc:
                    return

                state = JobState(doc["state"])

                states_methods[state](lock)

    def upload(self, lock: MongoLock):
        """
        Upload files for a locked Job in the CHECKED_OUT state.
        If successful set the state to UPLOADED.

        Parameters
        ----------
        lock
            The MongoLock with the locked Job document.
        """
        doc = lock.locked_document
        db_id = doc["db_id"]
        logger.debug(f"upload db_id: {db_id}")

        job_dict = doc["job"]

        worker = self.get_worker(doc["worker"])
        host = self.get_host(doc["worker"])
        store = self.jobstore
        # TODO would it be better/feasible to keep a pool of the required
        # Stores already connected, to avoid opening and closing them?
        store.connect()
        try:
            resolve_job_dict_args(job_dict, store)
        finally:
            try:
                store.close()
            except Exception:
                logging.error(f"error while closing the store {store}", exc_info=True)

        remote_path = get_job_path(job_dict["uuid"], job_dict["index"], worker.work_dir)

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

        serialized_input = get_remote_in_file(job_dict, remote_store)

        path_file = Path(remote_path, IN_FILENAME)
        host.put(serialized_input, str(path_file))

        set_output = {
            "$set": {"run_dir": remote_path, "state": JobState.UPLOADED.value}
        }
        lock.update_on_release = set_output

    def submit(self, lock: MongoLock):
        """
        Submit to the queue for a locked Job in the UPLOADED state.
        If successful set the state to SUBMITTED.

        Parameters
        ----------
        lock
            The MongoLock with the locked Job document.
        """
        doc = lock.locked_document
        logger.debug(f"submit db_id: {doc['db_id']}")

        job_dict = doc["job"]

        worker_name = doc["worker"]
        worker = self.get_worker(worker_name)

        remote_path = Path(doc["run_dir"])

        script_commands = [f"jf execution run {remote_path}"]

        queue_manager = self.get_queue_manager(worker_name)
        qout_fpath = remote_path / OUT_FNAME
        qerr_fpath = remote_path / ERR_FNAME

        exec_config = doc["exec_config"]
        if isinstance(exec_config, str):
            exec_config = self.config_manager.get_exec_config(
                exec_config_name=exec_config, project_name=self.project_name
            )
        elif isinstance(exec_config, dict):
            exec_config = ExecutionConfig.parse_obj(exec_config)

        # define an empty default if it is not set
        exec_config = exec_config or ExecutionConfig()

        if worker_name in self.batch_workers:
            resources: dict = {}

            set_name_out(
                resources, job_dict["name"], out_fpath=qout_fpath, err_fpath=qerr_fpath
            )
            shell_manager = queue_manager.get_shell_manager()
            shell_manager.write_submission_script(
                commands=script_commands,
                pre_run=exec_config.pre_run,
                post_run=exec_config.post_run,
                options=resources,
                export=exec_config.export,
                modules=exec_config.modules,
                work_dir=remote_path,
                create_submit_dir=False,
            )

            self.batch_workers[worker_name].submit_job(
                job_id=doc["uuid"], index=doc["index"]
            )
            lock.update_on_release = {
                "$set": {
                    "state": JobState.BATCH_SUBMITTED.value,
                }
            }
        else:
            # decode in case it contains a QResources. It was not deserialized before.
            resources = (
                MontyDecoder().process_decoded(doc["resources"])
                or worker.resources
                or {}
            )
            set_name_out(
                resources, job_dict["name"], out_fpath=qout_fpath, err_fpath=qerr_fpath
            )

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
                if worker_name in self.limited_workers:
                    self.limited_workers[worker_name]["current"] += 1
            else:
                raise RemoteError(
                    f"unhandled submission status {submit_result.status}", True
                )

    def download(self, lock):
        """
        Download the final files for a locked Job in the TERMINATED state.
        If successful set the state to DOWNLOADED.

        Parameters
        ----------
        lock
            The MongoLock with the locked Job document.
        """
        doc = lock.locked_document
        logger.debug(f"download db_id: {doc['db_id']}")

        # job_doc = JobDoc(**doc)
        job_dict = doc["job"]

        # If the worker is local do not copy the files in the temporary folder
        # It should not arrive to this point, since it should go directly
        # from SUBMITTED/RUNNING to DOWNLOADED in case of local worker
        worker = self.get_worker(doc["worker"])
        if not worker.is_local:
            host = self.get_host(doc["worker"])
            store = self.jobstore

            remote_path = doc["run_dir"]
            local_base_dir = Path(self.project.tmp_dir, "download")
            local_path = get_job_path(
                job_dict["uuid"], job_dict["index"], local_base_dir
            )

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
                    err_msg = f"file {remote_file_path} for job {job_dict['uuid']} does not exist"
                    logger.error(err_msg)
                    raise RemoteError(err_msg, True)

        lock.update_on_release = {"$set": {"state": JobState.DOWNLOADED.value}}

    def complete_job(self, lock):
        """
        Complete a locked Job in the DOWNLOADED state.
        If successful set the state to COMPLETED, otherwise to FAILED.

        Parameters
        ----------
        lock
            The MongoLock with the locked Job document.
        """
        doc = lock.locked_document
        logger.debug(f"complete job db_id: {doc['db_id']}")

        # if the worker is local the files were not copied to the temporary
        # folder, but the files could be directly updated
        worker = self.get_worker(doc["worker"])
        if worker.is_local:
            local_path = doc["run_dir"]
        else:
            local_base_dir = Path(self.project.tmp_dir, "download")
            local_path = get_job_path(doc["uuid"], doc["index"], local_base_dir)

        try:
            store = self.jobstore
            completed = self.job_controller.complete_job(doc, local_path, store)

        except json.JSONDecodeError:
            # if an empty file is copied this error can appear, do not retry
            err_msg = traceback.format_exc()
            raise RemoteError(err_msg, True)

        # remove local folder with downloaded files if successfully completed
        if completed and self.runner_options.delete_tmp_folder and not worker.is_local:
            shutil.rmtree(local_path, ignore_errors=True)

        if not completed:
            err_msg = "the parsed output does not contain the required information to complete the job"
            raise RemoteError(err_msg, True)

    def check_run_status(self):
        """
        Check the status of all the jobs submitted to a queue.

        If Jobs started update their state from SUBMITTED to RUNNING.
        If Jobs terminated set their state to TERMINATED if running on a remote
        host. If on a local host set them directly to DOWNLOADED.
        """
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
                    worker = self.get_worker(worker_name)
                    # if the worker is local go directly to DOWNLOADED, as files
                    # are not copied locally
                    if not worker.is_local:
                        next_state = JobState.TERMINATED
                    else:
                        next_state = JobState.DOWNLOADED
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
                    lock_filter = {
                        "uuid": doc["uuid"],
                        "index": doc["index"],
                        "state": doc["state"],
                    }
                    with self.job_controller.lock_job_for_update(
                        query=lock_filter,
                        max_step_attempts=self.runner_options.max_step_attempts,
                        delta_retry=self.runner_options.delta_retry,
                    ) as lock:
                        if lock.locked_document:
                            if error:
                                raise RemoteError(error, False)
                            set_output = {
                                "$set": {
                                    "remote.queue_state": (
                                        qstate.value if qstate else None
                                    ),
                                    "state": next_state.value,
                                }
                            }
                            if start_time:
                                set_output["$set"]["start_time"] = start_time
                            lock.update_on_release = set_output
                    # decrease the amount of jobs running if it is a limited worker
                    if (
                        next_state in (JobState.TERMINATED, JobState.DOWNLOADED)
                        and worker_name in self.limited_workers
                    ):
                        self.limited_workers[doc["worker"]]["current"] -= 1

    def checkout(self):
        """
        Checkout READY Jobs.
        """
        logger.debug("checkout jobs")
        n_checked_out = 0
        while True:
            reserved = self.job_controller.checkout_job()
            if not reserved:
                break

            n_checked_out += 1

        logger.debug(f"checked out {n_checked_out} jobs")

    def refresh_num_current_jobs(self):
        """
        Update the number of jobs currently running for worker with limited
        number of Jobs.
        """
        for name, state in self.limited_workers.items():
            query = {
                "state": {"$in": [JobState.SUBMITTED.value, JobState.RUNNING.value]},
                "worker": name,
            }
            state["current"] = self.job_controller.count_jobs(query)

    def update_batch_jobs(self):
        """
        Update the status of batch jobs.

        Includes submitting to the remote queue, checking the status of
        running jobs in the queue and handle the files with the Jobs information
        about their status.
        """
        logger.debug("update batch jobs")
        for worker_name, batch_manager in self.batch_workers.items():
            worker = self.get_worker(worker_name)
            # first check the processes that are running from the folder
            #  and set them to running if needed
            running_jobs = batch_manager.get_running()
            for job_id, job_index, process_running_uuid in running_jobs:
                lock_filter = {
                    "uuid": job_id,
                    "index": job_index,
                    "state": JobState.BATCH_SUBMITTED.value,
                }
                with self.job_controller.lock_job_for_update(
                    query=lock_filter,
                    max_step_attempts=self.runner_options.max_step_attempts,
                    delta_retry=self.runner_options.delta_retry,
                ) as lock:
                    if lock.locked_document:
                        set_output = {
                            "$set": {
                                "state": JobState.BATCH_RUNNING.value,
                                "start_time": datetime.utcnow(),
                                "remote.process_id": process_running_uuid,
                            }
                        }
                        lock.update_on_release = set_output

            # Check the processes that should be running on the remote queue
            # and update the state in the DB if something changed
            batch_processes_data = self.job_controller.get_batch_processes(worker_name)
            processes = list(batch_processes_data.keys())
            queue_manager = self.get_queue_manager(worker_name)
            if processes:
                qjobs = queue_manager.get_jobs_list(processes)
                running_processes = {qjob.job_id for qjob in qjobs}
                stopped_processes = set(processes) - running_processes
                for pid in stopped_processes:
                    self.job_controller.remove_batch_process(pid, worker_name)
                    # check if there are jobs that were in the running folder of a
                    # process that finished and set them to remote error
                    for job_id, job_index, process_running_uuid in running_jobs:
                        if batch_processes_data[pid] == process_running_uuid:
                            lock_filter = {
                                "uuid": job_id,
                                "index": job_index,
                                "state": {
                                    "$in": (
                                        JobState.BATCH_SUBMITTED.value,
                                        JobState.BATCH_RUNNING.value,
                                    )
                                },
                            }
                            with self.job_controller.lock_job_for_update(
                                query=lock_filter,
                                max_step_attempts=self.runner_options.max_step_attempts,
                                delta_retry=self.runner_options.delta_retry,
                            ) as lock:
                                if lock.locked_document:
                                    raise RuntimeError(
                                        f"The batch process that was running the job (process_id: {pid}, uuid: {process_running_uuid} was likely killed before terminating the job execution"
                                    )

                processes = list(running_processes)

            # check that enough processes are submitted and submit the required
            # amount to reach max_jobs, if needed.
            n_jobs = self.job_controller.count_jobs(
                {
                    "state": {
                        "$in": (
                            JobState.BATCH_SUBMITTED.value,
                            JobState.BATCH_RUNNING.value,
                        )
                    },
                    "worker": worker_name,
                }
            )
            n_processes = len(processes)
            n_jobs_to_submit = min(
                max(worker.max_jobs - n_processes, 0), max(n_jobs - n_processes, 0)
            )
            logger.debug(
                f"submitting {n_jobs_to_submit} batch jobs for worker {worker_name}"
            )
            for _ in range(n_jobs_to_submit):
                resources = worker.resources or {}
                process_running_uuid = suuid()
                remote_path = Path(
                    get_job_path(process_running_uuid, None, worker.batch.work_dir)
                )
                qout_fpath = remote_path / OUT_FNAME
                qerr_fpath = remote_path / ERR_FNAME
                set_name_out(
                    resources,
                    f"batch_{process_running_uuid}",
                    out_fpath=qout_fpath,
                    err_fpath=qerr_fpath,
                )

                # note that here the worker.work_dir needs to be passed,
                # not the worker.batch.work_dir
                command = f"jf execution run-batch {worker.work_dir} {worker.batch.jobs_handle_dir} {process_running_uuid}"
                if worker.batch.max_jobs:
                    command += f" -mj {worker.batch.max_jobs}"
                if worker.batch.max_time:
                    command += f" -mt {worker.batch.max_time}"
                if worker.batch.max_wait:
                    command += f" -mw {worker.batch.max_wait}"

                submit_result = queue_manager.submit(
                    commands=[command],
                    pre_run=worker.pre_run,
                    post_run=worker.post_run,
                    options=resources,
                    work_dir=remote_path,
                    create_submit_dir=True,
                )

                if submit_result.status == SubmissionStatus.FAILED:
                    logger.error(f"submission failed. {repr(submit_result)}")
                elif submit_result.status == SubmissionStatus.JOB_ID_UNKNOWN:
                    logger.error(
                        f"submission succeeded but ID not known. Job may be running but status cannot be checked. {repr(submit_result)}"
                    )

                elif submit_result.status == SubmissionStatus.SUCCESSFUL:
                    self.job_controller.add_batch_process(
                        submit_result.job_id, process_running_uuid, worker_name
                    )
                else:
                    logger.error(
                        f"unhandled submission status {submit_result.status}", True
                    )

            # check for jobs that have terminated in the batch runner and
            # update the DB state accordingly
            terminated_jobs = batch_manager.get_terminated()
            for job_id, job_index, process_running_uuid in terminated_jobs:
                lock_filter = {
                    "uuid": job_id,
                    "index": job_index,
                    "state": {
                        "$in": (
                            JobState.BATCH_SUBMITTED.value,
                            JobState.BATCH_RUNNING.value,
                        )
                    },
                }
                with self.job_controller.lock_job_for_update(
                    query=lock_filter,
                    max_step_attempts=self.runner_options.max_step_attempts,
                    delta_retry=self.runner_options.delta_retry,
                ) as lock:
                    if lock.locked_document:
                        if not worker.is_local:
                            next_state = JobState.TERMINATED
                        else:
                            next_state = JobState.DOWNLOADED
                        set_output = {
                            "$set": {
                                "state": next_state.value,
                                "remote.process_id": process_running_uuid,
                            }
                        }
                        lock.update_on_release = set_output
                batch_manager.delete_terminated(
                    [(job_id, job_index, process_running_uuid)]
                )

    def cleanup(self):
        """
        Close all the connections after stopping the Runner.
        """
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
