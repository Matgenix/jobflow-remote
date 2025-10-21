from __future__ import annotations

import logging
import os
import random
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING

from flufl.lock import Lock, LockError
from monty.json import MontyDecoder

if TYPE_CHECKING:
    from jobflow_remote.remote.host import BaseHost

from jobflow_remote.jobs.data import BATCH_INFO_FILENAME

logger = logging.getLogger(__name__)


LOCK_DIR = "lock"

RUN_FINISHED_DIR = "run_finished"

RUNNING_DIR = "running"

SUBMITTED_DIR = "submitted"


class RemoteBatchManager:
    """
    Manager of remote files containing information about Jobs to be handled by
    a batch worker.

    Used by the Runner.
    """

    def __init__(
        self,
        host: BaseHost,
        files_dir: str | Path,
    ) -> None:
        """

        Parameters
        ----------
        host
            The host where the files are.
        files_dir
            The full path to directory where the files are stored.
        """
        self.host = host
        self.files_dir = Path(files_dir)
        self.submitted_dir = self.files_dir / SUBMITTED_DIR
        self.running_dir = self.files_dir / RUNNING_DIR
        self.run_finished_dir = self.files_dir / RUN_FINISHED_DIR
        self.lock_dir = self.files_dir / LOCK_DIR
        # All the directories need to be initialized to check that they exist
        # and the host connected.
        # Doing it here has two downsides: 1) it slows down the
        # start of the runner just for a batch worker being present
        # 2) if the connection cannot be established the runner may not
        # even start due to the connection errors.
        # The initialization is thus done once when the first action is performed.
        self._dir_initialized = False

    def _init_files_dir(self) -> None:
        """Initialize the file directory, creating all the subdirectories."""
        self.host.connect()
        self.host.mkdir(self.files_dir)
        self.host.mkdir(self.submitted_dir)
        self.host.mkdir(self.running_dir)
        self.host.mkdir(self.run_finished_dir)
        self.host.mkdir(self.lock_dir)
        self._dir_initialized = True

    def submit_job(self, job_id: str, index: int) -> None:
        """
        Submit a Job by uploading the corresponding file.

        Parameters
        ----------
        job_id
            Uuid of the Job.
        index
            Index of the Job.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        self.host.write_text_file(self.submitted_dir / f"{job_id}_{index}", "")

    def get_submitted(self) -> list[str]:
        """
        Get a list of files present in the submitted directory.

        Returns
        -------
            The list of file names in the submitted directory.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        return self.host.listdir(self.submitted_dir)

    def get_run_finished(self) -> list[tuple[str, int, str]]:
        """
        Get job ids and batch unique ids of the jobs that finished to run from the corresponding
        directory on the host.

        Returns
        -------
        list
            The list of job ids, job indexes and batch process uuids in the host
            "run_finished" directory.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        run_finished = []
        for i in self.host.listdir(self.run_finished_dir):
            job_id, _index, batch_uid = i.split("_")
            index = int(_index)
            run_finished.append((job_id, index, batch_uid))
        return run_finished

    def get_running(self) -> list[tuple[str, int, str]]:
        """
        Get job ids and batch unique ids of the running jobs from the corresponding
        directory on the host.

        Returns
        -------
        list
            The list of job ids, job indexes and batch process uuids in the host
            running directory.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        running = []
        for filename in self.host.listdir(self.running_dir):
            job_id, _index, batch_uid = filename.split("_")
            index = int(_index)
            running.append((job_id, index, batch_uid))
        return running

    def delete_run_finished(self, ids: list[tuple[str, int, str]]) -> None:
        if not self._dir_initialized:
            self._init_files_dir()
        for job_id, index, batch_uid in ids:
            self.host.remove(self.run_finished_dir / f"{job_id}_{index}_{batch_uid}")

    def delete_running(self, batch_uid: str) -> None:
        """
        Remove job files from the running folder for a specific batch unique id.

        Should be used only for jobs that failed and left dangling running files.

        Parameters
        ----------
        batch_uid
            The uuid of the batch process for the running files to be removed.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        running_files = self.host.listdir(self.running_dir)
        for filename in running_files:
            if filename.endswith(batch_uid):
                self.host.remove(self.running_dir / filename)

    def cleanup(self) -> bool:
        """
        Remove the files directory on the host.

        Returns
        -------
        bool
            True if the directory was successfully deleted or was not existing.
        """
        if self.host.exists(self.files_dir):
            return self.host.rmtree(self.files_dir, raise_on_error=False)
        return True

    def get_batch_info(
        self, batch_dir: Path | str, batch_info_file: str = BATCH_INFO_FILENAME
    ) -> dict | None:
        batch_info_path = Path(batch_dir) / batch_info_file
        if not self.host.exists(batch_info_path):
            return None
        json_str = self.host.read_text_file(batch_info_path)
        return MontyDecoder().decode(json_str)


class LocalBatchManager:
    """
    Manager of local files  containing information about Jobs to be handled by
    a batch worker.

    Used in the worker to executes the batch Jobs.
    """

    def __init__(
        self,
        files_dir: str | Path,
        batch_uid: str,
        multiprocess_lock=None,
    ) -> None:
        """
        Parameters
        ----------
        files_dir
            The full path to directory where the files to handle the jobs
            to be executed in batch processes are stored.
        batch_uid
            The uuid associated to the batch process.
        multiprocess_lock
            A lock from the multiprocessing module to be used when executing jobs in
            parallel with other processes of the same worker.
        """
        self.batch_uid = batch_uid
        self.files_dir = Path(files_dir)
        self.multiprocess_lock = multiprocess_lock
        self.submitted_dir = self.files_dir / SUBMITTED_DIR
        self.running_dir = self.files_dir / RUNNING_DIR
        self.run_finished_dir = self.files_dir / RUN_FINISHED_DIR
        self.lock_dir = self.files_dir / LOCK_DIR

    def get_job(self) -> str | None:
        """
        Select randomly a job from the submitted directory to be executed.
        Move the file to the running directory.

        Locks will prevent the same job from being executed from other processes.
        If no job can be executed, None is returned.

        Returns
        -------
        str | None
            The name of the job that was selected, or None if no job can be executed.
        """
        files = os.listdir(self.submitted_dir)

        while files:
            selected = random.choice(files)
            try:
                with ExitStack() as lock_stack:
                    # if in a multiprocess execution, avoid concurrent interaction
                    # from processes belonging to the same job
                    if self.multiprocess_lock:
                        lock_stack.enter_context(self.multiprocess_lock)
                    lock_stack.enter_context(
                        Lock(
                            str(self.lock_dir / selected),
                            lifetime=60,
                            default_timeout=0,
                        )
                    )
                    os.remove(self.submitted_dir / selected)
                    (self.running_dir / f"{selected}_{self.batch_uid}").touch()
                    return selected
            except (LockError, FileNotFoundError):
                logger.exception(
                    f"Error while locking file {selected}. Will be ignored"
                )
                files.remove(selected)
        return None

    def set_job_finished(self, job_id: str, index: int) -> None:
        """
        Set a job as run_finished by removing the corresponding file from the running
        directory and adding a new file in the "run_finished" directory.

        Parameters
        ----------
        job_id
            The uuid of the job to set as run_finished.
        index
            The index of the job to set as run_finished.
        """
        os.remove(self.running_dir / f"{job_id}_{index}_{self.batch_uid}")
        (self.run_finished_dir / f"{job_id}_{index}_{self.batch_uid}").touch()
