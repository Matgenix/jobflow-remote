from __future__ import annotations

import logging
import os
import random
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING

from flufl.lock import Lock, LockError

if TYPE_CHECKING:
    from jobflow_remote.remote.host import BaseHost


logger = logging.getLogger(__name__)


LOCK_DIR = "lock"

TERMINATED_DIR = "terminated"

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
        self.terminated_dir = self.files_dir / TERMINATED_DIR
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
        self.host.mkdir(self.terminated_dir)
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

    def get_terminated(self) -> list[tuple[str, int, str]]:
        """
        Get job ids and process ids of the terminated jobs from the corresponding
        directory on the host.

        Returns
        -------
        list
            The list of job ids, job indexes and batch process uuids in the host
            terminated directory.
        """
        if not self._dir_initialized:
            self._init_files_dir()
        terminated = []
        for i in self.host.listdir(self.terminated_dir):
            job_id, _index, process_uuid = i.split("_")
            index = int(_index)
            terminated.append((job_id, index, process_uuid))
        return terminated

    def get_running(self) -> list[tuple[str, int, str]]:
        """
        Get job ids and process ids of the running jobs from the corresponding
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
            job_id, _index, process_uuid = filename.split("_")
            index = int(_index)
            running.append((job_id, index, process_uuid))
        return running

    def delete_terminated(self, ids: list[tuple[str, int, str]]) -> None:
        if not self._dir_initialized:
            self._init_files_dir()
        for job_id, index, process_uuid in ids:
            self.host.remove(self.terminated_dir / f"{job_id}_{index}_{process_uuid}")


class LocalBatchManager:
    """
    Manager of local files  containing information about Jobs to be handled by
    a batch worker.

    Used in the worker to executes the batch Jobs.
    """

    def __init__(
        self,
        files_dir: str | Path,
        process_id: str,
        multiprocess_lock=None,
    ) -> None:
        """
        Parameters
        ----------
        files_dir
            The full path to directory where the files to handle the jobs
            to be executed in batch processes are stored.
        process_id
            The uuid associated to the batch process.
        multiprocess_lock
            A lock from the multiprocessing module to be used when executing jobs in
            parallel with other processes of the same worker.
        """
        self.process_id = process_id
        self.files_dir = Path(files_dir)
        self.multiprocess_lock = multiprocess_lock
        self.submitted_dir = self.files_dir / SUBMITTED_DIR
        self.running_dir = self.files_dir / RUNNING_DIR
        self.terminated_dir = self.files_dir / TERMINATED_DIR
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
                    (self.running_dir / f"{selected}_{self.process_id}").touch()
                    return selected
            except (LockError, FileNotFoundError):
                logger.exception(
                    f"Error while locking file {selected}. Will be ignored"
                )
                files.remove(selected)
        return None

    def terminate_job(self, job_id: str, index: int) -> None:
        """
        Terminate a job by removing the corresponding file from the running
        directory and adding a new file in the terminated directory.

        Parameters
        ----------
        job_id
            The uuid of the job to terminate.
        index
            The index of the job to terminate.
        """
        os.remove(self.running_dir / f"{job_id}_{index}_{self.process_id}")
        (self.terminated_dir / f"{job_id}_{index}_{self.process_id}").touch()
