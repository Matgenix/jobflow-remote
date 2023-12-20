from __future__ import annotations

import logging
import os
import random
from pathlib import Path

from flufl.lock import Lock, LockError

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
    ):
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
        self._init_files_dir()

    def _init_files_dir(self):
        """
        Initialize the file directory, creating all the subdiretories.
        """
        self.host.connect()
        # Note that the check of the creation of the folders on a remote host
        # slows down the start of the runner by a few seconds.
        # If this proves to be an issue the folder creation should be moved
        # somewhere else and guaranteed in some other way (e.g. a CLI command
        # for the user?).
        self.host.mkdir(self.files_dir)
        self.host.mkdir(self.submitted_dir)
        self.host.mkdir(self.running_dir)
        self.host.mkdir(self.terminated_dir)
        self.host.mkdir(self.lock_dir)

    def submit_job(self, job_id: str, index: int):
        """
        Submit a Job by uploading the corresponding file.

        Parameters
        ----------
        job_id
            Uuid of the Job.
        index
            Index of the Job.
        """
        self.host.write_text_file(self.submitted_dir / f"{job_id}_{index}", "")

    def get_submitted(self) -> list[str]:
        """
        Get a list of files present in the submitted directory.

        Returns
        -------
            The list of file names in the directory.
        """
        return self.host.listdir(self.submitted_dir)

    def get_terminated(self) -> list[tuple[str, int, str]]:
        """
        Get job ids and process ids of the terminated jobs from the corresponding
        directory.

        Returns
        -------

        """
        terminated = []
        for i in self.host.listdir(self.terminated_dir):
            job_id, index, process_uuid = i.split("_")
            index = int(index)
            terminated.append((job_id, index, process_uuid))
        return terminated

    def get_running(self) -> list[tuple[str, int, str]]:
        running = []
        for filename in self.host.listdir(self.running_dir):
            job_id, index, process_uuid = filename.split("_")
            index = int(index)
            running.append((job_id, index, process_uuid))
        return running

    def delete_terminated(self, ids: list[tuple[str, int, str]]):
        for job_id, index, process_uuid in ids:
            self.host.remove(self.terminated_dir / f"{job_id}_{index}_{process_uuid}")


class LocalBatchManager:
    """
    Manager of local files  containing information about Jobs to be handled by
    a batch worker.

    Used in the worker to executes the batch Jobs.
    """

    def __init__(self, files_dir: str | Path, process_id: str):
        self.process_id = process_id
        self.files_dir = Path(files_dir)
        self.submitted_dir = self.files_dir / SUBMITTED_DIR
        self.running_dir = self.files_dir / RUNNING_DIR
        self.terminated_dir = self.files_dir / TERMINATED_DIR
        self.lock_dir = self.files_dir / LOCK_DIR

    def get_job(self) -> str | None:
        files = os.listdir(self.submitted_dir)

        while files:
            selected = random.choice(files)
            try:
                with Lock(
                    str(self.lock_dir / selected), lifetime=60, default_timeout=0
                ):
                    os.remove(self.submitted_dir / selected)
                    (self.running_dir / f"{selected}_{self.process_id}").touch()
                    return selected
            except (LockError, FileNotFoundError):
                logger.error(
                    f"Error while locking file {selected}. Will be ignored",
                    exc_info=True,
                )
                files.remove(selected)
        return None

    def terminate_job(self, job_id: str, index: int):
        os.remove(self.running_dir / f"{job_id}_{index}_{self.process_id}")
        (self.terminated_dir / f"{job_id}_{index}_{self.process_id}").touch()
