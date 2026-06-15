from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from jobflow_remote import ConfigManager

if TYPE_CHECKING:
    from pathlib import Path

    from jobflow_remote.config.base import Project
    from jobflow_remote.remote.host.base import BaseHost

logger = logging.getLogger(__name__)


class SharedHosts:
    """
    A singleton context manager to allow sharing the same host objects.

    Hosts are stored internally, associated to the worker name
    Being a singleton, opening the context manager multiple times allows
    to share the hosts across different sections of the code, if needed.
    Hosts connections are all closed only when leaving the last context
    manager.

    A single project can be active at a time: the project is bound when the
    outermost context is opened and released when the last one is closed.
    Opening a nested context for a different project raises an error, since
    sharing host connections across projects is not supported.

    Examples
    --------

    >>> with SharedHosts(project) as shared_hosts:
    ...     host = shared_hosts.get_host("worker_name")
    ...     # Use host as required
    """

    _instance: SharedHosts = None
    _ref_count: int = 0
    _hosts: ClassVar[dict[str, BaseHost]] = {}
    _project: Project | None = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, project: Project | None = None):
        """
        Parameters
        ----------
        project
            The project configuration.
        """
        cls = self.__class__
        if cls._ref_count == 0:
            # No context is currently active: this is (or precedes) the
            # outermost context, so (re)bind to the requested project,
            # discarding any project left over from a previous operation.
            if project is None:
                config_manager: ConfigManager = ConfigManager()
                project = config_manager.get_project(None)
            cls._project = project
        elif project is not None and project.name != cls._project.name:
            # A context is already open and bound to another project. Sharing
            # host connections across projects is not supported, so refuse
            # instead of silently using the wrong project's workers.
            raise ValueError(
                f"SharedHosts is already active for project '{cls._project.name}' "
                f"and cannot be used for a different project ('{project.name}') "
                "at the same time."
            )

    def get_host(self, worker: str) -> BaseHost:
        """
        Return the shared host, if already defined, otherwise retrieve
        the host from the project and connect it.

        Parameters
        ----------
        worker
            The name of a worker defined in the project
        Returns
        -------
        BaseHost
            The shared host.
        """
        if worker not in self._project.workers:
            raise ValueError(f"Worker {worker} not defined in {self._project.name}")
        if worker in self._hosts:
            return self._hosts[worker]

        host = self._project.workers[worker].get_host()
        host.connect()
        self._hosts[worker] = host
        return host

    def close_hosts(self) -> None:
        """Close the connection to all the connected hosts"""
        for worker in list(self._hosts):
            try:
                self._hosts[worker].close()
            except Exception:
                logger.exception(
                    f"Error while closing the connection to the {worker} worker"
                )
            finally:
                self._hosts.pop(worker)

    def __enter__(self):
        # Increment reference count
        self.__class__._ref_count += 1

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Decrement reference count
        self.__class__._ref_count -= 1

        # Cleanup only when the last context exits
        if self.__class__._ref_count == 0:
            self.close_hosts()
            # Release the project so the next operation rebinds a fresh one
            self.__class__._project = None


class UnsafeDeletionError(Exception):
    """
    Error to signal that Job files could not be deleted as the safety check
    did not pass.
    """


def safe_remove_job_files(
    host: BaseHost, run_dir: str | Path | None, raise_on_error: bool = False
) -> bool:
    if not run_dir:
        return False

    remote_files = host.listdir(run_dir)
    # safety measure to avoid mistakenly deleting other folders
    if not remote_files:
        return False
    if any(fn in remote_files for fn in ("jfremote_in.json", "jfremote_in.json.gz")):
        return host.rmtree(path=run_dir, raise_on_error=raise_on_error)

    if raise_on_error:
        raise UnsafeDeletionError(
            f"Could not delete folder {run_dir} "
            "since it may not contain a jobflow-remote execution. Some files are present, "
            "but jfremote_in.json is missing",
        )

    logger.warning(
        f"Did not delete folder {run_dir} "
        "since it may not contain a jobflow-remote executionSome files are present, "
        "but jfremote_in.json is missing",
    )
    return False
