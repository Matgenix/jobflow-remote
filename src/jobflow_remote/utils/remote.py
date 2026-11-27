from __future__ import annotations

import logging
import socket
import threading
from typing import TYPE_CHECKING, ClassVar

from paramiko.ssh_exception import (
    AuthenticationException,
    NoValidConnectionsError,
    SSHException,
)

from jobflow_remote import ConfigManager
from jobflow_remote.remote.host.base import HostConnectionError, HostError

if TYPE_CHECKING:
    from pathlib import Path

    from jobflow_remote.config.base import Project
    from jobflow_remote.remote.host.base import BaseHost

logger = logging.getLogger(__name__)


class _SharedHostsState(threading.local):
    """
    The state of SharedHosts, kept separately for each thread.

    ``threading.local`` calls ``__init__`` the first time each thread accesses
    the object, so every thread starts with no context open.
    """

    def __init__(self) -> None:
        self.ref_count: int = 0
        self.hosts: dict[str, BaseHost] = {}
        self.project: Project | None = None


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

    The state (the hosts, the project and the count of the open contexts) is
    kept separately for each thread. Nested contexts in the same thread share
    their hosts, while contexts opened in different threads are independent:
    each thread has its own connections, can be bound to a different project,
    and closes only its own hosts. For this reason ``get_host`` must be called
    inside a context opened in the same thread: the object cannot be passed
    to another thread to use the hosts of the first one.

    Examples
    --------

    >>> with SharedHosts(project) as shared_hosts:
    ...     host = shared_hosts.get_host("worker_name")
    ...     # Use host as required
    """

    _instance: SharedHosts | None = None
    _state: ClassVar[_SharedHostsState] = _SharedHostsState()

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
        state = self._state
        if state.ref_count == 0:
            # No context is currently active in this thread: this is (or
            # precedes) the outermost context, so (re)bind to the requested
            # project, discarding any project left over from a previous operation.
            if project is None:
                config_manager: ConfigManager = ConfigManager()
                project = config_manager.get_project(None)
            state.project = project
        elif project is not None and project.name != state.project.name:
            # A context is already open in this thread and bound to another
            # project. Sharing host connections across projects is not
            # supported, so refuse instead of silently using the wrong
            # project's workers.
            raise ValueError(
                f"SharedHosts is already active for project '{state.project.name}' "
                f"and cannot be used for a different project ('{project.name}') "
                "at the same time."
            )

    @property
    def _hosts(self) -> dict[str, BaseHost]:
        """The hosts shared in the current thread, by worker name."""
        return self._state.hosts

    @property
    def _project(self) -> Project | None:
        """The project bound in the current thread."""
        return self._state.project

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

        Raises
        ------
        RuntimeError
            If called outside a context opened in the current thread.
        HostConnectionError
            If the connection to the host cannot be established. The original
            error is its ``__cause__``.
        """
        if self._state.ref_count == 0:
            # Outside a context the host would never be closed. In another
            # thread than the one that opened the context, the state is not
            # shared: the hosts of that thread are not available here.
            raise RuntimeError(
                "SharedHosts.get_host can only be called inside a "
                "'with SharedHosts(...)' block opened in the same thread."
            )
        if worker not in self._project.workers:
            raise ValueError(f"Worker {worker} not defined in {self._project.name}")
        if worker in self._hosts:
            return self._hosts[worker]

        host = self._project.workers[worker].get_host()
        try:
            host.connect()
        except HostError:
            # already a host error with a meaningful message
            raise
        except Exception as exc:
            raise HostConnectionError(
                f"Could not connect to the worker '{worker}': "
                f"{_describe_connection_error(exc)} ({type(exc).__name__}: {exc})",
                worker=worker,
            ) from exc
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
        # Increment reference count of the current thread
        self._state.ref_count += 1

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Decrement reference count of the current thread
        state = self._state
        state.ref_count -= 1

        # Cleanup only when the last context of this thread exits
        if state.ref_count == 0:
            self.close_hosts()
            # Release the project so the next operation rebinds a fresh one
            state.project = None


def _describe_connection_error(exc: Exception) -> str:
    """
    Describe in words the reason of an error raised while connecting to a host.

    Parameters
    ----------
    exc
        The error raised while connecting.

    Returns
    -------
    str
        A short description of the reason of the failure.
    """
    # NoValidConnectionsError is raised by paramiko when the connection is
    # refused or the host is unreachable on all its addresses.
    if isinstance(exc, NoValidConnectionsError):
        return "the host refused the connection or could not be reached"
    if isinstance(exc, TimeoutError):
        return "the host did not answer in time"
    if isinstance(exc, ConnectionRefusedError):
        return "the host refused the connection"
    if isinstance(exc, socket.gaierror):
        return "the host name could not be resolved"
    if isinstance(exc, AuthenticationException):
        return "the authentication failed"
    if isinstance(exc, SSHException):
        return "the SSH connection failed"
    return "the connection failed"


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
