"""Host implementation that uses separate connections for commands and file transfers.

This is useful for HPC systems where login nodes have SFTP disabled but a dedicated
data transfer node is available (e.g., LRC at LBNL).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from jobflow_remote.remote.host.base import BaseHost

if TYPE_CHECKING:
    from pathlib import Path

    from jobflow_remote.remote.host.remote import RemoteHost

logger = logging.getLogger(__name__)


class SeparatedTransferHost(BaseHost):
    """
    Host that delegates commands and file transfers to separate connections.

    This enables HPC systems where:

    - Login node: SSH/scheduler commands work, but SFTP is disabled
    - Transfer node: SFTP works, but scheduler commands don't work

    Command execution (execute, shell) goes through the command_host.
    File operations (put, get, mkdir, etc.) go through the transfer_host.

    Parameters
    ----------
    command_host : RemoteHost
        Host for executing commands (e.g., login node with SLURM access).
    transfer_host : RemoteHost
        Host for file transfers (e.g., data transfer node with SFTP).
    """

    def __init__(
        self,
        command_host: RemoteHost,
        transfer_host: RemoteHost,
    ) -> None:
        self.command_host = command_host
        self.transfer_host = transfer_host
        # Use sanitize setting from command host
        super().__init__(sanitize=command_host.sanitize)

    def __eq__(self, other):
        if not isinstance(other, SeparatedTransferHost):
            return False
        return (
            self.command_host == other.command_host
            and self.transfer_host == other.transfer_host
        )

    # -------------------------------------------------------------------------
    # Command execution - delegated to command_host
    # -------------------------------------------------------------------------

    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        timeout: int | None = None,
    ) -> tuple[str, str, int]:
        """Execute the given command on the command host.

        Parameters
        ----------
        command : str or list of str
            Command to execute, as a str or list of str.
        workdir : str or Path or None
            Path where the command will be executed.
        timeout : int or None
            Timeout for the execution of the command.

        Returns
        -------
        stdout : str
            Standard output of the command.
        stderr : str
            Standard error of the command.
        exit_code : int
            Exit code of the command.
        """
        return self.command_host.execute(command, workdir, timeout)

    def shell(self, pre_cmd: str | None = None, shell: str = "bash"):
        """Open a shell on the command host.

        Parameters
        ----------
        pre_cmd : str or None
            Any command to be executed before starting the shell.
        shell : str
            The name of the shell to start.
        """
        return self.command_host.shell(pre_cmd, shell)

    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """Create directory on the command host.

        Parameters
        ----------
        directory : str or Path
            Path of the directory to create.
        recursive : bool
            If True, create parent directories as needed.
        exist_ok : bool
            If True, do not raise an error if directory exists.

        Returns
        -------
        bool
            True if the directory was created successfully.
        """
        return self.command_host.mkdir(directory, recursive, exist_ok)

    def copy(self, src, dst) -> None:
        """Copy a file on the command host.

        Parameters
        ----------
        src : str or Path
            Source path on remote host.
        dst : str or Path
            Destination path on remote host.
        """
        return self.command_host.copy(src, dst)

    def move(self, src, dst) -> None:
        """Move a file on the command host.

        Parameters
        ----------
        src : str or Path
            Source path on remote host.
        dst : str or Path
            Destination path on remote host.
        """
        return self.command_host.move(src, dst)

    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        """Recursively delete a directory tree on the command host.

        Parameters
        ----------
        path : str or Path
            Path to the directory tree to be removed.
        raise_on_error : bool
            If False (default), errors will be ignored. Otherwise, errors
            will raise an exception.

        Returns
        -------
        bool
            True if the directory tree was successfully removed.
        """
        return self.command_host.rmtree(path, raise_on_error)

    # -------------------------------------------------------------------------
    # File operations via SFTP commands - delegated to transfer_host
    # -------------------------------------------------------------------------

    def write_text_file(self, filepath: str | Path, content: str) -> None:
        """Write content to a file on the transfer host.

        Parameters
        ----------
        filepath : str or Path
            Path to the file to write.
        content : str
            Content to write to the file.
        """
        return self.transfer_host.write_text_file(filepath, content)

    def read_text_file(self, filepath: str | Path) -> str:
        """Read content from a file on the transfer host.

        Parameters
        ----------
        filepath : str or Path
            Path to the file to read.

        Returns
        -------
        str
            Content of the file.
        """
        return self.transfer_host.read_text_file(filepath)

    def put(self, src, dst) -> None:
        """Upload a file to the transfer host.

        Parameters
        ----------
        src : str or Path or file-like
            Local source path or file-like object.
        dst : str or Path
            Remote destination path.
        """
        return self.transfer_host.put(src, dst)

    def get(self, src, dst) -> None:
        """Download a file from the transfer host.

        Parameters
        ----------
        src : str or Path
            Remote source path.
        dst : str or Path or file-like
            Local destination path or file-like object.
        """
        return self.transfer_host.get(src, dst)

    def listdir(self, path: str | Path) -> list[str]:
        """List directory contents on the transfer host.

        Parameters
        ----------
        path : str or Path
            Path to the directory to list.

        Returns
        -------
        list of str
            List of filenames in the directory.
        """
        return self.transfer_host.listdir(path)

    def remove(self, path: str | Path) -> None:
        """Remove a file on the transfer host.

        Parameters
        ----------
        path : str or Path
            Path to the file to remove.
        """
        return self.transfer_host.remove(path)

    def exists(self, path: str | Path) -> bool:
        """Check if a path exists on the transfer host.

        Parameters
        ----------
        path : str or Path
            The path to check.

        Returns
        -------
        bool
            True if the path exists.
        """
        return self.transfer_host.exists(path)

    # -------------------------------------------------------------------------
    # Connection management - manages both hosts
    # -------------------------------------------------------------------------

    def connect(self) -> None:
        """Open connections to both the command and transfer hosts."""
        self.command_host.connect()
        self.transfer_host.connect()

    def close(self) -> bool:
        """Close connections to both hosts.

        Returns
        -------
        bool
            True if both connections were closed successfully.
        """
        cmd_closed = self.command_host.close()
        transfer_closed = self.transfer_host.close()
        return cmd_closed and transfer_closed

    @property
    def is_connected(self) -> bool:
        """Check if both connections are open.

        Returns
        -------
        bool
            True if both command and transfer hosts are connected.
        """
        return self.command_host.is_connected and self.transfer_host.is_connected

    @property
    def interactive_login(self) -> bool:
        """Check if either host requires interactive login.

        Returns
        -------
        bool
            True if either host requires interactive login.
        """
        return (
            self.command_host.interactive_login or self.transfer_host.interactive_login
        )
