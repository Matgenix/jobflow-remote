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
    """Host that delegates commands and file transfers to separate connections.

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
        """Execute the given command on the command host."""
        return self.command_host.execute(command, workdir, timeout)

    def shell(self, pre_cmd: str | None = None, shell: str = "bash"):
        """Open a shell on the command host."""
        return self.command_host.shell(pre_cmd, shell)

    # -------------------------------------------------------------------------
    # File operations - delegated to transfer_host
    # -------------------------------------------------------------------------

    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """Create directory via transfer host."""
        return self.transfer_host.mkdir(directory, recursive, exist_ok)

    def write_text_file(self, filepath: str | Path, content: str) -> None:
        """Write content to a file via transfer host."""
        return self.transfer_host.write_text_file(filepath, content)

    def read_text_file(self, filepath: str | Path) -> str:
        """Read content from a file via transfer host."""
        return self.transfer_host.read_text_file(filepath)

    def put(self, src, dst) -> None:
        """Upload file via transfer host."""
        return self.transfer_host.put(src, dst)

    def get(self, src, dst) -> None:
        """Download file via transfer host."""
        return self.transfer_host.get(src, dst)

    def copy(self, src, dst) -> None:
        """Copy file on transfer host."""
        return self.transfer_host.copy(src, dst)

    def move(self, src, dst) -> None:
        """Move file on transfer host."""
        return self.transfer_host.move(src, dst)

    def listdir(self, path: str | Path) -> list[str]:
        """List directory via transfer host."""
        return self.transfer_host.listdir(path)

    def remove(self, path: str | Path) -> None:
        """Remove file via transfer host."""
        return self.transfer_host.remove(path)

    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        """Recursively delete directory tree via transfer host."""
        return self.transfer_host.rmtree(path, raise_on_error)

    def exists(self, path: str | Path) -> bool:
        """Check if path exists via transfer host."""
        return self.transfer_host.exists(path)

    # -------------------------------------------------------------------------
    # Connection management - manages both hosts
    # -------------------------------------------------------------------------

    def connect(self) -> None:
        """Open both connections."""
        self.command_host.connect()
        self.transfer_host.connect()

    def close(self) -> bool:
        """Close both connections."""
        cmd_closed = self.command_host.close()
        transfer_closed = self.transfer_host.close()
        return cmd_closed and transfer_closed

    @property
    def is_connected(self) -> bool:
        """True if both connections are open."""
        return self.command_host.is_connected and self.transfer_host.is_connected

    @property
    def interactive_login(self) -> bool:
        """True if either host requires interactive login."""
        return (
            self.command_host.interactive_login
            or self.transfer_host.interactive_login
        )
