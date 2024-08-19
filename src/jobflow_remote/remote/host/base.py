from __future__ import annotations

import abc
import traceback
from typing import TYPE_CHECKING

from monty.json import MSONable

if TYPE_CHECKING:
    from pathlib import Path


class BaseHost(MSONable):
    """Base Host class."""

    @abc.abstractmethod
    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        timeout: int | None = None,
    ) -> tuple[str, str, int]:
        """Execute the given command on the host.

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str
        workdir: str or None
            path where the command will be executed.

        """
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """Create directory on the host."""
        raise NotImplementedError

    @abc.abstractmethod
    def write_text_file(self, filepath, content):
        """Write content to a file on the host."""
        raise NotImplementedError

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> bool:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, src, dst):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, src, dst):
        raise NotImplementedError

    @abc.abstractmethod
    def copy(self, src, dst):
        raise NotImplementedError

    def test(self) -> str | None:
        msg = None
        try:
            cmd = "echo 'test'"
            stdout, stderr, returncode = self.execute(cmd)
            if returncode != 0 or stdout.strip() != "test":
                msg = f"Command was executed but some error occurred.\nstdoud: {stdout}\nstderr: {stderr}"
        except Exception:
            exc = traceback.format_exc()
            msg = f"Error while executing command:\n {exc}"

        return msg

    @abc.abstractmethod
    def listdir(self, path: str | Path) -> list[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def remove(self, path: str | Path):
        raise NotImplementedError

    @abc.abstractmethod
    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        """Recursively delete a directory tree on a host.

        This method must be implemented by subclasses of  `BaseHost`.
        It is intended to remove an entire directory tree, including all files
        and subdirectories, on the host represented by the subclass.

        Parameters
        ----------
        path : str or Path
            The path to the directory tree to be removed.

        raise_on_error : bool, optional
            If set to `False` (default), errors will be ignored, and the method will
            attempt to continue removing remaining files and directories.
            Otherwise, any errors encountered during the removal process
            will raise an exception.

        Returns
        -------
        bool
            True if the directory tree was successfully removed, False otherwise.
        """
        raise NotImplementedError

    @property
    def interactive_login(self) -> bool:
        """
        True if the host requires interactive actions upon login.
        False by default. Subclasses should override the method to customize the value.
        """
        return False


class HostError(Exception):
    pass
