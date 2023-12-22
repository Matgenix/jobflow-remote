from __future__ import annotations

import abc
import traceback
from pathlib import Path

from monty.json import MSONable


class BaseHost(MSONable):
    """Base Host class."""

    @abc.abstractmethod
    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        timeout: int | None = None,
    ):
        """Execute the given command on the host

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


class HostError(Exception):
    pass
