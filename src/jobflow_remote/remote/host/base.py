from __future__ import annotations

import abc
import logging
import re
import traceback
from typing import TYPE_CHECKING

from monty.json import MSONable

if TYPE_CHECKING:
    from pathlib import Path


logger = logging.getLogger(__name__)

SANITIZE_KEY = r"_-_-_-_-_### JFREMOTE SANITIZE ###_-_-_-_-_"


class BaseHost(MSONable):
    """Base Host class."""

    def __init__(self, sanitize: bool = False):
        """
        Parameters
        ----------
        sanitize
            If True text a string will be prepended and appended to the output
            of the commands, to ease the parsing and avoid failures due to spurious
            text coming from the host shell.
        """
        self.sanitize = sanitize
        self._sanitize_regex: re.Pattern | None = None

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
        timeout
            Timeout for the execution of the commands.
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
            if returncode != 0:
                msg = f"Command was executed but return code was different from zero.\nstdoud: {stdout}\nstderr: {stderr}"
            elif stdout.strip() != "test" or stderr.strip() != "":
                msg = (
                    "Command was executed but the output is not the expected one (i.e. a single 'test' "
                    f"string in both stdout and stderr).\nstdoud: {stdout}\nstderr: {stderr}"
                )
                if not self.sanitize:
                    msg += (
                        "\nIf the output contains additional text the problem may be solved by setting "
                        "the 'sanitize_command' option to True in the project configuration."
                    )

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

        raise_on_error : bool
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

    @property
    def sanitize_regex(self) -> re.Pattern:
        """
        Regular expression to sanitize sensitive info in command outputs.
        """
        if not self._sanitize_regex:
            escaped_key = re.escape(SANITIZE_KEY)
            # Optionally match the newline that comes from the "echo" command.
            # The -n option for echo to suppress the newline seems to not be
            # supported on all systems
            self._sanitize_regex = re.compile(
                f"{escaped_key}\r?\n?(.*?)(?:{escaped_key}\r?\n?|$)", re.DOTALL
            )

        return self._sanitize_regex

    def sanitize_command(self, cmd: str) -> str:
        """
        Sanitizes a command by adding a prefix and suffix to the command string if
        sanitization is enabled.
        The prefix and suffix are the same and are used to mark the parts of the output
        that should be sanitized. The prefix and suffix are defined by `SANITIZE_KEY`.

        Parameters
        ----------
        cmd
            The command string to be sanitized

        Returns
        -------
        str
            The sanitized command string
        """
        if self.sanitize:
            echo_cmd = f'echo "{SANITIZE_KEY}" | tee /dev/stderr'
            cmd = f"{echo_cmd};{cmd};{echo_cmd}"
        return cmd

    def sanitize_output(self, output: str) -> str:
        """
        Sanitizes the output of a command by selecting the section between the
        SANITIZE_KEY strings.
        If the second instance of the key is not found, the part of the output after the key is returned.
        If the key is not present, the entire output is returned.

        Parameters
        ----------
        output
            The output of the command to be sanitized

        Returns
        -------
        str
            The sanitized output
        """
        if self.sanitize:
            match = self.sanitize_regex.search(output)
            if not match:
                logger.warning(
                    f"Even if sanitization was required, there was no match for the output: {output}. Returning the complete output"
                )
                return output
            return match.group(1)
        return output


class HostError(Exception):
    pass
