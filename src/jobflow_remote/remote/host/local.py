from __future__ import annotations

import os
import shutil
import subprocess
import warnings
from pathlib import Path

from monty.os import cd

from jobflow_remote.remote.host.base import BaseHost


class LocalHost(BaseHost):
    def __init__(self, timeout_execute: int = None, sanitize: bool = False) -> None:
        self.timeout_execute = timeout_execute
        super().__init__(sanitize=sanitize)

    def __eq__(self, other):
        return isinstance(other, LocalHost)

    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        timeout: int | None = None,
    ):
        """Execute the given command on the host.

        Note that the command is executed with shell=True, so commands can
        be exposed to command injection. Consider whether to escape part of
        the input if it comes from external users.

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str
        workdir: str or None
            path where the command will be executed.
        timeout
            Timeout for the execution of the commands.

        Returns
        -------
        stdout : str
            Standard output of the command
        stderr : str
            Standard error of the command
        exit_code : int
            Exit code of the command.
        """
        if isinstance(command, (list, tuple)):
            command = " ".join(command)
        command = self.sanitize_command(command)
        workdir = str(workdir) if workdir else Path.cwd()
        timeout = timeout or self.timeout_execute
        with cd(workdir):
            proc = subprocess.run(
                command, capture_output=True, shell=True, timeout=timeout, check=False
            )
        stdout = self.sanitize_output(proc.stdout.decode())
        stderr = self.sanitize_output(proc.stderr.decode())
        return stdout, stderr, proc.returncode

    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        try:
            Path(directory).mkdir(parents=recursive, exist_ok=exist_ok)
        except OSError:
            return False
        return True

    def write_text_file(self, filepath, content) -> None:
        Path(filepath).write_text(content)

    def connect(self) -> None:
        pass

    def close(self) -> bool:
        return True

    @property
    def is_connected(self) -> bool:
        return True

    def put(self, src, dst) -> None:
        is_file_like = hasattr(src, "read") and callable(src.read)

        src_base = getattr(src, "name", None) if is_file_like else os.path.basename(src)

        if Path(dst).is_dir():
            if src_base:
                dst = Path(dst, src_base)
            elif is_file_like:
                raise ValueError(
                    "could not determine the file name and dst is a folder"
                )

        if is_file_like:
            with open(dst, "wb") as f:
                f.write(src.read())
        else:
            self.copy(src, dst)

    def get(self, src, dst) -> None:
        is_file_like = hasattr(dst, "write") and callable(dst.write)

        if is_file_like:
            with open(src, "rb") as f:
                dst.write(f.read())
        else:
            self.copy(src, dst)

    def copy(self, src, dst) -> None:
        shutil.copy(src, dst)

    def listdir(self, path: str | Path) -> list[str]:
        try:
            return os.listdir(path)
        except FileNotFoundError:
            return []

    def remove(self, path: str | Path) -> None:
        os.remove(path)

    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        """Recursively delete a directory tree on a local host.

        It is intended to remove an entire directory tree, including all files
        and subdirectories, on this local host.

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
        removed = True

        def onerror(func, dir_path, err):
            nonlocal removed
            removed = False
            warnings.warn(f"Error while deleting folder {path}: {err[1]}", stacklevel=2)

        shutil.rmtree(path, onerror=onerror if not raise_on_error else None)

        return removed

    def shell(self, pre_cmd: str | None = None, shell: str = "bash"):
        """
        Open a connection to the host and starts the selected shell

        Parameters
        ----------
        pre_cmd
            Any command to be executed before starting the shell
        shell
            The name of the shell to start
        """

        from invoke import Context

        cmd = shell
        if pre_cmd:
            cmd = f"{pre_cmd}; {shell}"
        Context().run(cmd, pty=True)
