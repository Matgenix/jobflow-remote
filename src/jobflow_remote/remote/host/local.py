from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from monty.os import cd

from jobflow_remote.remote.host.base import BaseHost


class LocalHost(BaseHost):
    def __init__(self, timeout_execute: int = None):
        self.timeout_execute = timeout_execute

    def __eq__(self, other):
        return isinstance(other, LocalHost)

    def execute(
        self,
        command: str | list[str],
        workdir: str | Path | None = None,
        timeout: int | None = None,
    ):
        """Execute the given command on the host

        Note that the command is executed with shell=True, so commands can
        be exposed to command injection. Consider whether to escape part of
        the input if it comes from external users.

        Parameters
        ----------
        command: str or list of str
            Command to execute, as a str or list of str

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
        if not workdir:
            workdir = Path.cwd()
        else:
            workdir = str(workdir)
        timeout = timeout or self.timeout_execute
        with cd(workdir):
            proc = subprocess.run(
                command, capture_output=True, shell=True, timeout=timeout
            )
        return proc.stdout.decode(), proc.stderr.decode(), proc.returncode

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

    def connect(self):
        pass

    def close(self) -> bool:
        return True

    @property
    def is_connected(self) -> bool:
        return True

    def put(self, src, dst):
        is_file_like = hasattr(src, "read") and callable(src.read)

        if is_file_like:
            src_base = getattr(src, "name", None)
        else:
            src_base = os.path.basename(src)

        if Path(dst).is_dir():
            if src_base:
                dst = Path(dst, src_base)
            else:
                if is_file_like:
                    raise ValueError(
                        "could not determine the file name and dst is a folder"
                    )

        if is_file_like:
            with open(dst, "wb") as f:
                f.write(src.read())
        else:
            self.copy(src, dst)

    def get(self, src, dst):
        is_file_like = hasattr(dst, "write") and callable(dst.write)

        if is_file_like:
            with open(src, "rb") as f:
                dst.write(f.read())
        else:
            self.copy(src, dst)

    def copy(self, src, dst):
        shutil.copy(src, dst)

    def listdir(self, path: str | Path) -> list[str]:
        try:
            return os.listdir(path)
        except FileNotFoundError:
            return []

    def remove(self, path: str | Path):
        os.remove(path)
