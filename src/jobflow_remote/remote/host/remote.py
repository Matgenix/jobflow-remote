from __future__ import annotations

import io
from pathlib import Path

import fabric

from jobflow_remote.remote.host.base import BaseHost

# from fabric import Connection, Config


class RemoteHost(BaseHost):
    """
    Execute commands on a remote host.
    For some commands assumes the remote can run unix
    """

    def __init__(
        self,
        host,
        user=None,
        port=None,
        config=None,
        gateway=None,
        forward_agent=None,
        connect_timeout=None,
        connect_kwargs=None,
        inline_ssh_env=None,
        timeout_execute=None,
        keepalive=60,
    ):
        self.host = host
        self.user = user
        self.port = port
        self.config = config
        self.gateway = gateway
        self.forward_agent = forward_agent
        self.connect_timeout = connect_timeout
        self.connect_kwargs = connect_kwargs
        self.inline_ssh_env = inline_ssh_env
        self.timeout_execute = timeout_execute
        self.keepalive = keepalive
        self._connection = fabric.Connection(
            host=self.host,
            user=self.user,
            port=self.port,
            config=self.config,
            gateway=self.gateway,
            forward_agent=self.forward_agent,
            connect_timeout=self.connect_timeout,
            connect_kwargs=self.connect_kwargs,
            inline_ssh_env=self.inline_ssh_env,
        )

    @property
    def connection(self):
        return self._connection

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
            Command to execute, as a str or list of str.
        workdir: str or None
            path where the command will be executed.

        Returns
        -------
        stdout : str
            Standard output of the command
        stderr : str
            Standard error of the command
        exit_code : int
            Exit code of the command.
        """

        # TODO: check if this works:
        if not workdir:
            workdir = "."
        else:
            workdir = str(workdir)
        timeout = timeout or self.timeout_execute
        with self.connection.cd(workdir):
            out = self.connection.run(command, hide=True, warn=True, timeout=timeout)

        return out.stdout, out.stderr, out.exited

    def mkdir(self, directory, recursive: bool = True, exist_ok: bool = True) -> bool:
        """Create directory on the host."""
        command = "mkdir "
        if recursive:
            command += "-p "
        command += str(directory)
        try:
            stdout, stderr, returncode = self.execute(command)
            return returncode == 0
        except Exception:
            return False

    def write_text_file(self, filepath: str | Path, content: str):
        """Write content to a file on the host."""
        f = io.StringIO(content)

        self.connection.put(f, str(filepath))

    def connect(self):
        self.connection.open()
        if self.keepalive:
            self.connection.transport.set_keepalive(self.keepalive)

    def close(self) -> bool:
        try:
            self.connection.close()
        except Exception:
            return False
        return True

    @property
    def is_connected(self) -> bool:
        return self.connection.is_connected

    def put(self, src, dst):
        self.connection.put(src, dst)

    def get(self, src, dst):
        self.connection.get(src, dst)

    def copy(self, src, dst):
        cmd = ["cp", str(src), str(dst)]
        self.execute(cmd)
