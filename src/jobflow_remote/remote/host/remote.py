from __future__ import annotations

import io
import logging
import shlex
import traceback
from pathlib import Path

import fabric
from paramiko.ssh_exception import SSHException

from jobflow_remote.remote.host.base import BaseHost

logger = logging.getLogger(__name__)


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
        shell_cmd="bash",
        login_shell=True,
        retry_on_closed_connection=True,
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
        self.shell_cmd = shell_cmd
        self.login_shell = login_shell
        self.retry_on_closed_connection = retry_on_closed_connection
        self._create_connection()

    def _create_connection(self):
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

    def __eq__(self, other):
        if not isinstance(other, RemoteHost):
            return False
        return self.as_dict() == other.as_dict()

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
        self._check_connected()

        if isinstance(command, (list, tuple)):
            command = " ".join(command)

        # TODO: check if this works:
        if not workdir:
            workdir = "."

        workdir = Path(workdir)

        timeout = timeout or self.timeout_execute

        if self.shell_cmd:
            shell_cmd = self.shell_cmd
            if self.login_shell:
                shell_cmd += " -l "
            shell_cmd += " -c "
            remote_command = shell_cmd + shlex.quote(command)
        else:
            remote_command = command

        with self.connection.cd(workdir):
            out = self._execute_remote_func(
                self.connection.run,
                remote_command,
                hide=True,
                warn=True,
                timeout=timeout,
            )

        return out.stdout, out.stderr, out.exited

    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """Create directory on the host."""
        directory = Path(directory)
        command = f"mkdir {'-p ' if recursive else ''}{str(directory)!r}"
        try:
            stdout, stderr, returncode = self.execute(command)
            if returncode != 0:
                logger.warning(
                    f"Error creating folder {directory}. stdout: {stdout}, stderr: {stderr}"
                )
            return returncode == 0
        except Exception:
            logger.warning(f"Error creating folder {directory}", exc_info=True)
            return False

    def write_text_file(self, filepath: str | Path, content: str):
        """Write content to a file on the host."""
        self._check_connected()

        f = io.StringIO(content)

        self._execute_remote_func(self.connection.put, f, str(filepath))

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
        self._check_connected()

        self._execute_remote_func(self.connection.put, src, dst)

    def get(self, src, dst):
        self._check_connected()

        self._execute_remote_func(self.connection.get, src, dst)

    def copy(self, src, dst):
        cmd = ["cp", str(src), str(dst)]
        self.execute(cmd)

    def _execute_remote_func(self, remote_cmd, *args, **kwargs):
        if self.retry_on_closed_connection:
            try:
                return remote_cmd(*args, **kwargs)
            except OSError as e:
                msg = getattr(e, "message", str(e))
                error = e
                if "Socket is closed" not in msg:
                    raise e
            except SSHException as e:
                error = e
                msg = getattr(e, "message", str(e))
                if "Server connection dropped" not in msg:
                    raise e
            except EOFError as e:
                error = e
        else:
            return remote_cmd(*args, **kwargs)

        # if the code gets here one of the errors that could be due to drop of the
        # connection occurred. Try to close and reopen the connection and retry
        # one more time
        logger.warning(
            f"Error while trying to execute a command on host {self.host}:\n"
            f"{''.join(traceback.format_exception(error))}"
            "Probably due to the connection dropping. "
            "Will reopen the connection and retry."
        )
        try:
            self.connection.close()
        except Exception:
            logger.warning(
                "Error while closing the connection during a retry. "
                "Proceeding with the retry.",
                exc_info=True,
            )
        self._create_connection()
        self.connect()
        return remote_cmd(*args, **kwargs)

    def listdir(self, path: str | Path):
        self._check_connected()

        try:
            return self._execute_remote_func(self.connection.sftp().listdir, str(path))
        except FileNotFoundError:
            return []

    def remove(self, path: str | Path):
        self._check_connected()

        self._execute_remote_func(self.connection.sftp().remove, str(path))

    def _check_connected(self) -> bool:
        """
        Helper method to determine if fabric consider the connection open and
        open it otherwise

        Since many operations requiring connections happen in the runner,
        if the connection drops there are cases where the host may not be
        reconnected. To avoid this issue always try to reconnect automatically
        if the connection is not open.

        Returns
        -------
        True if the connection is open.
        """

        if not self.is_connected:
            # Note: raising here instead of reconnecting demonstrated to be a
            # problem for how the queue managers are handled in the Runner.
            self.connect()
        return True
