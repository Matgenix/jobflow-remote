from unittest.mock import MagicMock, patch

import pytest


@patch("fabric.Connection.run")
@patch("fabric.Connection.cd")
def test_sanitize(mock_cd, mock_run):
    from jobflow_remote.remote.host.base import SANITIZE_KEY
    from jobflow_remote.remote.host.remote import RemoteHost

    rh = RemoteHost(
        host="localhost",
        retry_on_closed_connection=False,
        sanitize=True,
        shell_cmd=None,
    )
    rh._check_connected = lambda: True

    cmd = "echo 'test'"

    echo_cmd = f'echo "{SANITIZE_KEY}" | tee /dev/stderr'
    expected_cmd = f"{echo_cmd};{cmd};{echo_cmd}"
    mock_stdout = f"SOME NOISE --{SANITIZE_KEY}\ntest{SANITIZE_KEY}\nSOME appended TEXT"

    # Configure the mock
    mock_cd.return_value.__enter__ = (
        MagicMock()
    )  # This makes the context manager do nothing
    mock_cd.return_value.__exit__ = MagicMock()
    mock_run.return_value.stdout = mock_stdout
    mock_run.return_value.stderr = ""

    # Call the function that uses subprocess.run
    stdout, stderr, _ = rh.execute(cmd)

    # Assert that subprocess.run was called with the expected arguments
    mock_run.assert_called_once_with(expected_cmd, timeout=None, hide=True, warn=True)

    # Assert on the result of your function
    assert stdout == "test"
    assert stderr == ""


def test_build_ssh_command():
    from fabric import Connection

    from jobflow_remote.remote.host.remote import build_ssh_command

    c = Connection("a", gateway=Connection("b", gateway=Connection("c")))
    with pytest.raises(NotImplementedError):
        build_ssh_command(c)

    c = Connection(host="host", user="name")
    assert build_ssh_command(c) == "ssh -t -p 22 name@host"

    c = Connection(host="host", user="name", connect_kwargs={"password": "test"})
    assert build_ssh_command(c) == "sshpass -p test ssh -t -p 22 name@host"

    c = Connection(host="host", user="name", forward_agent=True, connect_timeout=30)
    assert build_ssh_command(c) == "ssh -t -A -p 22 -o ConnectTimeout=30 name@host"

    c = Connection(host="host", user="name", gateway=Connection("host2", user="name2"))
    assert (
        build_ssh_command(c)
        == "ssh -t -o ProxyCommand='ssh -t -p 22 name2@host2 -W %h:%p' -p 22 name@host"
    )

    c = Connection(host="host", user="name", gateway="ssh name2@host2")
    assert (
        build_ssh_command(c)
        == "ssh -t -o ProxyCommand='ssh name2@host2' -p 22 name@host"
    )

    c = Connection(host="host", user="name", gateway="host2")
    assert build_ssh_command(c) == "ssh -t -J host2 -p 22 name@host"

    c = Connection(
        host="host", user="name", connect_kwargs={"key_filename": "/path/to/key"}
    )
    assert build_ssh_command(c) == "ssh -t -p 22 -i /path/to/key name@host"

    c = Connection(
        host="host",
        user="name",
        connect_kwargs={"look_for_keys": False, "allow_agent": True},
    )
    assert (
        build_ssh_command(c)
        == "ssh -t -p 22 -o PubkeyAuthentication=no -o IdentityAgent=yes name@host"
    )

    c = Connection(
        host="host", user="name", connect_kwargs={"compress": True, "keepalive": True}
    )
    assert (
        build_ssh_command(c) == "ssh -t -p 22 -C -o ServerAliveInterval=True name@host"
    )

    c = Connection(host="host", user="name", connect_kwargs={"passphrase": "pass"})
    with pytest.warns(
        match="passphrase argument from the configuration will be ignored"
    ):
        assert build_ssh_command(c) == "ssh -t -p 22 name@host"
