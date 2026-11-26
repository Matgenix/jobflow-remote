import socket

import pytest
from paramiko.ssh_exception import (
    AuthenticationException,
    NoValidConnectionsError,
    SSHException,
)


@pytest.fixture()
def local_project(tmp_path):
    from jobflow_remote.config.base import LocalWorker, Project

    return Project(
        name="test_shared_hosts",
        base_dir=str(tmp_path),
        queue={},
        workers={
            "local_w": LocalWorker(scheduler_type="shell", work_dir=str(tmp_path))
        },
    )


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (TimeoutError("timed out"), "the host did not answer in time"),
        (
            ConnectionRefusedError(61, "Connection refused"),
            "the host refused the connection",
        ),
        (
            socket.gaierror(8, "nodename nor servname provided, or not known"),
            "the host name could not be resolved",
        ),
        (
            NoValidConnectionsError(
                {("10.0.0.1", 22): ConnectionRefusedError(61, "Connection refused")}
            ),
            "the host refused the connection or could not be reached",
        ),
        (
            AuthenticationException("Authentication failed."),
            "the authentication failed",
        ),
        (
            SSHException("Error reading SSH protocol banner"),
            "the SSH connection failed",
        ),
        (RuntimeError("unexpected"), "the connection failed"),
    ],
)
def test_get_host_connection_error(local_project, monkeypatch, error, reason) -> None:
    from jobflow_remote.remote.host.base import HostConnectionError, HostError
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.utils.remote import SharedHosts

    calls = []

    def failing_connect(self) -> None:
        calls.append(self)
        raise error

    monkeypatch.setattr(LocalHost, "connect", failing_connect)

    with SharedHosts(local_project) as shared_hosts:
        with pytest.raises(HostConnectionError) as exc_info:
            shared_hosts.get_host("local_w")
        # the host that failed is not kept: a later call connects again
        with pytest.raises(HostConnectionError):
            shared_hosts.get_host("local_w")
        assert not shared_hosts._hosts
    assert len(calls) == 2

    exc = exc_info.value
    assert isinstance(exc, HostError)
    assert exc.worker == "local_w"
    assert exc.__cause__ is error
    assert str(exc) == (
        f"Could not connect to the worker 'local_w': {reason} "
        f"({type(error).__name__}: {error})"
    )


def test_get_host_host_error_not_wrapped(local_project, monkeypatch) -> None:
    from jobflow_remote.remote.host.base import HostError
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.utils.remote import SharedHosts

    class CustomHostError(HostError):
        pass

    error = CustomHostError("a host error with its own message")

    def failing_connect(self) -> None:
        raise error

    monkeypatch.setattr(LocalHost, "connect", failing_connect)

    with (
        SharedHosts(local_project) as shared_hosts,
        pytest.raises(CustomHostError) as exc_info,
    ):
        shared_hosts.get_host("local_w")

    assert exc_info.value is error


def test_get_host_connects_once(local_project, monkeypatch) -> None:
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.utils.remote import SharedHosts

    calls = []
    monkeypatch.setattr(LocalHost, "connect", lambda self: calls.append(self))

    with SharedHosts(local_project) as shared_hosts:
        host1 = shared_hosts.get_host("local_w")
        host2 = shared_hosts.get_host("local_w")

    assert host1 is host2
    assert calls == [host1]
