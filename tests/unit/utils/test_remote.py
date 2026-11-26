import re
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


def test_shared_hosts_per_thread(tmp_path, monkeypatch) -> None:
    """
    Two threads can open SharedHosts for different projects at the same time:
    each gets its own hosts and closes only its own.
    """
    import threading

    from jobflow_remote.config.base import LocalWorker, Project
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.utils.remote import SharedHosts

    projects = [
        Project(
            name=f"test_shared_hosts_{i}",
            base_dir=str(tmp_path / str(i)),
            queue={},
            workers={"w": LocalWorker(scheduler_type="shell", work_dir=str(tmp_path))},
        )
        for i in range(2)
    ]

    closed = []

    def recording_close(self) -> bool:
        closed.append((threading.current_thread().name, self))
        return True

    monkeypatch.setattr(LocalHost, "close", recording_close)

    barrier = threading.Barrier(2, timeout=10)
    results: dict[str, tuple] = {}
    errors = []

    def use_hosts(project) -> None:
        try:
            with SharedHosts(project) as shared_hosts:
                host = shared_hosts.get_host("w")
                # both threads are inside a context, for different projects
                barrier.wait()
                results[threading.current_thread().name] = (
                    host,
                    shared_hosts._project.name,
                    dict(shared_hosts._hosts),
                )
                # do not leave the context before the other thread has checked
                barrier.wait()
        except BaseException as exc:
            errors.append(exc)
            barrier.abort()

    threads = [
        threading.Thread(target=use_hosts, args=(p,), name=f"t{i}")
        for i, p in enumerate(projects)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    host_0, project_0, hosts_0 = results["t0"]
    host_1, project_1, hosts_1 = results["t1"]
    assert host_0 is not host_1
    assert (project_0, project_1) == (projects[0].name, projects[1].name)
    assert hosts_0 == {"w": host_0}
    assert hosts_1 == {"w": host_1}
    # each thread closed only its own host
    assert sorted(closed, key=lambda c: c[0]) == [("t0", host_0), ("t1", host_1)]


def test_get_host_requires_context_in_same_thread(local_project) -> None:
    import threading

    from jobflow_remote.utils.remote import SharedHosts

    match = r"inside a 'with SharedHosts\(\.\.\.\)' block opened in the same thread"

    # no context open
    with pytest.raises(RuntimeError, match=match):
        SharedHosts(local_project).get_host("local_w")

    # a context open in another thread
    errors = []

    def use_from_other_thread(shared_hosts) -> None:
        try:
            shared_hosts.get_host("local_w")
        except RuntimeError as exc:
            errors.append(exc)

    with SharedHosts(local_project) as shared_hosts:
        thread = threading.Thread(target=use_from_other_thread, args=(shared_hosts,))
        thread.start()
        thread.join()

    assert len(errors) == 1
    assert re.search(match, str(errors[0]))
