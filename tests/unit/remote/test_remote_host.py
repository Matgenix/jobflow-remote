import getpass
import io
import warnings

import pytest


def _no_input(prompt):
    raise AssertionError("hidden prompts must go through getpass")


def test_inter_handler_in_daemon(monkeypatch, capsys):
    # The runner daemon has no terminal, so getpass takes its fallback path,
    # which warns that the input may be echoed. Only the prompts are shown.
    from jobflow_remote.remote.host.remote import inter_handler

    monkeypatch.setenv("SUPERVISOR_PROCESS_NAME", "run_jobflow0")
    monkeypatch.setattr("getpass.getpass", getpass.fallback_getpass)
    monkeypatch.setattr("builtins.input", _no_input)
    monkeypatch.setattr("sys.stdin", io.StringIO("secret\n123456\n"))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        resp = inter_handler("", "", [("Password: ", False), ("OTP: ", False)])
    assert resp == ("secret", "123456")
    assert capsys.readouterr() == ("Password: OTP: ", "")


def test_close_moves_to_gateway_after_error(monkeypatch):
    # An error while closing a connection must not stop the loop over the
    # gateways, nor make it close the same connection forever.
    import fabric

    from jobflow_remote.remote.host.remote import RemoteHost

    class CloseCalledAgainError(BaseException):
        # a BaseException is not caught by close(): the test fails instead of
        # hanging if the same connection is closed twice.
        pass

    gateway = fabric.Connection("gateway.example.org")
    main = fabric.Connection("main.example.org", gateway=gateway)

    calls = []

    def failing_close():
        calls.append("main")
        if calls.count("main") > 1:
            raise CloseCalledAgainError
        raise OSError("Socket is closed")

    monkeypatch.setattr(main, "close", failing_close)
    monkeypatch.setattr(gateway, "close", lambda: calls.append("gateway"))

    host = RemoteHost(host="main.example.org")
    host._connection = main

    assert host.close() is False
    assert calls == ["main", "gateway"]


def test_interactive_login_not_allowed(monkeypatch):
    import fabric

    import jobflow_remote
    from jobflow_remote.remote.host.base import InteractiveLoginRequiredError
    from jobflow_remote.remote.host.remote import RemoteHost

    opened = []
    monkeypatch.setattr(
        fabric.Connection, "open", lambda self: opened.append(self.host)
    )

    interactive = RemoteHost(
        host="interactive.example.org", interactive_login=True, keepalive=None
    )
    other = RemoteHost(host="other.example.org", keepalive=None)

    monkeypatch.setattr(jobflow_remote.SETTINGS, "allow_interactive_login", False)
    with pytest.raises(InteractiveLoginRequiredError, match="interactive.example.org"):
        interactive.connect()
    # methods for which fabric would open the connection go through the check
    with pytest.raises(InteractiveLoginRequiredError) as exc_info:
        interactive.exists("/some/path")
    assert exc_info.value.host == "interactive.example.org"
    # no connection was opened, and hosts without interactive login connect
    other.connect()
    assert opened == ["other.example.org"]

    # with the default the interactive host connects as before
    monkeypatch.setattr(jobflow_remote.SETTINGS, "allow_interactive_login", True)
    interactive.connect()
    assert opened == ["other.example.org", "interactive.example.org"]
