import io

import pytest


def _no_getpass(prompt):
    raise AssertionError("getpass must not be used in the runner daemon")


def test_inter_handler_in_daemon(monkeypatch):
    # The runner daemon has no terminal: getpass would warn that the input
    # may be echoed, so the answers are read from stdin with input().
    from jobflow_remote.remote.host.remote import inter_handler

    monkeypatch.setenv("SUPERVISOR_PROCESS_NAME", "run_jobflow0")
    monkeypatch.setattr("getpass.getpass", _no_getpass)
    monkeypatch.setattr("sys.stdin", io.StringIO("secret\n123456\n"))
    resp = inter_handler("", "", [("Password: ", False), ("OTP: ", False)])
    assert resp == ("secret", "123456")


@pytest.mark.parametrize("process_name", [None, "jupyterlab"])
def test_inter_handler_outside_daemon(monkeypatch, process_name):
    # Outside the runner daemon, including other supervisord programs,
    # hidden prompts still go through getpass.
    from jobflow_remote.remote.host.remote import inter_handler

    if process_name is None:
        monkeypatch.delenv("SUPERVISOR_PROCESS_NAME", raising=False)
    else:
        monkeypatch.setenv("SUPERVISOR_PROCESS_NAME", process_name)
    monkeypatch.setattr("getpass.getpass", lambda prompt: "hidden")
    monkeypatch.setattr("builtins.input", lambda prompt: "shown")
    resp = inter_handler("", "", [("Password: ", False), ("User: ", True)])
    assert resp == ("hidden", "shown")
