import getpass
import io
import warnings


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
