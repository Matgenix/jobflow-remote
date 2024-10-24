from unittest.mock import MagicMock, patch


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
