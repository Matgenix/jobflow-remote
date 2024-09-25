from unittest.mock import patch


@patch("subprocess.run")
def test_sanitize(mock_run):
    from jobflow_remote.remote.host.base import SANITIZE_KEY
    from jobflow_remote.remote.host.local import LocalHost

    lh = LocalHost(sanitize=True)

    cmd = "echo 'test'"

    echo_cmd = f'echo -n "{SANITIZE_KEY}" | tee >(cat >&2)'
    expected_cmd = f"{echo_cmd};{cmd};{echo_cmd}"
    mock_stdout = f"SOME NOISE --{SANITIZE_KEY}test{SANITIZE_KEY}SOME appended TEXT"

    # Configure the mock
    mock_run.return_value.returncode = 0
    mock_run.return_value.stdout = mock_stdout.encode()
    mock_run.return_value.stderr = b""

    stdout, stderr, _ = lh.execute(cmd)

    mock_run.assert_called_once_with(
        expected_cmd,
        capture_output=True,
        shell=True,  # noqa: S604
        timeout=None,
        check=False,
    )

    assert stdout == "test"
    assert stderr == ""
