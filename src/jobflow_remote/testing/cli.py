from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import IO, Any

from typer.testing import CliRunner, Result

from jobflow_remote.cli.jf import app


def run_check_cli(
    cli_args: str | Sequence[str] | None = None,
    cli_input: bytes | str | IO[Any] | None = None,
    cli_env: Mapping[str, str] | None = None,
    catch_exceptions: bool = False,
    required_out: str | Sequence[str] | None = None,
    excluded_out: str | Sequence[str] | None = None,
    error: bool = False,
    terminal_width: int = 1000,
) -> Result:
    if isinstance(required_out, str):
        required_out = [required_out]
    if isinstance(excluded_out, str):
        excluded_out = [excluded_out]

    cli_runner = CliRunner()

    result = cli_runner.invoke(
        app,
        args=cli_args,
        input=cli_input,
        env=cli_env,
        catch_exceptions=catch_exceptions,
        terminal_width=terminal_width,
    )

    # note that stderr is not captured separately
    assert error == (
        result.exit_code != 0
    ), f"cli should have {'not ' if not error else ''}failed. exit code: {result.exit_code}. stdout: {result.stdout}"

    if required_out:
        for ro in required_out:
            assert ro in result.stdout, f"{ro} missing from stdout: {result.stdout}"

    if excluded_out:
        for eo in excluded_out:
            assert eo not in result.stdout, f"{eo} present in stdout: {result.stdout}"

    return result
