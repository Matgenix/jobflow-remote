from __future__ import annotations

import re
from typing import IO, TYPE_CHECKING, Any

from typer.testing import CliRunner, Result

from jobflow_remote.cli.jf import app

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


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
    assert (
        error == (result.exit_code != 0)
    ), f"cli should have {'' if error else 'not '}failed. exit code: {result.exit_code}. stdout: {result.stdout}"

    # the print of the output in the console during the tests may result in newlines added
    # that prevent the output to be matched. replace all spaces with a single space.
    single_space_output = re.sub(r"[\n\t\s]*", " ", result.stdout)

    if required_out:
        for ro in required_out:
            assert (
                re.sub(r"[\n\t\s]*", " ", ro) in single_space_output
            ), f"{ro} missing from stdout: {result.stdout}"

    if excluded_out:
        for eo in excluded_out:
            assert (
                re.sub(r"[\n\t\s]*", " ", eo) not in single_space_output
            ), f"{eo} present in stdout: {result.stdout}"

    return result
