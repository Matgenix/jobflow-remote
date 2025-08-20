from __future__ import annotations

import io
import re
from typing import IO, TYPE_CHECKING, Any

from rich.console import Console
from rich.text import Text
from typer.testing import CliRunner, Result

from jobflow_remote.cli.jf import app

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


ansi_esc_pattern = re.compile(r"\x1B\[\d+(;\d+){0,2}m")


def run_check_cli(
    cli_args: str | Sequence[str] | None = None,
    cli_input: bytes | str | IO[Any] | None = None,
    cli_env: Mapping[str, str] | None = None,
    catch_exceptions: bool = False,
    required_out: str | Sequence[str] | None = None,
    excluded_out: str | Sequence[str] | None = None,
    required_out_colored: str | Sequence[str] | None = None,
    error: bool = False,
    terminal_width: int = 1000,
) -> Result:
    if isinstance(required_out, str):
        required_out = [required_out]
    if isinstance(excluded_out, str):
        excluded_out = [excluded_out]
    if isinstance(required_out_colored, str):
        required_out_colored = [required_out_colored]

    cli_runner = CliRunner()

    result = cli_runner.invoke(
        app,
        args=cli_args,
        input=cli_input,
        env=cli_env,
        catch_exceptions=catch_exceptions,
        terminal_width=terminal_width,
        color=True,
    )

    # Since typer 0.16.0 and click 8.2 the stderr is necessarily separated from the output.
    # To keep backward compatibility with previous click versions handle both defaults cases.
    try:
        result_out = result.stdout + "\n" + result.stderr
    except ValueError:
        result_out = result.stdout

    # note that stderr is not captured separately
    assert (
        error == (result.exit_code != 0)
    ), f"cli should have {'' if error else 'not '}failed. exit code: {result.exit_code}. stdout: {result_out}"

    # The print of the output in the console during the tests may result in newlines added
    # that prevent the output to be matched. replace all spaces with a single space.
    single_space_output = re.sub(r"\s+", " ", result_out)
    # Remove ansi escape codes
    single_space_output_no_ansi_esc = ansi_esc_pattern.sub("", single_space_output)
    # Remove again multiple spaces. This is needed otherwise we can sometimes (??) have 2 spaces
    # when ansi escape codes have been removed)
    single_space_output_no_ansi_esc = re.sub(
        r"\s+", " ", single_space_output_no_ansi_esc
    )

    if required_out:
        for ro in required_out:
            assert re.sub(r"\s+", " ", ro) in repr(
                single_space_output_no_ansi_esc
            ), f"{ro} missing from stdout: {result_out}"

    if excluded_out:
        for eo in excluded_out:
            assert (
                re.sub(r"\s+", " ", eo) not in single_space_output_no_ansi_esc
            ), f"{eo} present in stdout: {result_out}"

    if required_out_colored:
        for roc in required_out_colored:
            roc_text = Text.from_markup(roc)
            buf = io.StringIO()
            console = Console(file=buf, force_terminal=True, color_system="truecolor")
            console.print(roc_text, end="")
            roc_string = buf.getvalue()
            assert (
                re.sub(r"\s+", " ", roc_string) in single_space_output
            ), f'"{roc_string}" ({roc}) missing from stdout: {result_out}'

    return result
