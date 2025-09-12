import pytest
from typer.testing import Result


def test_run_check_cli(run_check_cli):
    # Check multiple lines is found
    flow_excerpt = """├── flow: Commands for managing the flows
│   ├── delete: Permanently delete Flows from the database
│   ├── graph: Provide detailed information on a Flow.
│   ├── info: Provide detailed information on a Flow.
│   ├── list: Get the list of Flows in the database."""
    res = run_check_cli(
        ["--tree"], required_out=["flow: Commands for managing the flows", flow_excerpt]
    )
    assert isinstance(res, Result)
    assert res.exit_code == 0

    # Check exclusion
    with pytest.raises(
        AssertionError, match=r"flow: Commands for managing the flows present in stdout"
    ):
        run_check_cli(
            ["--tree"], excluded_out=["flow: Commands for managing the flows"]
        )

    # Check with some additional spaces
    with pytest.raises(
        AssertionError,
        match=r"flow:   Commands for managing the \nflows present in stdout",
    ):
        run_check_cli(
            ["--tree"], excluded_out=["flow:   Commands for managing the \nflows"]
        )

    run_check_cli(["--tree"], required_out_colored="[bold red]jf[/bold red]")

    with pytest.raises(AssertionError):
        run_check_cli(["--tree"], required_out_colored="[green]jf[/green]")

    run_check_cli(
        ["--tree"],
        required_out_colored="[bold green]job[/bold green]: Commands for   managing the jobs",
    )

    with pytest.raises(AssertionError):
        run_check_cli(
            ["--tree"], required_out_colored="job: Commands for managing the jobs"
        )

    with pytest.raises(AssertionError):
        run_check_cli(
            ["--tree"],
            required_out_colored="[bold green]job[/bold green]: Commands [red]for[/red] managing the jobs",
        )
