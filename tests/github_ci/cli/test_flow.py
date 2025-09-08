import pytest


def test_flow_info(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker"]
    outputs = columns + [f"add{i}" for i in range(1, 3)] + ["READY", "WAITING"]
    excluded = [f"add{i}" for i in range(3, 5)]
    run_check_cli(
        ["flow", "info", "-j", "1"], required_out=outputs, excluded_out=excluded
    )

    pytest.fail("Explicitly failing this test for testing CI save db artifact.")
