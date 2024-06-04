def test_flows_list(job_controller, four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Flow id", "Num Jobs", "Last updated"]
    outputs = columns + [f"f{i}" for i in range(1, 3)] + ["READY"]

    run_check_cli(["flow", "list"], required_out=outputs)

    # the output table is squeezed. Hard to check the stdout. Just check that runs correctly
    run_check_cli(["flow", "list", "-v"])

    # trigger the additional information
    outputs = ["The number of Flows printed is limited by the maximum selected"]
    run_check_cli(["flow", "list", "-m", "1"], required_out=outputs)

    outputs = ["READY"]
    run_check_cli(["flow", "list", "-fid", four_jobs[0].uuid], required_out=outputs)


def test_delete(job_controller, four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[0].uuid],
        required_out="Deleted Flow",
        cli_input="y",
    )
    assert job_controller.count_flows() == 1

    # don't confirm
    run_check_cli(["flow", "delete", "-fid", four_jobs[1].uuid], cli_input="n")
    assert job_controller.count_flows() == 1

    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[0].uuid],
        required_out="No flows matching criteria",
    )


def test_flow_info(job_controller, four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker"]
    outputs = columns + [f"add{i}" for i in range(1, 3)] + ["READY", "WAITING"]
    excluded = [f"add{i}" for i in range(3, 5)]
    run_check_cli(
        ["flow", "info", "-j", "1"], required_out=outputs, excluded_out=excluded
    )
