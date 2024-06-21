import pytest


def test_flows_list(job_controller, four_jobs):
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


def test_delete(job_controller, four_jobs):
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    # run one of the jobs to check that the output is not deleted
    runner = Runner()
    job_1_uuid = four_jobs[0].jobs[0].uuid
    runner.run_one_job(job_id=(job_1_uuid, 1))
    assert job_controller.get_job_doc(job_id=job_1_uuid).state == JobState.COMPLETED
    assert job_controller.jobstore.get_output(job_1_uuid) == 6

    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[0].uuid],
        required_out="Deleted Flow",
        cli_input="y",
    )
    assert job_controller.count_flows() == 1
    assert job_controller.jobstore.get_output(job_1_uuid) == 6

    # run the command without returning any match
    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[0].uuid],
        required_out="No flows matching criteria",
    )

    # don't confirm and verbose option
    # only check the first characters of the uuid because it may be cut in the output
    outputs = ["This operation will delete the following 1 Flow", four_jobs[1].uuid[:5]]
    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[1].uuid, "-v"],
        required_out=outputs,
        cli_input="n",
    )
    assert job_controller.count_flows() == 1

    # run one job and delete with the outputs
    runner = Runner()
    job_2_uuid = four_jobs[1].jobs[0].uuid
    runner.run_one_job(job_id=(job_2_uuid, 1))
    assert job_controller.get_job_doc(job_id=job_2_uuid).state == JobState.COMPLETED
    assert job_controller.jobstore.get_output(job_2_uuid) == 6

    outputs = [f"Deleted Flow(s) with id: {four_jobs[1].uuid}"]
    run_check_cli(
        ["flow", "delete", "-fid", four_jobs[1].uuid, "-o"],
        required_out=outputs,
        cli_input="y",
    )
    assert job_controller.count_flows() == 0

    # output should be deleted
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(job_2_uuid)


def test_flow_info(job_controller, four_jobs):
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker"]
    outputs = columns + [f"add{i}" for i in range(1, 3)] + ["READY", "WAITING"]
    excluded = [f"add{i}" for i in range(3, 5)]
    run_check_cli(
        ["flow", "info", "-j", "1"], required_out=outputs, excluded_out=excluded
    )
