import os.path

import pytest


def test_flows_list(job_controller, two_flows_four_jobs) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Flow id", "Num Jobs", "Last updated"]
    outputs = columns + [f"f{i}" for i in range(1, 3)] + ["READY"]

    run_check_cli(["flow", "list"], required_out=outputs)

    # the output table is squeezed. Hard to check stdout. Just check that runs correctly
    run_check_cli(["flow", "list", "-v"])

    # trigger the additional information
    outputs = ["The number of Flows printed is limited by the maximum selected"]
    run_check_cli(["flow", "list", "-m", "1"], required_out=outputs)

    outputs = ["READY"]
    run_check_cli(
        ["flow", "list", "-fid", two_flows_four_jobs[0].uuid], required_out=outputs
    )

    # test metadata query
    j = add(1, 2)
    flow = Flow([j])
    flow.update_metadata({"test": "x"})
    submit_flow(flow, worker="test_local_worker")

    outputs = [flow.uuid[:5]]
    excluded = [two_flows_four_jobs[0].uuid[:5], two_flows_four_jobs[1].uuid[:5]]
    run_check_cli(
        ["flow", "list", "--metadata", "test=x"],
        required_out=outputs,
        excluded_out=excluded,
    )


def test_delete(job_controller, two_flows_four_jobs) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add
    from jobflow_remote.testing.cli import run_check_cli

    # run one of the jobs to check that the output is not deleted
    runner = Runner()
    job_1_uuid = two_flows_four_jobs[0].jobs[0].uuid
    runner.run_one_job(job_id=(job_1_uuid, 1))
    job_1_doc = job_controller.get_job_doc(job_id=job_1_uuid)
    assert job_1_doc.state == JobState.COMPLETED
    assert job_controller.jobstore.get_output(job_1_uuid) == 6

    assert os.path.isdir(job_1_doc.run_dir)

    run_check_cli(
        ["flow", "delete", "-fid", two_flows_four_jobs[0].uuid],
        required_out="Deleted Flow",
        cli_input="y",
    )
    assert job_controller.count_flows() == 1
    assert job_controller.jobstore.get_output(job_1_uuid) == 6

    # check that the directory was not deleted
    assert os.path.isdir(job_1_doc.run_dir)

    # run the command without returning any match
    run_check_cli(
        ["flow", "delete", "-fid", two_flows_four_jobs[0].uuid],
        required_out="No flows matching criteria",
    )

    # don't confirm and verbose option
    # only check the first characters of the uuid because it may be cut in the output
    outputs = [
        "This operation will delete the following 1 Flow",
        two_flows_four_jobs[1].uuid[:5],
    ]
    run_check_cli(
        ["flow", "delete", "-fid", two_flows_four_jobs[1].uuid, "-v"],
        required_out=outputs,
        cli_input="n",
    )
    assert job_controller.count_flows() == 1

    # run all the remaining jobs and delete with the outputs
    runner = Runner()
    job_2_uuid = two_flows_four_jobs[1].jobs[0].uuid
    runner.run_all_jobs()
    job_3_doc = job_controller.get_job_doc(job_id=job_2_uuid)
    job_4_doc = job_controller.get_job_doc(job_id=two_flows_four_jobs[1].jobs[1].uuid)
    assert job_3_doc.state == JobState.COMPLETED
    assert job_controller.jobstore.get_output(job_2_uuid) == 6

    # remove the jfremote_in.json file from one of the folders, so it will
    # not be deleted
    os.remove(os.path.join(job_4_doc.run_dir, "jfremote_in.json"))

    outputs = [f"Deleted Flow(s) with id: {two_flows_four_jobs[1].uuid}"]
    run_check_cli(
        ["flow", "delete", "-fid", two_flows_four_jobs[1].uuid, "-a"],
        required_out=outputs,
        cli_input="y",
    )
    assert not os.path.isdir(job_3_doc.run_dir)
    assert os.path.isdir(job_4_doc.run_dir)
    assert job_controller.count_flows() == 0

    # output should be deleted
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(job_2_uuid)

    assert not os.path.isdir(job_3_doc.run_dir)
    assert os.path.isdir(job_4_doc.run_dir)

    # create more than 10 flows and delete them
    for i in range(11):
        j1 = add(i, 1)
        flow = Flow([j1])
        submit_flow(flow, worker="test_local_worker")

    outputs = [
        " Cannot delete 11 Flows as they exceeds the specified maximum limit (10)"
    ]
    done_output = ["Deleted Flow(s) with id"]
    # try deleting the flow with max=10. It fails
    run_check_cli(
        ["flow", "delete"],
        required_out=outputs,
        excluded_out=done_output,
        cli_input="y",
        error=True,
    )

    # increase the maximum value to succeed
    run_check_cli(
        ["flow", "delete", "-m", "20"],
        required_out=done_output,
        cli_input="y",
    )


def test_flow_info(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker"]
    outputs = columns + [f"add{i}" for i in range(1, 3)] + ["READY", "WAITING"]
    excluded = [f"add{i}" for i in range(3, 5)]
    run_check_cli(
        ["flow", "info", "-j", "1"], required_out=outputs, excluded_out=excluded
    )


def test_report(job_controller) -> None:
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add_sleep
    from jobflow_remote.testing.cli import run_check_cli

    # run first with an empty db to check that everything works fine
    now = datetime.now()
    output = [
        "Flow Summary",
        "Flow State Distribution",
        "Flow Trends",
        now.strftime("%Y-%m-%d"),
    ]
    run_check_cli(
        ["flow", "report", "days", "2"],
        required_out=[*output, "Running Flows │   0"],
    )

    # a long sleeping job. Will not finish.
    j = add_sleep(1, 10)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    run_check_cli(
        ["flow", "report", "days", "2"],
        required_out=[*output, "Running Flows │   0"],
    )
