import os.path
import re
import time

import pytest


def test_flows_list(job_controller, two_flows_four_jobs, run_check_cli) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    columns = ["DB id", "Name", "State", "Flow id", "Num Jobs", "Last updated"]
    outputs = columns + [f"f{i}" for i in range(1, 3)] + ["READY"]

    run_check_cli(["flow", "list"], required_out=outputs)
    run_check_cli(["flow", "list", "--count"], required_out="Number of Flows: 2")

    # the output table is squeezed. Hard to check stdout. Just check that runs correctly
    run_check_cli(["flow", "list", "-v"])

    # trigger the additional information
    outputs = ["The number of Flows printed is limited by the maximum selected"]
    run_check_cli(["flow", "list", "-m", "1"], required_out=outputs)

    outputs = ["READY"]
    run_check_cli(
        ["flow", "list", "-fid", two_flows_four_jobs[0].uuid], required_out=outputs
    )
    run_check_cli(
        ["flow", "list", "-fid", two_flows_four_jobs[0].uuid, "--count"],
        required_out="Number of Flows: 1",
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
    run_check_cli(
        ["flow", "list", "--metadata", "test=x", "--count"],
        required_out="Number of Flows: 1",
    )


def test_delete(job_controller, two_flows_four_jobs, run_check_cli) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    # run one of the jobs to check that the output is not deleted
    runner = Runner()
    job_1_uuid = two_flows_four_jobs[0].jobs[0].uuid
    runner.run_one_job(job_id=(job_1_uuid, 1))
    job_1_doc = job_controller.get_job_doc(job_id=job_1_uuid)
    assert job_1_doc.state == JobState.COMPLETED
    assert job_controller.jobstore.get_output(job_1_uuid) == 6

    assert os.path.isdir(job_1_doc.run_dir)

    # try deleting the Flow while locked. Should not succeed
    with job_controller.lock_flow(filter={"uuid": two_flows_four_jobs[0].uuid}):
        run_check_cli(
            ["flow", "delete", "-fid", two_flows_four_jobs[0].uuid],
            required_out=[
                "FlowLockedError",
                "Some of the selected Flows were not deleted",
            ],
            excluded_out="Deleted Flow",
            cli_input="y",
        )

    assert job_controller.count_flows() == 2
    assert job_controller.count_jobs() == 4

    run_check_cli(
        ["flow", "delete", "-fid", two_flows_four_jobs[0].uuid],
        required_out="Deleted Flow",
        excluded_out="Some of the selected Flows were not deleted",
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
        " Cannot delete 11 Flows as they exceed the specified maximum limit (10)"
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


def test_flow_info(job_controller, two_flows_four_jobs, run_check_cli) -> None:
    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker"]
    outputs = columns + [f"add{i}" for i in range(1, 3)] + ["READY", "WAITING"]
    excluded = [f"add{i}" for i in range(3, 5)] + ["{'f1_metadata': 'some_info'}"]
    res_flow_info = run_check_cli(
        ["flow", "info", "-j", "1", "--jobs-sort", "db_id"],
        required_out=outputs,
        excluded_out=excluded,
    )
    res_job_list = run_check_cli(["job", "list", "-fid", "1", "--sort", "db_id"])
    # Check that the job tables are the same between jf flow info and jf job list for the same flow
    table_flow_info = re.search(r"(┏[\s\S]+?┘)", res_flow_info.output).group(1)
    table_job_list = re.search(r"(┏[\s\S]+?┘)", res_job_list.output).group(1)
    assert table_flow_info == table_job_list

    res_flow_info_v = run_check_cli(
        ["flow", "info", "-j", "1", "-v", "--jobs-sort", "db_id"],
        required_out=["{'f1_metadata': 'some_info'}"],
    )
    res_job_list_v = run_check_cli(
        ["job", "list", "-fid", "1", "-v", "--sort", "db_id"]
    )
    table_flow_info_v = re.search(r"(┏[\s\S]+?┘)", res_flow_info_v.output).group(1)
    table_job_list_v = re.search(r"(┏[\s\S]+?┘)", res_job_list_v.output).group(1)
    assert table_flow_info_v == table_job_list_v

    res_flow_info_vv = run_check_cli(
        ["flow", "info", "-j", "1", "-vv", "--jobs-sort", "db_id"],
        required_out=["{'f1_metadata': 'some_info'}"],
    )
    res_job_list_vv = run_check_cli(
        ["job", "list", "-fid", "1", "-vv", "--sort", "db_id"]
    )
    table_flow_info_vv = re.search(r"(┏[\s\S]+?┘)", res_flow_info_vv.output).group(1)
    table_job_list_vv = re.search(r"(┏[\s\S]+?┘)", res_job_list_vv.output).group(1)
    assert table_flow_info_vv == table_job_list_vv

    res_flow_info_vvv = run_check_cli(
        ["flow", "info", "-j", "1", "-vvv", "--jobs-sort", "db_id"],
        required_out=["{'f1_metadata': 'some_info'}"],
    )
    res_job_list_vvv = run_check_cli(
        ["job", "list", "-fid", "1", "-vvv", "--sort", "db_id"]
    )
    table_flow_info_vvv = re.search(r"(┏[\s\S]+?┘)", res_flow_info_vvv.output).group(1)
    table_job_list_vvv = re.search(r"(┏[\s\S]+?┘)", res_job_list_vvv.output).group(1)
    assert table_flow_info_vvv == table_job_list_vvv

    run_check_cli(["flow", "info", "-j", "3", "-v"], required_out=["Metadata: {}"])


def test_report(job_controller, run_check_cli) -> None:
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add_sleep

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


def test_resume(job_controller, two_flows_four_jobs, run_check_cli) -> None:
    from jobflow_remote.jobs.state import FlowState, JobState

    job_controller.stop_job(db_id="1")
    job_controller.set_job_state(JobState.STOPPED, db_id="2")

    assert job_controller.get_flows_info(db_ids="1")[0].state == FlowState.STOPPED

    run_check_cli(
        ["flow", "resume", "1"],
        required_out="2 Job(s) resumed",
        excluded_out="The Flow was not fully resumed",
    )
    assert job_controller.get_job_info(db_id="1").state == JobState.READY
    assert job_controller.get_job_info(db_id="2").state == JobState.WAITING

    assert job_controller.get_flows_info(db_ids="1")[0].state == FlowState.READY


def test_set_store(job_controller, runner, one_job, run_check_cli):
    assert job_controller.get_flow_store(one_job.uuid) is None

    run_check_cli(
        ["flow", "set", "store", one_job.uuid, "other_jobstore"],
        required_out="Flow has been updated",
    )

    assert job_controller.get_flow_store(one_job.uuid) == "other_jobstore"

    run_check_cli(
        ["flow", "set", "store", "1"],
        required_out="Flow has been updated",
    )

    assert job_controller.get_flow_store(one_job.uuid) is None

    runner.run_one_job(db_id="1")

    run_check_cli(
        ["flow", "set", "store", "1", "other_jobstore"],
        required_out="The JobStore can be set only for a READY Flow",
        excluded_out="Flow has been updated",
        error=True,
    )

    assert job_controller.get_flow_store(one_job.uuid) is None


@pytest.mark.filterwarnings("ignore:Some jobs are not connected")
def test_clean(
    job_controller,
    two_flows_four_jobs,
    run_check_cli,
    daemon_manager,
    wait_daemon_started,
    wait_daemon_shutdown,
    tmp_dir,
) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add, add_sleep

    run_check_cli(
        ["flow", "delete", "-fid", "wrong_uuid"],
        required_out="No flows matching criteria",
        cli_input="y",
    )

    # run one of the jobs to check that the output is not deleted
    runner = Runner()
    runner.run_all_jobs(max_seconds=30)
    job_1_1_doc = job_controller.get_job_doc(job_id=two_flows_four_jobs[0].jobs[0].uuid)
    job_1_2_doc = job_controller.get_job_doc(job_id=two_flows_four_jobs[0].jobs[1].uuid)
    job_2_1_doc = job_controller.get_job_doc(job_id=two_flows_four_jobs[1].jobs[0].uuid)
    job_2_2_doc = job_controller.get_job_doc(job_id=two_flows_four_jobs[1].jobs[1].uuid)

    assert os.path.isdir(job_1_1_doc.run_dir)

    required_out_1 = [
        "This operation will delete the files of the following 1 Flow(s)",
        two_flows_four_jobs[0].uuid,
        "Deleted execution folders of 2 Jobs",
    ]
    run_check_cli(
        ["flow", "clean", "-v", "-fid", two_flows_four_jobs[0].uuid],
        required_out=required_out_1,
        cli_input="y",
    )
    assert job_controller.count_flows() == 2

    # check that the directory was deleted
    assert not os.path.isdir(job_1_1_doc.run_dir)
    assert not os.path.isdir(job_1_2_doc.run_dir)

    assert os.path.isdir(job_2_1_doc.run_dir)
    assert os.path.isdir(job_2_2_doc.run_dir)

    # don't confirm and no verbose option
    required_out_2 = [
        "This operation will delete the files of 1 Flow(s)",
    ]
    run_check_cli(
        ["flow", "clean", "-fid", two_flows_four_jobs[1].uuid],
        required_out=required_out_2,
        excluded_out=two_flows_four_jobs[1].uuid,
        cli_input="n",
    )
    assert os.path.isdir(job_2_1_doc.run_dir)
    assert os.path.isdir(job_2_2_doc.run_dir)

    os.unlink(os.path.join(job_2_2_doc.run_dir, "jfremote_in.json"))

    required_out_3 = [
        "Deleted execution folders of 1 Jobs",
        "Folder was not deleted for the following jobs:",
        f"- {job_2_2_doc.db_id}",
        "WARNING  Did not delete folder",
        "jfremote_in.json is missing",
    ]
    excluded_out = ["Proceed anyway"]
    run_check_cli(
        ["flow", "clean", "-fid", two_flows_four_jobs[1].uuid, "--yes"],
        required_out=required_out_3,
        excluded_out=excluded_out,
    )

    assert not os.path.isdir(job_2_1_doc.run_dir)
    assert os.path.isdir(job_2_2_doc.run_dir)

    # add one more Flow with a slow sleep
    slow_flow = Flow(add_sleep(2, 5))
    submit_flow(slow_flow, worker="test_local_worker")

    daemon_manager.start(raise_on_error=True)
    wait_daemon_started(daemon_manager)

    for _ in range(20):
        time.sleep(0.1)
        job_slow_doc = job_controller.get_job_doc(job_id=slow_flow.jobs[0].uuid)
        if job_slow_doc.state in (JobState.SUBMITTED, JobState.RUNNING):
            break
    else:
        raise RuntimeError(
            f"The slow job did not become RUNNING within the expected time. Final state: {job_slow_doc.state}"
        )
    run_check_cli(
        ["flow", "clean", "-fid", slow_flow.uuid, "--all-states"],
        required_out="The daemon should not be running while performing this operation",
        error=True,
    )
    assert os.path.isdir(job_slow_doc.run_dir)

    run_check_cli(
        ["flow", "clean", "-fid", slow_flow.uuid, "--all-states", "--force"],
        excluded_out="The daemon should not be running while performing this operation",
        required_out="Deleted execution folders of 1 Jobs",
        cli_input="y",
    )
    assert not os.path.isdir(job_slow_doc.run_dir)

    daemon_manager.shut_down(raise_on_error=True)
    wait_daemon_shutdown(daemon_manager)

    # add enough jobs to trigger
    running_fids = []
    for _ in range(3):
        add_jobs = [add(1, 2) for _ in range(4)]
        flow = Flow(add_jobs)
        running_fids.append(flow.uuid)
        submit_flow(flow, worker="test_local_worker")
        for j in add_jobs:
            assert job_controller.set_job_doc_properties(
                {
                    "state": JobState.RUNNING.value,
                    "run_dir": "/SOME/not/EXISTING/fake/PaTh",
                },
                job_id=j.uuid,
            )

    run_check_cli(
        sum((["-fid", fid] for fid in running_fids), start=["flow", "clean"]),
        required_out=[
            "The number of skipped jobs is too large to be printed",
            "Deleted execution folders of 0 Jobs",
        ],
        cli_input="y\ny",
    )
    assert os.path.isfile("skipped_cleanup.dat")
