import os


def test_jobs_list(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    # split "job id" from "index", because it can be sent to a new line
    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker", "Last updated"]
    outputs = columns + [f"add{i}" for i in range(1, 5)] + ["READY", "WAITING"]

    run_check_cli(["job", "list"], required_out=outputs)

    # the output table is squeezed. Hard to check stdout. Just check that runs correctly
    run_check_cli(["job", "list", "-v"])
    run_check_cli(["job", "list", "-vvv"])

    outputs = ["add1", "READY"]
    excluded = [f"add{i}" for i in range(2, 5)]
    run_check_cli(
        ["job", "list", "-did", "1"], required_out=outputs, excluded_out=excluded
    )
    run_check_cli(
        ["job", "list", "-q", '{"db_id": "1"}'],
        required_out=outputs,
        excluded_out=excluded,
    )

    # trigger the additional information
    assert job_controller.set_job_state(JobState.REMOTE_ERROR, db_id="1")
    run_check_cli(
        ["job", "list"], required_out=["Get more information about the errors"]
    )

    outputs = ["WAITING", "READY", "State", "DB id", "whatever"]
    excluded = ["add1", "add2", "Name", "Job id"]
    run_check_cli(
        ["job", "list", "-o", "state,db_id", "-sdk", "whatever"],
        required_out=outputs,
        excluded_out=excluded,
    )

    output = "Header keys not supported: {'not_existing_key'}"
    run_check_cli(
        ["job", "list", "-o", "state,not_existing_key"], required_out=output, error=True
    )

    output = "Options output, verbosity are incompatible"
    run_check_cli(
        ["job", "list", "-o", "state,name", "-vv"], required_out=output, error=True
    )


def test_jobs_list_settings(job_controller, two_flows_four_jobs, monkeypatch) -> None:
    from jobflow_remote import SETTINGS
    from jobflow_remote.testing.cli import run_check_cli

    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "cli_job_list_columns", ["state", "db_id"])

        outputs = ["WAITING", "READY", "State", "DB id"]
        excluded = ["add1", "add2", "Name", "Job id"]
        run_check_cli(
            ["job", "list"],
            required_out=outputs,
            excluded_out=excluded,
        )

        # using -v will lead to a very large table which isn't fully displayed
        columns = ["DB", "id", "Name", "Sta", "Job", "id", "Wor", "Last", "Loc"]
        outputs = columns + [f"add{i}" for i in range(1, 5)] + ["REA", "WAI"]
        run_check_cli(
            ["job", "list", "-v"],
            required_out=outputs,
        )


def test_job_info(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    outputs = ["name = 'add1'", "state = 'READY'"]
    excluded_n = ["run_dir = None", "start_time = None"]
    excluded = [*excluded_n, "job = {"]
    run_check_cli(["job", "info", "1"], required_out=outputs, excluded_out=excluded)

    outputs += excluded_n
    run_check_cli(
        ["job", "info", two_flows_four_jobs[0].jobs[0].uuid, "-n"], required_out=outputs
    )

    outputs = ["state = 'READY'", "job = {"]
    run_check_cli(["job", "info", "1", "-vvv"], required_out=outputs)

    run_check_cli(
        ["job", "info", "10"], error=True, required_out="No data matching the request"
    )


def test_job_info_by_pid(job_controller, two_flows_four_jobs):
    from jobflow_remote.testing.cli import run_check_cli

    # Set a known process ID for one of the jobs
    test_pid = 12345
    job_controller.jobs.update_one(
        {"db_id": "1"}, {"$set": {"remote.process_id": str(test_pid)}}
    )

    # Test successful retrieval of job info by process ID
    outputs = ["name = 'add1'", "state = 'READY'", f"'process_id': '{test_pid}'"]
    run_check_cli(["job", "info", "--pid", str(test_pid)], required_out=outputs)

    # Test with invalid process ID
    invalid_pid = 99999
    run_check_cli(
        ["job", "info", "--pid", str(invalid_pid)],
        error=True,
        required_out="No data matching the request",
    )

    # Test that we can't use --pid with other job identification options
    run_check_cli(
        ["job", "info", "1", "--pid", str(test_pid)],
        error=True,
        required_out="Cannot specify both job ID/index and process ID",
    )


def test_set_state(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set-state", "UPLOADED", "1"], required_out="operation completed"
    )
    assert job_controller.set_job_state(JobState.UPLOADED, db_id="1")
    run_check_cli(
        ["job", "set-state", "UPLOADED", "10"],
        required_out="Could not change the job state",
        error=True,
    )


def test_rerun(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    runner = Runner()
    runner.run_one_job(db_id="1")

    job_info = job_controller.get_job_info(db_id="1")
    assert job_info.state == JobState.COMPLETED
    assert len(os.listdir(job_info.run_dir)) > 0

    # rerun without deleting files
    run_check_cli(
        ["job", "rerun", "-did", "1", "-f", "-nd"],
        required_out="Operation completed: 2 jobs modified",
    )

    assert len(os.listdir(job_info.run_dir)) > 0
    assert job_controller.get_job_info(db_id="1").state == JobState.READY

    # fails because already READY
    run_check_cli(["job", "rerun", "-did", "1"], required_out="Error while rerunning")

    # set the job back to completed to try deleting the files as well
    assert job_controller.set_job_state(JobState.COMPLETED, db_id="1")

    # note: here only 1 job is modified, since the child was not set to READY
    run_check_cli(
        ["job", "rerun", "-did", "1", "-f"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert os.path.isdir(job_info.run_dir)
    ji = job_controller.get_job_info(db_id="1")
    assert ji.state == JobState.READY
    assert ji.remote.prerun_cleanup

    # set the job back to completed and set remote.cleanup=False. rerun with --no-delete
    assert job_controller.set_job_state(JobState.COMPLETED, db_id="1")
    assert job_controller.set_job_doc_properties(
        {"remote.prerun_cleanup": False}, db_id="1"
    )
    run_check_cli(
        ["job", "rerun", "-did", "1", "-f", "-nd"],
    )

    ji = job_controller.get_job_info(db_id="1")
    assert ji.state == JobState.READY
    assert not ji.remote.prerun_cleanup


def test_retry(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.set_job_state(JobState.UPLOADED, db_id="1")
    assert job_controller.get_job_info(db_id="1").state == JobState.UPLOADED

    run_check_cli(
        ["job", "retry", "-did", "1", "-bl"],
        required_out="Operation completed: 1 jobs modified",
    )
    assert job_controller.get_job_info(db_id="1").state == JobState.UPLOADED
    run_check_cli(["job", "retry", "-did", "2"], required_out="Error while retrying")


def test_play_pause(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "pause", "-did", "1"],
        required_out="Operation completed: 1 jobs modified",
    )
    run_check_cli(["job", "pause", "-did", "1"], required_out="Error while pausing")
    assert job_controller.get_job_info(db_id="1").state == JobState.PAUSED

    run_check_cli(
        ["job", "play", "-did", "1"],
        required_out="Operation completed: 1 jobs modified",
    )
    assert job_controller.get_job_info(db_id="1").state == JobState.READY
    run_check_cli(["job", "play", "-did", "1"], required_out="Error while playing")


def test_stop(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "stop", "-did", "1", "-bl"],
        required_out="Operation completed: 1 jobs modified",
    )
    run_check_cli(["job", "stop", "-did", "1"], required_out="Error while stopping")
    assert job_controller.get_job_info(db_id="1").state == JobState.USER_STOPPED


def test_queue_out(job_controller, one_job) -> None:
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "queue-out", "1"],
        required_out=["The remote folder has not been created yet"],
    )

    runner = Runner()
    runner.run_one_job(db_id="1")

    run_check_cli(["job", "queue-out", "1"], required_out=["Queue output from", "add"])

    run_check_cli(
        ["job", "queue-out", "10"],
        required_out="No data matching the request",
        error=True,
    )


def test_set_worker(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "worker", "-did", "1", "test_local_worker_2"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_info(db_id="1").worker == "test_local_worker_2"


def test_set_exec_config(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "exec-config", "-did", "1", "test"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_doc(db_id="1").exec_config == "test"


def test_set_resources(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "resources", "-did", "1", '{"ntasks": 1}'],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_doc(db_id="1").resources == {"ntasks": 1}


def test_set_priority(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.get_job_doc(db_id="1").priority == 0
    run_check_cli(
        ["job", "set", "priority", "-did", "1", "10"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_doc(db_id="1").priority == 10


def test_job_dump(job_controller, one_job, tmp_dir) -> None:
    import os

    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["job", "dump", "-did", "1"])
    assert os.path.isfile("jobs_dump.json")


def test_output(job_controller, one_job) -> None:
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["job", "output", "1"], required_out="has no output", error=True)

    runner = Runner()
    runner.run_one_job(db_id="1")

    run_check_cli(["job", "output", "1"], required_out="6")


def test_files_list(job_controller, one_job) -> None:
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "files", "ls", "1"],
        required_out="The remote folder has not been created yet",
    )

    runner = Runner()
    runner.run_one_job(db_id="1")

    run_check_cli(["job", "files", "ls", "1"], required_out=["queue.out", "queue.err"])

    run_check_cli(
        ["job", "files", "ls", "10"],
        error=True,
        required_out="No data matching the request",
    )


def test_files_get(job_controller, one_job, tmp_dir) -> None:
    import os

    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "files", "get", "1", "queue.err"],
        required_out="The remote folder has not been created yet",
    )

    runner = Runner()
    runner.run_one_job(db_id="1")

    run_check_cli(["job", "files", "get", "1", "queue.out"])
    assert os.path.isfile("queue.out")

    run_check_cli(
        ["job", "files", "get", "10", "queue.out"],
        error=True,
        required_out="No data matching the request",
    )


def test_delete(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "delete", "-did", "2", "--output"],
        required_out="Operation completed: 1 jobs modified",
    )
    flow_doc = job_controller.get_flow_info_by_job_uuid(two_flows_four_jobs[0][0].uuid)

    assert len(flow_doc["jobs"]) == 1
    assert len(flow_doc["ids"]) == 1
    assert len(flow_doc["parents"]) == 1
    assert len(flow_doc["parents"][two_flows_four_jobs[0][0].uuid]["1"]) == 0


def test_queries(job_controller, two_flows_four_jobs) -> None:
    import datetime

    from jobflow_remote.testing.cli import run_check_cli

    # test different query methods

    run_check_cli(
        ["job", "pause", "-meta", '{"test_meta": 1}'],
        required_out="Operation completed: 1 jobs modified",
    )

    req_output_all = [
        "Operation completed: 1 jobs modified",
        "No filter has been set. This will apply the change to all the jobs in the DB.",
    ]
    run_check_cli(
        ["job", "play"],
        cli_input="y",
        required_out=req_output_all,
    )

    run_check_cli(
        ["job", "pause", "--query", "db_id=1"],
        required_out="Operation completed: 1 jobs modified",
    )

    req_output_partial = [
        "Operation completed: 1 jobs modified",
        "Error while playing for job 2 ValueError: Job in state WAITING. The action cannot be performed",
    ]
    run_check_cli(
        ["job", "play", "--worker", "test_local_worker"],
        cli_input="y",
        required_out=req_output_partial,
    )

    now = datetime.datetime.now()
    yesterday = (now - datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    tomorrow = (now + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    run_check_cli(
        ["job", "pause", "--start-date", tomorrow],
        required_out="Operation completed: 0 jobs modified",
    )
    run_check_cli(
        ["job", "pause", "--start-date", yesterday],
        required_out="Operation completed: 4 jobs modified",
    )

    run_check_cli(
        ["job", "play", "--end-date", yesterday],
        required_out="Operation completed: 0 jobs modified",
    )
    run_check_cli(
        ["job", "play", "--end-date", tomorrow],
        required_out="Operation completed: 4 jobs modified",
    )

    run_check_cli(
        ["job", "pause", "--days", "1"],
        required_out="Operation completed: 4 jobs modified",
    )

    run_check_cli(
        ["job", "play", "--hours", "1"],
        required_out="Operation completed: 4 jobs modified",
    )

    run_check_cli(
        ["job", "pause", "--start-date", yesterday, "--days", "1"],
        required_out="Options start_date, days are incompatible",
    )

    run_check_cli(
        ["job", "pause", "--name", "addxxx"],
        required_out="Operation completed: 0 jobs modified",
    )
    run_check_cli(
        ["job", "pause", "--name", "add1"],
        required_out="Operation completed: 1 jobs modified",
    )
    run_check_cli(
        ["job", "play", "--name", "add*"],
        required_out="Operation completed: 1 jobs modified",
    )


def test_report(job_controller) -> None:
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing import add_sleep
    from jobflow_remote.testing.cli import run_check_cli

    now = datetime.now()
    output = [
        "Job Summary",
        "Job State Distribution",
        "Job Trends",
        now.strftime("%Y-%m-%d"),
    ]
    excluded = ["Longest running jobs"]

    # run first with an empty db to check that everything works fine
    run_check_cli(
        ["job", "report", "days", "2"],
        required_out=[*output, "Running Jobs │   0"],
        excluded_out=excluded,
    )

    # a long sleeping job. Will not finish.
    j = add_sleep(1, 10)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    output.append("Worker Jobs Distribution")

    run_check_cli(
        ["job", "report", "days", "2"],
        required_out=[*output, "Running Jobs │   0"],
        excluded_out=excluded,
    )

    runner = Runner()
    runner.run(ticks=2)

    run_check_cli(
        ["job", "report", "days", "2"],
        required_out=output + excluded + ["Running Jobs │   1"],
    )
