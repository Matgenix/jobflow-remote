def test_jobs_list(job_controller, four_jobs):

    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    # split "job id" from "index", because it can be sent to a new line
    columns = ["DB id", "Name", "State", "Job id", "(Index)", "Worker", "Last updated"]
    outputs = columns + [f"add{i}" for i in range(1, 5)] + ["READY", "WAITING"]

    run_check_cli(["job", "list"], required_out=outputs)

    # the output table is squeezed. Hard to check the stdout. Just check that runs correctly
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


def test_job_info(job_controller, four_jobs):

    from jobflow_remote.testing.cli import run_check_cli

    outputs = ["name = 'add1'", "state = 'READY'"]
    excluded_n = ["run_dir = None", "start_time = None"]
    excluded = excluded_n + ["job = {"]
    run_check_cli(["job", "info", "1"], required_out=outputs, excluded_out=excluded)

    outputs += excluded_n
    run_check_cli(
        ["job", "info", four_jobs[0].jobs[0].uuid, "-n"], required_out=outputs
    )

    outputs = ["state = 'READY'", "job = {"]
    run_check_cli(["job", "info", "1", "-vvv"], required_out=outputs)

    run_check_cli(
        ["job", "info", "10"], error=True, required_out="No data matching the request"
    )


def test_set_state(job_controller, four_jobs):

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


def test_rerun(job_controller, four_jobs):

    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.set_job_state(JobState.COMPLETED, db_id="1")

    run_check_cli(
        ["job", "rerun", "-did", "1", "-f"],
        required_out="Operation completed: 1 jobs modified",
    )
    assert job_controller.get_job_info(db_id="1").state == JobState.READY
    # fails because already READY
    run_check_cli(["job", "rerun", "-did", "1"], required_out="Error while rerunning")


def test_retry(job_controller, four_jobs):

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


def test_play_pause(job_controller, four_jobs):

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


def test_stop(job_controller, four_jobs):

    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "stop", "-did", "1", "-bl"],
        required_out="Operation completed: 1 jobs modified",
    )
    run_check_cli(["job", "stop", "-did", "1"], required_out="Error while stopping")
    assert job_controller.get_job_info(db_id="1").state == JobState.USER_STOPPED


def test_queue_out(job_controller, one_job):

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


def test_set_worker(job_controller, one_job):

    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "worker", "-did", "1", "test_local_worker_2"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_info(db_id="1").worker == "test_local_worker_2"


def test_set_exec_config(job_controller, one_job):

    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "exec-config", "-did", "1", "test"],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_doc(db_id="1").exec_config == "test"


def test_set_resources(job_controller, one_job):

    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["job", "set", "resources", "-did", "1", '{"ntasks": 1}'],
        required_out="Operation completed: 1 jobs modified",
    )

    assert job_controller.get_job_doc(db_id="1").resources == {"ntasks": 1}


def test_job_dump(job_controller, one_job, tmp_dir):

    import os

    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["job", "dump", "-did", "1"])
    assert os.path.isfile("jobs_dump.json")


def test_output(job_controller, one_job):

    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["job", "output", "1"], required_out="has no output", error=True)

    runner = Runner()
    runner.run_one_job(db_id="1")

    run_check_cli(["job", "output", "1"], required_out="6")


def test_files_list(job_controller, one_job):

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


def test_files_get(job_controller, one_job, tmp_dir):

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
