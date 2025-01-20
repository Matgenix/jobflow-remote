def test_std_operations(
    wait_daemon_started,
    wait_daemon_stopped,
    wait_daemon_shutdown,
    daemon_manager,
    job_controller,
):
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["runner", "status"],
        required_out="Daemon status: shut_down",
    )

    run_check_cli(
        ["runner", "info"],
        required_out=["Daemon is not running", "No running runner defined in the DB"],
    )

    run_check_cli(
        ["runner", "start"],
    )

    wait_daemon_started(daemon_manager)

    run_check_cli(
        ["runner", "start"],
        required_out="Error while starting the daemon: Daemon process is already running",
        error=True,
    )

    run_check_cli(
        ["runner", "status"],
        required_out="Daemon status: running",
    )

    info_required = [
        "supervisord",
        "runner_daemon_checkout:run_jobflow_checkout",
        "runner_daemon_complete:run_jobflow_complete0",
        "runner_daemon_queue:run_jobflow_queue",
        "runner_daemon_transfer:run_jobflow_transfer0",
        "RUNNING",
    ]
    run_check_cli(
        ["runner", "info"],
        required_out=[*info_required, "hostname", "last_pinged"],
        excluded_out="processes_info",
    )

    run_check_cli(
        ["runner", "info", "-v"],
        required_out=[*info_required, "hostname", "last_pinged", "processes_info"],
    )

    run_check_cli(
        ["runner", "stop"],
        required_out="The stop signal has been sent to the Runner",
    )

    wait_daemon_stopped(daemon_manager)

    run_check_cli(
        ["runner", "info"],
        required_out=[*info_required, "EXITED"],
        excluded_out=["hostname", "last_pinged"],
    )

    run_check_cli(
        ["runner", "shutdown"],
    )

    wait_daemon_shutdown(daemon_manager)

    run_check_cli(
        ["runner", "status"],
        required_out="Daemon status: shut_down",
    )

    run_check_cli(
        ["runner", "info"],
        required_out=["Daemon is not running", "No running runner defined in the DB"],
    )

    # add a fake running runner document to the DB and check that it is still shown
    job_controller.auxiliary.find_one_and_update(
        {"running_runner": {"$exists": True}},
        {"$set": {"running_runner": {"hostname": "test_hostname"}}},
    )
    run_check_cli(
        ["runner", "info"],
        required_out=["Daemon is not running", "hostname"],
        excluded_out="No running runner defined in the DB",
    )


def test_reset(wait_daemon_started, daemon_manager, job_controller):
    from jobflow_remote.testing.cli import run_check_cli

    # set some fake value inside the running_runner document
    runner_info = daemon_manager._get_runner_info()
    runner_info["mac_address"] = "XXXXXXX"
    runner_info["hostname"] = "YYYYYYY"
    job_controller.auxiliary.find_one_and_update(
        {"running_runner": {"$exists": True}}, {"$set": {"running_runner": runner_info}}
    )

    run_check_cli(
        ["runner", "start"],
        required_out=[
            "A daemon runner process may be running on a different machine",
            "YYYYYYY",
            "jf runner reset",
        ],
        error=True,
    )

    run_check_cli(
        ["runner", "reset"],
        required_out=[
            "Do you want to proceed?",
            "YYYYYYY",
            "The running runner document was reset",
        ],
        cli_input="y",
    )

    assert job_controller.get_running_runner() is None

    run_check_cli(
        ["runner", "start"],
    )

    wait_daemon_started(daemon_manager)
    assert daemon_manager.check_status().value == "RUNNING"
