def test_std_operations(
    wait_daemon_started,
    wait_daemon_stopped,
    wait_daemon_shutdown,
    daemon_manager,
    job_controller,
    run_check_cli,
):
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

    rr_before = job_controller.get_running_runner()
    run_check_cli(
        ["runner", "restart"],
    )
    wait_daemon_started(daemon_manager)
    rr_after = job_controller.get_running_runner()
    assert rr_after["start_time"] > rr_before["start_time"]

    run_check_cli(
        ["runner", "stop-processes"],
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
        ["runner", "start"],
    )

    wait_daemon_started(daemon_manager)

    # "stop" means "shutdown", test this as well
    run_check_cli(
        ["runner", "stop"],
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


def test_reset(wait_daemon_started, daemon_manager, job_controller, run_check_cli):
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
            "A daemon runner process associated to this database may be already running",
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


def test_info(
    wait_daemon_started,
    wait_daemon_shutdown,
    daemon_manager,
    job_controller,
    run_check_cli,
):
    import time

    # start the daemon and test the correct behaviour
    daemon_manager.start(single=True)
    wait_daemon_started(daemon_manager)

    running_runner = job_controller.get_running_runner()
    runner_info = daemon_manager._get_runner_info()

    # wait for the runner to have pinged the DB before proceeding
    for _ in range(30):
        time.sleep(1)
        if len(job_controller.get_runner_pings()) > 0:
            break
    else:
        raise RuntimeError("The runner did not ping the DB within the allocated time")

    ping_data = {
        "daemon_id": runner_info["processes_info"]["supervisord"]["pid"],
        "runner_id": runner_info["processes_info"]["runner_daemon:run_jobflow0"]["pid"],
        "project_name": running_runner["project_name"],
        "hostname": running_runner["hostname"],
        "run_options": {},
        "user": running_runner["user"],
        "daemon_dir": running_runner["daemon_dir"],
    }

    run_check_cli(
        ["runner", "info"],
        required_out=[
            "supervisord",
            "runner_daemon",
            "hostname",
            running_runner["hostname"],
        ],
        excluded_out=[
            "Daemon is not running",
            "No running runner defined in the DB",
            "Runner pings",
            f"│ {ping_data['hostname']}",
            "inconsistency",
        ],
    )

    run_check_cli(
        ["runner", "info", "--pings"],
        required_out=[
            "supervisord",
            "runner_daemon",
            "hostname",
            running_runner["hostname"],
            "Runner pings",
            f"│ {ping_data['hostname']}",
        ],
        excluded_out=[
            "Daemon is not running",
            "No running runner defined in the DB",
            "inconsistency",
        ],
    )

    # now stop and restart the runner to trigger issues
    daemon_manager.shut_down()
    wait_daemon_shutdown(daemon_manager)

    daemon_manager.start(single=True)
    wait_daemon_started(daemon_manager)
    job_controller.ping_running_runner(data=ping_data)

    req_out = [
        "supervisord",
        "runner_daemon",
        "hostname",
        running_runner["hostname"],
        "inconsistency between the actual runner information and the last ping in the database",
        f"running under another supervisor process: {ping_data['daemon_id']}",
    ]
    excl_out = [
        "Daemon is not running",
        "No running runner defined in the DB",
        "Runner pings",
        f"│ {ping_data['hostname']}",
    ]
    run_check_cli(
        ["runner", "info"],
        required_out=req_out,
        excluded_out=[*excl_out, "active on another machine"],
    )

    ping_data["hostname"] = "XXXX"
    job_controller.ping_running_runner(data=ping_data)
    run_check_cli(
        ["runner", "info"],
        required_out=[*req_out, "active on another machine"],
        excluded_out=excl_out,
    )
