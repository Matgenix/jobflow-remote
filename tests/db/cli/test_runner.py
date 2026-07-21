import pytest


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

    # wait for the runner to have pinged the DB before proceeding
    for _ in range(30):
        time.sleep(1)
        if len(job_controller.get_runner_pings()) > 0:
            break
    else:
        raise RuntimeError("The runner did not ping the DB within the allocated time")

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


@pytest.mark.parametrize(
    ("subcommand", "started_suffix", "final_status_name"),
    [
        ("stop-processes", "_2", "STOPPED"),
        ("shutdown", "_1", "SHUT_DOWN"),
        ("kill", "_2", "STOPPED"),
    ],
    ids=["stop", "shutdown", "kill"],
)
def test_all_runners_termination(
    job_controller,
    wait_daemon_started,
    run_check_cli,
    create_tmp_project,
    daemon_manager,
    tmp_dir,
    random_project_name,
    subcommand,
    started_suffix,
    final_status_name,
):
    from monty.serialization import loadfn

    from jobflow_remote.jobs.daemon import DaemonManager, DaemonStatus

    final_status = DaemonStatus[final_status_name]
    unstarted_suffix = "_1" if started_suffix == "_2" else "_2"
    _, pn_started = create_tmp_project(suffix=started_suffix)
    _, pn_unstarted = create_tmp_project(suffix=unstarted_suffix)
    dm_started = DaemonManager.from_project_name(pn_started)
    dm_unstarted = DaemonManager.from_project_name(pn_unstarted)

    daemon_manager.start()
    dm_started.start()
    wait_daemon_started(daemon_manager)
    wait_daemon_started(dm_started)

    assert dm_unstarted.check_status() == DaemonStatus.SHUT_DOWN

    json_path = tmp_dir / "affected_proj.json"
    run_check_cli(
        ["runner", subcommand, "--all", "--wait", "--json", json_path],
        required_out=[random_project_name, pn_started],
        excluded_out=pn_unstarted,
    )

    assert daemon_manager.check_status() == final_status
    assert dm_started.check_status() == final_status
    assert dm_unstarted.check_status() == DaemonStatus.SHUT_DOWN
    assert set(loadfn(json_path)) == {random_project_name, pn_started}


@pytest.mark.parametrize(
    ("subcommand", "patched_method_name", "action_name", "final_status_name"),
    [
        ("stop-processes", "stop", "stopping", "STOPPED"),
        ("shutdown", "shut_down", "shutting down", "SHUT_DOWN"),
        ("kill", "kill", "killing", "STOPPED"),
    ],
    ids=["stop", "shutdown", "kill"],
)
def test_all_runners_termination_with_error(
    job_controller,
    wait_daemon_started,
    run_check_cli,
    create_tmp_project,
    daemon_manager,
    tmp_dir,
    random_project_name,
    subcommand,
    patched_method_name,
    action_name,
    final_status_name,
):
    from unittest.mock import Mock, patch

    from monty.serialization import loadfn

    from jobflow_remote.jobs.daemon import DaemonManager, DaemonStatus

    final_status = DaemonStatus[final_status_name]
    _, pn1 = create_tmp_project(suffix="_1")
    _, pn2 = create_tmp_project(suffix="_2")
    dm1 = DaemonManager.from_project_name(pn1)
    dm2 = DaemonManager.from_project_name(pn2)

    daemon_manager.start()
    dm1.start()
    dm2.start()

    wait_daemon_started(daemon_manager)
    wait_daemon_started(dm1)
    wait_daemon_started(dm2)

    original_method = getattr(DaemonManager, patched_method_name)
    error_project = pn1

    def with_error(self, **kwargs):
        if self.project.name == error_project:
            raise RuntimeError(f"Simulated test error for project {error_project}")
        return original_method(self, **kwargs)

    json_path = tmp_dir / "affected_proj_err.json"

    with patch.object(DaemonManager, patched_method_name, with_error):
        run_check_cli(
            [
                "runner",
                subcommand,
                "--all",
                "--wait",
                "--json",
                json_path,
            ],
            required_out=[
                f"- {random_project_name}",
                f"- {pn2}",
                f"Simulated test error for project {error_project}",
            ],
        )

    assert daemon_manager.check_status() == final_status
    assert dm2.check_status() == final_status
    # dm1 was not affected due to the error
    affected_set = set(loadfn(json_path))
    assert affected_set == {random_project_name, pn2}
    assert pn1 not in affected_set

    # Test timeout: dm1 is still running; mock the action so the daemon stays
    # alive while the wait loop polls check_status, triggering the max-wait timeout.
    with patch.object(DaemonManager, patched_method_name, Mock(return_value=True)):
        run_check_cli(
            [
                "runner",
                subcommand,
                "--all",
                "--wait",
                "--max-wait",
                "1",
            ],
            required_out=[
                f"Not all the runners finished {action_name} within the allocated time",
                pn1,
            ],
            error=True,
        )
