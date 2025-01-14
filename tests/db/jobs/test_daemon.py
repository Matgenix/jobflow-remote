import os
import time

import pytest


def _check_running_runner_doc(job_controller, runner_info):
    # check that the state in the DB was not changed.
    # do not check dates, as apparently they can be slightly off by few a fraction of a second.
    db_info = job_controller.auxiliary.find_one({"running_runner": {"$exists": True}})[
        "running_runner"
    ]
    for key in db_info:
        if key in ("start_time", "last_pinged"):
            continue
        assert db_info[key] == runner_info[key]


@pytest.mark.parametrize(
    "single",
    [True, False],
)
def test_start_stop(
    job_controller,
    single,
    daemon_manager,
    wait_daemon_started,
    wait_daemon_shutdown,
    caplog,
) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.daemon import DaemonStatus
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j = add(1, 5)

    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    assert job_controller.count_jobs(states=JobState.READY) == 1

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=single)
    wait_daemon_started(daemon_manager)

    finished_states = (JobState.REMOTE_ERROR, JobState.FAILED, JobState.COMPLETED)

    for _ in range(20):
        time.sleep(1)
        jobs_info = job_controller.get_jobs_info()
        if all(ji.state in finished_states for ji in jobs_info):
            break

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1

    processes_info = daemon_manager.get_processes_info()
    expected_nprocs = 2 if single else 5
    assert len(processes_info) == expected_nprocs

    assert daemon_manager.stop(raise_on_error=True, wait=True)
    assert daemon_manager.check_status() == DaemonStatus.STOPPED

    assert daemon_manager.start(raise_on_error=True, single=True)
    if not single:
        warn_msg = (
            "Daemon is STOPPED, but the options single differ from the values used "
            "to activate supervisor. The daemon will start with the initial configurations"
        )
        assert warn_msg in caplog.text

    wait_daemon_started(daemon_manager)
    assert daemon_manager.shut_down(raise_on_error=True)
    wait_daemon_shutdown(daemon_manager)
    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    processes_info = daemon_manager.get_processes_info()
    assert processes_info is None


def test_kill(job_controller, daemon_manager, wait_daemon_started) -> None:
    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    # killing when shut down should not have an effect
    assert daemon_manager.kill(raise_on_error=True)

    assert daemon_manager.start(raise_on_error=True, single=True)
    wait_daemon_started(daemon_manager)

    assert daemon_manager.kill(raise_on_error=True)
    time.sleep(1)
    assert daemon_manager.check_status() == DaemonStatus.STOPPED


def test_kill_supervisord(
    job_controller, daemon_manager, caplog, wait_daemon_started
) -> None:
    import signal
    import time

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=True)
    wait_daemon_started(daemon_manager)

    processes_info = daemon_manager.get_processes_info()
    supervisord_pid = processes_info["supervisord"]["pid"]

    # directly kill the supervisord process
    os.kill(supervisord_pid, signal.SIGKILL)
    # also kill all the processes
    for process_dict in processes_info.values():
        os.kill(process_dict["pid"], signal.SIGKILL)
    time.sleep(2)
    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    # check that the warning message is present among the logged messages
    log_msg = caplog.messages
    assert len(log_msg) > 0
    assert (
        f"Process with pid {supervisord_pid} is not running but daemon files are present"
        in log_msg[-1]
    )


def test_kill_one_process(job_controller, daemon_manager, wait_daemon_started) -> None:
    import signal
    import time

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=False)
    wait_daemon_started(daemon_manager)

    procs_info = daemon_manager.get_processes_info()
    run_jobflow_queue_pid = procs_info["runner_daemon_queue:run_jobflow_queue"]["pid"]

    # directly kill the supervisord process
    os.kill(run_jobflow_queue_pid, signal.SIGKILL)
    time.sleep(1)
    assert daemon_manager.check_status() == DaemonStatus.PARTIALLY_RUNNING


def test_runner_info_auxiliary(
    job_controller, daemon_manager, wait_daemon_started
) -> None:
    import datetime

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert job_controller.get_running_runner() is None
    assert daemon_manager.start(raise_on_error=True, single=False)
    wait_daemon_started(daemon_manager)
    ref_runner_info = daemon_manager._get_runner_info()
    runner_info = job_controller.get_running_runner()
    for key in [
        "hostname",
        "mac_address",
        "user",
        "daemon_dir",
        "project_name",
        "runner_options",
    ]:
        assert ref_runner_info[key] == runner_info[key]
    ref_process_pids = sorted(
        [
            (process, pdict["pid"])
            for process, pdict in ref_runner_info["processes_info"].items()
        ]
    )
    process_pids = sorted(
        [
            (process, pdict["pid"])
            for process, pdict in runner_info["processes_info"].items()
        ]
    )
    assert ref_process_pids == process_pids
    assert isinstance(runner_info["start_time"], datetime.datetime)
    assert isinstance(runner_info["last_pinged"], datetime.datetime)


def test_runner_different_machine(daemon_manager, job_controller, caplog) -> None:
    # Test that all the methods will not have effect if in the DB
    # a running_runner is present with a different machine
    from jobflow_remote.jobs.daemon import DaemonStatus, RunningDaemonError

    # set some fake value inside the running_runner document
    runner_info = daemon_manager._get_runner_info()
    runner_info["mac_address"] = "XXXXXXX"
    runner_info["hostname"] = "YYYYYYY"
    job_controller.auxiliary.find_one_and_update(
        {"running_runner": {"$exists": True}}, {"$set": {"running_runner": runner_info}}
    )

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    with pytest.raises(
        RunningDaemonError,
        match=r"A daemon runner process may be running on a different machine",
    ):
        daemon_manager.start(raise_on_error=True, single=False)

    assert not daemon_manager.start(raise_on_error=False, single=False)

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    _check_running_runner_doc(job_controller=job_controller, runner_info=runner_info)

    with pytest.raises(
        RunningDaemonError,
        match=r"A daemon runner process may be running on a different machine",
    ):
        daemon_manager.kill(raise_on_error=True)

    assert not daemon_manager.kill(raise_on_error=False)

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    _check_running_runner_doc(job_controller=job_controller, runner_info=runner_info)

    with pytest.raises(
        RunningDaemonError,
        match=r"A daemon runner process may be running on a different machine",
    ):
        daemon_manager.stop(raise_on_error=True)

    assert not daemon_manager.stop(raise_on_error=False)

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    _check_running_runner_doc(job_controller=job_controller, runner_info=runner_info)

    with pytest.raises(
        RunningDaemonError,
        match=r"A daemon runner process may be running on a different machine",
    ):
        daemon_manager.shut_down(raise_on_error=True)

    assert not daemon_manager.shut_down(raise_on_error=False)

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    _check_running_runner_doc(job_controller=job_controller, runner_info=runner_info)


def test_stop_restart_diff(daemon_manager, caplog, wait_daemon_started):
    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=False)
    wait_daemon_started(daemon_manager)

    assert daemon_manager.stop(raise_on_error=True, wait=True)
    assert daemon_manager.check_status() == DaemonStatus.STOPPED

    assert daemon_manager.start(raise_on_error=True, single=False)
    wait_daemon_started(daemon_manager)
    assert caplog.text == ""

    assert daemon_manager.stop(raise_on_error=True, wait=True)
    assert daemon_manager.check_status() == DaemonStatus.STOPPED

    assert daemon_manager.start(raise_on_error=True, single=True, log_level="debug")
    wait_daemon_started(daemon_manager)
    assert (
        "Daemon is STOPPED, but the options single, log_level differ from the values used to activate supervisor"
        in caplog.text
    )
    assert daemon_manager.check_status() == DaemonStatus.RUNNING


def test_missing_running_runner_doc(
    daemon_manager,
    job_controller,
    wait_daemon_started,
    wait_daemon_stopped,
    wait_daemon_shutdown,
):
    from jobflow_remote.jobs.daemon import DaemonStatus

    # remote the running_runner document
    assert (
        job_controller.auxiliary.delete_one(
            {"running_runner": {"$exists": True}}
        ).deleted_count
        == 1
    )

    error = (
        "No daemon runner document.\nThe auxiliary collection does"
        " not contain information about running daemon. Your database was "
        "likely set up or reset using an old version of Jobflow Remote. You can "
        'upgrade the database using the command "jf admin upgrade"'
    )

    # the runner cannot start
    with pytest.raises(ValueError, match=error):
        daemon_manager.start(raise_on_error=True, single=False)

    # recreate the document
    job_controller.reset()

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    assert daemon_manager.start(raise_on_error=True, single=False)
    wait_daemon_started(daemon_manager)

    # remote the running_runner document
    assert (
        job_controller.auxiliary.delete_one(
            {"running_runner": {"$exists": True}}
        ).deleted_count
        == 1
    )

    # daemon can be stopped
    daemon_manager.stop(raise_on_error=True)
    wait_daemon_stopped(daemon_manager)

    assert daemon_manager.check_status() == DaemonStatus.STOPPED

    # daemon can be shut-down
    daemon_manager.shut_down(raise_on_error=True)
    wait_daemon_shutdown(daemon_manager)

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    # still cannot start
    with pytest.raises(ValueError, match=error):
        daemon_manager.start(raise_on_error=True, single=False)
