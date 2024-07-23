import os
import time

import pytest

# pytestmark = pytest.mark.skipif(
#     not os.environ.get("CI"),
#     reason="Only run integration tests in CI, unless forced with 'CI' env var",
# )


def _wait_daemon_started(daemon_manager, max_wait: int = 10) -> bool:
    from jobflow_remote.jobs.daemon import DaemonStatus

    for _i in range(max_wait):
        time.sleep(1)
        state = daemon_manager.check_status()
        assert state in (DaemonStatus.STARTING, DaemonStatus.RUNNING)
        if state == DaemonStatus.RUNNING:
            return True
    raise RuntimeError(
        f"The daemon did not start running within the expected time ({max_wait})"
    )


def _wait_daemon_shutdown(daemon_manager, max_wait: int = 10) -> bool:
    from jobflow_remote.jobs.daemon import DaemonError, DaemonStatus

    for _i in range(max_wait):
        time.sleep(1)
        state = None
        try:
            state = daemon_manager.check_status()
        except DaemonError:
            pass
        # TODO: should we check state here such as in _wait_daemon_started ?
        if state == DaemonStatus.SHUT_DOWN:
            return True
    raise RuntimeError(
        f"The daemon did not start running within the expected time ({max_wait})"
    )


@pytest.mark.parametrize(
    "single",
    [True, False],
)
def test_start_stop(job_controller, single, daemon_manager) -> None:
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
    _wait_daemon_started(daemon_manager)

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
    _wait_daemon_started(daemon_manager)
    assert daemon_manager.shut_down(raise_on_error=True)
    _wait_daemon_shutdown(daemon_manager)
    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    processes_info = daemon_manager.get_processes_info()
    assert processes_info is None


def test_kill(job_controller, daemon_manager) -> None:
    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN

    # killing when shut down should not have an effect
    assert daemon_manager.kill(raise_on_error=True)

    assert daemon_manager.start(raise_on_error=True, single=True)
    _wait_daemon_started(daemon_manager)

    assert daemon_manager.kill(raise_on_error=True)
    time.sleep(1)
    assert daemon_manager.check_status() == DaemonStatus.STOPPED


def test_kill_supervisord(job_controller, daemon_manager, caplog) -> None:
    import signal
    import time

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=True)
    _wait_daemon_started(daemon_manager)

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
    # TODO if run alone the check below passes. If run among all the others
    # the log message is not present.
    # log_msg = caplog.messages
    # assert len(log_msg) > 0
    # assert f"Process with pid {supervisord_pid} is not running but daemon files are
    # present" in log_msg[-1]


def test_kill_one_process(job_controller, daemon_manager) -> None:
    import signal
    import time

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=False)
    _wait_daemon_started(daemon_manager)

    procs_info = daemon_manager.get_processes_info()
    run_jobflow_queue_pid = procs_info["runner_daemon_queue:run_jobflow_queue"]["pid"]

    # directly kill the supervisord process
    os.kill(run_jobflow_queue_pid, signal.SIGKILL)
    time.sleep(1)
    assert daemon_manager.check_status() == DaemonStatus.PARTIALLY_RUNNING


def test_runner_info_auxiliary(job_controller, daemon_manager) -> None:
    import datetime

    from jobflow_remote.jobs.daemon import DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert job_controller.get_running_runner() is None
    assert daemon_manager.start(raise_on_error=True, single=False)
    _wait_daemon_started(daemon_manager)
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


def test_start_two_runners(daemon_manager) -> None:
    from jobflow_remote.jobs.daemon import DaemonError, DaemonStatus

    assert daemon_manager.check_status() == DaemonStatus.SHUT_DOWN
    assert daemon_manager.start(raise_on_error=True, single=False)
    _wait_daemon_started(daemon_manager)

    with pytest.raises(DaemonError, match=r"A daemon runner process may be running"):
        assert daemon_manager.start(raise_on_error=True, single=False)
