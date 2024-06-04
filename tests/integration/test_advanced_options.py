import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_run_batch(job_controller, monkeypatch) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_sleep

    job_ids = []
    for _ in range(3):
        add_first = add_sleep(2, 1)
        add_second = add_sleep(add_first.output, 1)

        flow = Flow([add_first, add_second])
        submit_flow(flow, worker="test_batch_remote_worker")
        job_ids.append([add_first.uuid, add_second.uuid])

    runner = Runner()

    # set this so it will be called
    monkeypatch.setattr(runner.runner_options, "delay_update_batch", 5)

    runner.run_all_jobs(max_seconds=120)

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 6


def test_max_jobs_worker(job_controller, daemon_manager) -> None:
    import time

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_sleep

    # run the daemon in background to check what happens to the
    # jobs during the execution
    daemon_manager.start(raise_on_error=True)

    job_ids = []
    for _ in range(4):
        j = add_sleep(2, 5)
        job_ids.append((j.uuid, 1))
        flow = Flow([j])
        submit_flow(flow, worker="test_max_jobs_worker")

    finished_states = (JobState.REMOTE_ERROR, JobState.FAILED, JobState.COMPLETED)
    running_states = (JobState.RUNNING, JobState.SUBMITTED)

    max_running_jobs = 0
    for _ in range(20):
        time.sleep(1)
        jobs_info = job_controller.get_jobs_info(job_ids=job_ids)
        if all(ji.state in finished_states for ji in jobs_info):
            break
        current_running = sum(ji.state in running_states for ji in jobs_info)
        max_running_jobs = max(max_running_jobs, current_running)

    jobs_info = job_controller.get_jobs_info(job_ids=job_ids)
    assert all(ji.state == JobState.COMPLETED for ji in jobs_info)

    # the max running jobs should be two, meaning that it was reached and cannot
    # be larger. The check could be <= 2, but if it does not reach two it will
    # not be testing some parts of the code and the test is not complete.
    assert max_running_jobs == 2
