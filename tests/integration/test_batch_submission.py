import os
import time

import pytest

from jobflow_remote.jobs.state import BatchState

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


@pytest.mark.parametrize(
    "patch_project",
    [
        {
            "_set": {
                "runner->delay_update_batch": 0.2,
                "runner->delay_advance_status": 0.2,
                "runner->delay_check_run_status": 0.2,
                "runner->delay_checkout": 0.2,
            }
        },
    ],
    indirect=True,
)
@pytest.mark.workers(["test_batch_remote_worker"])
def test_run_batch_submission1(
    job_controller,
    monkeypatch,
    clean_slurm_queue,
    daemon_manager,
    wait_daemon_started,
    wait_daemon_stopped,
    patch_project,
    runner,
    mocker,
) -> None:
    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add_sleep

    for _ in range(4):
        add_sleep_job = add_sleep(1, 30)
        submit_flow(add_sleep_job, worker="test_batch_remote_worker")

    daemon_manager.start(raise_on_error=True)
    wait_daemon_started(daemon_manager)

    for _ in range(20):
        if (
            len(
                job_controller.get_all_batches(
                    batch_state=[BatchState.SUBMITTED, BatchState.RUNNING]
                )
            )
            == 1
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError("The batch job never started")

    daemon_manager.stop(raise_on_error=True)
    wait_daemon_stopped(daemon_manager)

    runner.update_batch_jobs()
    assert (
        len(
            job_controller.get_all_batches(
                batch_state=[BatchState.SUBMITTED, BatchState.RUNNING]
            )
        )
        == 1
    )
    # Fake a TimeoutError when getting jobs from the worker
    mocker.patch.object(
        runner.queue_managers["test_batch_remote_worker"],
        "get_jobs_list",
        side_effect=TimeoutError,
    )
    # Call to update_batch_jobs should not submit a new job as there is already one running
    runner.update_batch_jobs()

    assert (
        len(
            job_controller.get_all_batches(
                batch_state=[BatchState.SUBMITTED, BatchState.RUNNING]
            )
        )
        == 1
    )
