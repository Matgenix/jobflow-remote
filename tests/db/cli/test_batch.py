import time

import pytest


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
def test_batch_worker(
    job_controller,
    runner,
    daemon_manager,
    wait_daemon_started,
    wait_daemon_stopped,
    patch_project,
    run_check_cli,
):
    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import BatchState, JobState
    from jobflow_remote.testing import add_sleep

    for _ in range(8):
        add_j = add_sleep(2, 1)
        submit_flow(add_j, worker="test_local_batch_worker")

    assert job_controller.count_flows() == 8
    assert job_controller.count_jobs() == 8

    daemon_manager.start()
    wait_daemon_started(daemon_manager)

    for _ in range(30):
        if job_controller.count_jobs(states=JobState.COMPLETED) == 8:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Jobs did not complete")

    for _ in range(20):
        if not job_controller.get_batches(
            batch_state=[BatchState.RUNNING, BatchState.SUBMITTED]
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError("Batch processes did not finish")

    daemon_manager.stop()
    wait_daemon_stopped(daemon_manager)

    batches = job_controller.get_batches()
    assert len(batches) == 4
    # sorting on update time and process id as update time may be the same (ms precision in MongoDB)
    ordered_batches = sorted(batches, key=lambda x: (x.updated_on, x.process_id))
    ordered_batches.sort(key=lambda x: x.updated_on, reverse=True)

    run_check_cli(
        ["batch", "list"],
        required_out="Batches info",
        excluded_out=["Running batches info", "RUNNING"],
    )

    run_check_cli(
        ["batch", "list", "-m", "2"],
        required_out=[
            "Batches info",
            "FINISHED",
            ordered_batches[0].process_id,
            ordered_batches[1].process_id,
            ordered_batches[0].batch_uid,
            ordered_batches[1].batch_uid,
        ],
        excluded_out=[
            "Running batches info",
            "RUNNING",
            ordered_batches[-1].process_id,
            ordered_batches[-2].process_id,
            ordered_batches[-1].batch_uid,
            ordered_batches[-2].batch_uid,
        ],
    )

    # test "batch info"
    run_check_cli(
        ["batch", "info", batches[0].batch_uid],
        required_out=[
            "process_id",
            "batch_uid",
            "FINISHED",
            batches[0].batch_uid,
            batches[0].process_id,
        ],
    )
    run_check_cli(
        ["batch", "info", batches[0].process_id],
        required_out=[
            "process_id",
            "batch_uid",
            "FINISHED",
            batches[0].batch_uid,
            batches[0].process_id,
        ],
    )

    # test "batch delete"
    run_check_cli(
        ["batch", "delete", "-pid", batches[0].process_id],
        required_out=["This operation will delete 1 batch"],
        excluded_out=["1 batch processes deleted"],
        cli_input="n",
    )
    assert job_controller.count_batches() == 4

    run_check_cli(
        ["batch", "delete", "-pid", batches[0].process_id, "-y"],
        excluded_out=["This operation will delete 1 batch"],
        required_out=["1 batch processes deleted"],
    )
    assert job_controller.count_batches() == 3

    run_check_cli(
        ["batch", "delete", "--state", "RUNNING"],
        required_out=["This could lead to inconsistencies or data loss"],
        excluded_out=["batch processes deleted"],
        cli_input="n",
    )
    assert job_controller.count_batches() == 3

    run_check_cli(
        ["batch", "delete"],
        excluded_out=["This could lead to inconsistencies or data loss"],
        required_out=["This operation will delete", "batch processes deleted"],
        cli_input="y",
    )
    assert job_controller.count_batches() == 0
