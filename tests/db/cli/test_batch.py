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

    job_ids_db_ids = []
    for _ in range(8):
        add_j = add_sleep(2, 1)
        job_ids_db_ids.append(
            (add_j.uuid, submit_flow(add_j, worker="test_local_batch_worker")[0])
        )

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

    job_info = job_controller.get_job_info(job_id=job_ids_db_ids[0][0])
    missing_batch_uids = [
        ob.batch_uid
        for ob in ordered_batches
        if ob.process_id != job_info.remote.process_id
    ]
    run_check_cli(
        ["batch", "list", "-jid", f"{job_ids_db_ids[0][0]}:1"],
        required_out=[
            "Batches info",
            "FINISHED",
            job_info.remote.process_id,
        ],
        excluded_out=["Running batches info", "RUNNING", *missing_batch_uids],
    )

    run_check_cli(
        ["batch", "list", "-did", job_ids_db_ids[0][1]],
        required_out=[
            "Batches info",
            "FINISHED",
            job_info.remote.process_id,
        ],
        excluded_out=["Running batches info", "RUNNING", *missing_batch_uids],
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

    run_check_cli(
        ["batch", "info", "FAKE_PROCESS_ID"],
        required_out="No batch process matching the request",
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

    # manually set one of the batch jobs to RUNNING
    job_controller.batches.find_one_and_update(
        {"process_id": ordered_batches[1].process_id},
        {"$set": {"batch_state": BatchState.RUNNING.value}},
    )

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
    assert job_controller.count_batches() == 1

    # add a second job with the same process id to trigger an error in "batch info"
    job_controller.add_batch_process(
        process_id=ordered_batches[1].process_id,
        batch_uid="fake_uid",
        worker="fake_worker",
    )

    run_check_cli(
        ["batch", "info", ordered_batches[1].process_id],
        required_out="More than one document matches the selection criteria",
        error=True,
    )


def test_fix_batch_doc_dict(job_controller, run_check_cli):
    from jobflow_remote.utils.data import suuid

    uuid1 = suuid()
    uuid2 = suuid()
    job_controller.add_batch_process(
        process_id="1234", batch_uid=uuid1, worker="test_local_batch_worker"
    )
    job_controller.add_batch_process(
        process_id="5678", batch_uid=uuid2, worker="test_local_batch_worker"
    )
    job_controller.update_job_in_batch(
        batch_uid=uuid1, job_id="jobuuid1", job_index=1, db_id="1"
    )
    # setting the "jobs" to a dict (instead of a list) to test that the error
    # is handled correctly and prints the proper error message (this is due to
    # a change in the type during development)
    job_controller.batches.find_one_and_update(
        {"process_id": "5678"},
        {"$set": {"jobs": {"jobuuid2": {"1": {"status": "BATCH_SUBMITTED"}}}}},
    )

    run_check_cli(
        ["batch", "list"],
        required_out="It seems that you have used a development version of jobflow-remote",
        error=True,
    )

    run_check_cli(
        ["batch", "delete", "-y", "-s", "SUBMITTED"],
        required_out="It seems that you have used a development version of jobflow-remote",
        error=True,
    )

    assert isinstance(job_controller.get_batches(process_id="1234")[0].jobs, list)
    assert isinstance(
        job_controller.batches.find_one({"process_id": "5678"})["jobs"], dict
    )

    run_check_cli(
        ["batch", "fix-batch-doc-jobs-dict"],
        required_out="1 batch documents modified",
    )
    # check that the existing list is not modified and the dict is converted to a list
    assert job_controller.get_batches(process_id="1234")[0].jobs == [
        ["1", "jobuuid1", 1]
    ]
    assert job_controller.get_batches(process_id="5678")[0].jobs == []

    run_check_cli(
        ["batch", "list"],
        required_out=["1234", "5678"],
    )
