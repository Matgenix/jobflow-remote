import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


@pytest.mark.workers(["test_batch_remote_worker"])
@pytest.mark.parametrize(
    "patch_project",
    [
        {"_set": {"queue->batches_collection": "the_batches"}},
        {"_set": {"queue->batches_collection": None}},
    ],
    ids=["with_batches_collection", "without_batches_collection"],
    indirect=True,
)
def test_run_batch(
    job_controller, monkeypatch, clean_slurm_queue, mocker, patch_project, request
) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.cli.utils import check_valid_uuid
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

    add_batch_process_spy = mocker.spy(runner.job_controller, "add_batch_process")
    if request.node.callspec.id == "with_batches_collection":
        insert_batch_process_spy = mocker.spy(
            runner.job_controller.batches, "insert_one"
        )

    # set this so it will be called
    monkeypatch.setattr(runner.runner_options, "delay_update_batch", 0.05)
    monkeypatch.setattr(runner.runner_options, "delay_advance_status", 0.05)
    monkeypatch.setattr(runner.runner_options, "delay_check_run_status", 0.05)
    monkeypatch.setattr(runner.runner_options, "delay_checkout", 0.05)
    monkeypatch.setattr(
        runner.workers["test_batch_remote_worker"].batch, "sleep_time", 0.5
    )
    monkeypatch.setattr(runner.workers["test_batch_remote_worker"].batch, "max_wait", 5)

    runner.run_all_jobs(max_seconds=30)

    assert add_batch_process_spy.call_count == 1
    args, kwargs = add_batch_process_spy.call_args
    slurm_job_id = args[0]
    # make sure the process id is not a uuid (we could check that it is a "valid" slurm id also)
    assert not check_valid_uuid(slurm_job_id, raise_on_error=False)

    if request.node.callspec.id == "with_batches_collection":
        assert runner.job_controller.batches is not None
        assert insert_batch_process_spy.call_count == 1
    elif request.node.callspec.id == "without_batches_collection":
        assert runner.job_controller.batches is None
    else:
        pytest.fail("Should not be here!")
    assert runner.job_controller.count_jobs(states=JobState.COMPLETED) == 6

    # verify that only one job was executed at the time. start_time of a job
    # is after the end_time of the one preceding it.
    # This should test that the batch runner is not running with multiple
    # parallel processes
    jobs_info = runner.job_controller.get_jobs_info()
    jobs_info = sorted(jobs_info, key=lambda x: x.start_time)
    for i in range(len(jobs_info) - 1):
        assert jobs_info[i].end_time < jobs_info[i + 1].start_time
    # Check that the process id is the one of the batch process (slurm id)
    for job_info in jobs_info:
        assert job_info.remote.process_id == slurm_job_id


@pytest.mark.workers(["test_batch_multi_remote_worker"])
def test_run_batch_multi(job_controller, monkeypatch, clean_slurm_queue) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_sleep

    # add two jobs that will take a few seconds to run. They should
    # be executed simultaneously
    job_ids = []
    for _ in range(2):
        add_j = add_sleep(2, 15)

        flow = Flow([add_j])
        submit_flow(flow, worker="test_batch_multi_remote_worker")
        job_ids.append(add_j.uuid)

    runner = Runner()

    # set this so it will be called
    monkeypatch.setattr(runner.runner_options, "delay_update_batch", 1)

    runner.run_all_jobs(max_seconds=120)

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 2

    # verify that the two jobs where executed in parallel.
    jobs_info = job_controller.get_jobs_info()
    for ji1 in jobs_info:
        for ji2 in jobs_info:
            assert ji1.start_time < ji2.end_time


@pytest.mark.workers(["test_batch_multi_remote_worker"])
@pytest.mark.parametrize(
    "patch_project",
    [
        {
            "_set": {
                "queue->batches_collection": "the_batches",
                "runner->delay_update_batch": 1,
            }
        },
        {"_set": {"queue->batches_collection": None, "runner->delay_update_batch": 1}},
    ],
    ids=["with_batches_collection", "without_batches_collection"],
    indirect=True,
)
def test_run_batch_multi_fail(
    patch_project,
    job_controller,
    daemon_manager,
    wait_daemon_started,
    wait_daemon_shutdown,
    clean_slurm_queue,
    patch_cli_consoles,
    request,
) -> None:
    from qtoolkit.core.data_objects import CancelStatus

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.batch import RemoteBatchManager
    from jobflow_remote.jobs.state import BatchState, JobState
    from jobflow_remote.remote.queue import QueueManager
    from jobflow_remote.testing import add_sleep
    from jobflow_remote.testing.cli import run_check_cli
    from jobflow_remote.utils.data import check_valid_uuid

    # First reset everything
    assert job_controller.reset(max_limit=0)

    run_check_cli(
        ["batch", "list"],
        required_out_colored="[gold1]No batch processes running[/gold1]",
        excluded_out="Running batches info",
    )
    run_check_cli(
        ["batch", "list", "--all"],
        required_out_colored="[gold1]No batch processes[/gold1]",
    )

    def submit_jobs(n: int, sleep: int):
        job_ids = []
        for _ in range(n):
            add_j = add_sleep(2, sleep)

            submit_flow(add_j, worker=worker_name)
            job_ids.append(add_j.uuid)
        return job_ids

    proj = job_controller.project
    worker_name = "test_batch_multi_remote_worker"

    job_ids = submit_jobs(2, sleep=60)

    daemon_manager.start()
    wait_daemon_started(daemon_manager)

    for _ in range(20):
        if (
            len(
                job_controller.get_jobs_info(
                    job_ids=list(zip(job_ids, [1] * len(job_ids))),
                    states=JobState.BATCH_RUNNING,
                )
            )
            == 2
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError(
            "The submitted jobs were never both running at the same time"
        )

    run_check_cli(
        ["batch", "list"],
        required_out="Running batches info",
    )
    if request.node.callspec.id == "with_batches_collection":
        run_check_cli(
            ["batch", "list", "-a", "-v"], required_out=["Batches info", *job_ids]
        )

    daemon_manager.shut_down()
    wait_daemon_shutdown(daemon_manager)

    assert (
        len(
            job_controller.get_jobs_info(
                job_ids=list(zip(job_ids, [1] * len(job_ids))),
                states=JobState.BATCH_RUNNING,
            )
        )
        == 2
    )

    # check that jobs were submitted only for the correct worker
    # (a bug submitted jobs for the wrong worker as well)
    full_dict_batch_processes = job_controller.get_batch_processes()
    for batch_worker_name, submitted_jobs in full_dict_batch_processes.items():
        n_submitted = 1 if batch_worker_name == worker_name else 0
        assert (
            len(submitted_jobs) == n_submitted
        ), f"wrong number of jobs for worker {batch_worker_name}"

    worker = proj.workers[worker_name]
    host = worker.get_host()
    host.connect()
    batch_manager = RemoteBatchManager(host, worker.batch.jobs_handle_dir)
    queue_manager = QueueManager(worker.get_scheduler_io(), host)
    dict_batch_processes = job_controller.get_batch_processes(worker_name)
    assert len(dict_batch_processes[worker_name]) == 1
    assert (
        queue_manager.cancel(next(iter(dict_batch_processes[worker_name]))).status
        == CancelStatus.SUCCESSFUL
    )
    assert len(queue_manager.get_jobs_list()) == 0

    running_jobs = batch_manager.get_running()
    assert len(running_jobs) == 2
    batch_ids = {batch_uid for _, _, batch_uid in running_jobs}
    assert len(batch_ids) == 1
    batch_uid = next(iter(batch_ids))
    assert check_valid_uuid(batch_uid)

    if request.node.callspec.id == "with_batches_collection":
        assert len(job_controller.get_all_batches()) == 1

        assert len(job_controller.get_all_batches(batch_state=BatchState.FINISHED)) == 0
        assert (
            len(job_controller.get_all_batches(batch_state=[BatchState.RUNNING])) == 1
        )
    else:
        assert job_controller.get_all_batches() is None

    # now restart the runner and verify that the job is set to remote error
    # and running files are properly cleaned
    daemon_manager.start()
    wait_daemon_started(daemon_manager)
    for _ in range(20):
        if all(
            ji.state == JobState.REMOTE_ERROR
            for ji in job_controller.get_jobs_info(
                job_ids=list(zip(job_ids, [1] * len(job_ids)))
            )
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError("The Jobs were not set to REMOTE_ERROR state")

    run_check_cli(["batch", "list"], required_out="No batch processes running")
    if request.node.callspec.id == "with_batches_collection":
        run_check_cli(
            ["batch", "list", "-a"],
            required_out=["Batches info", "Status"],
        )

    assert len(batch_manager.get_running()) == 0
    assert len(batch_manager.get_terminated()) == 0
    assert len(batch_manager.get_submitted()) == 0
    assert len(batch_manager.get_running()) == 0
    all_batches = job_controller.get_all_batches()
    if request.node.callspec.id == "with_batches_collection":
        assert len(all_batches) == 1
        assert len(job_controller.get_all_batches(batch_state=BatchState.FINISHED)) == 1
    else:
        assert all_batches is None

    # submit more jobs, will also be used to check that the files are cleaned during the reset
    job_ids = submit_jobs(4, 15)

    for _ in range(20):
        if (
            len(
                job_controller.get_jobs_info(
                    job_ids=list(zip(job_ids, [1] * len(job_ids))),
                    states=JobState.BATCH_RUNNING,
                )
            )
            == 2
        ):
            break
        time.sleep(1)
    else:
        raise RuntimeError(
            "The submitted jobs were never both running at the same time"
        )

    run_check_cli(
        ["batch", "list"],
        required_out="Running batches info",
    )

    daemon_manager.shut_down()
    wait_daemon_shutdown(daemon_manager)

    assert (
        len(
            job_controller.get_jobs_info(
                job_ids=list(zip(job_ids, [1] * len(job_ids))),
                states=JobState.BATCH_RUNNING,
            )
        )
        == 2
    )
    assert (
        len(
            job_controller.get_jobs_info(
                job_ids=list(zip(job_ids, [1] * len(job_ids))),
                states=JobState.BATCH_SUBMITTED,
            )
        )
        == 2
    )

    assert len(batch_manager.get_running()) == 2

    # now reset the DB, the files should also be cleaned up
    job_controller.reset()
    assert len(batch_manager.get_terminated()) == 0
    assert len(batch_manager.get_submitted()) == 0
    assert len(batch_manager.get_running()) == 0

    if request.node.callspec.id == "with_batches_collection":
        assert len(job_controller.get_all_batches()) == 0


@pytest.mark.workers(["test_max_jobs_worker"])
def test_max_jobs_worker(
    job_controller, daemon_manager, wait_daemon_started, wait_daemon_shutdown
) -> None:
    import time

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_sleep

    # run the daemon in background to check what happens to the
    # jobs during the execution
    daemon_manager.start(raise_on_error=True)
    wait_daemon_started(daemon_manager)

    job_ids = []
    for _ in range(4):
        j = add_sleep(2, 20)
        job_ids.append((j.uuid, 1))
        flow = Flow([j])
        submit_flow(flow, worker="test_max_jobs_worker")

    t0 = time.time()

    def check_running_jobs(seconds):
        finished_states = (JobState.REMOTE_ERROR, JobState.FAILED, JobState.COMPLETED)
        running_states = (JobState.RUNNING, JobState.SUBMITTED)

        max_running_jobs = 0
        for _ in range(seconds):
            time.sleep(1)
            jobs_info = job_controller.get_jobs_info(job_ids=job_ids)
            if all(ji.state in finished_states for ji in jobs_info):
                break
            current_running = sum(ji.state in running_states for ji in jobs_info)
            max_running_jobs = max(max_running_jobs, current_running)
        return max_running_jobs

    max_running_jobs = check_running_jobs(4)

    assert job_controller.count_jobs(states=JobState.RUNNING) == 2
    assert job_controller.count_jobs(states=JobState.UPLOADED) == 2

    # the max running jobs should be two, meaning that it was reached and cannot
    # be larger. The check could be <= 2, but if it does not reach two it will
    # not be testing some parts of the code and the test is not complete.
    assert max_running_jobs == 2

    # now stop the runner and restart it, so it can check that upon restart the current
    # running jobs are correctly taken into account
    daemon_manager.shut_down(raise_on_error=True)
    wait_daemon_shutdown(daemon_manager)
    daemon_manager.start(raise_on_error=True)
    wait_daemon_started(daemon_manager)

    if time.time() - t0 > 15:
        raise RuntimeError(
            "The execution of the first part of the test took too long (probably "
            "the restart of the runner) and the test will not be reliable. "
            "Consider repeating it or increasing the job sleep time"
        )

    max_running_jobs = check_running_jobs(60)
    assert max_running_jobs == 2

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 4
