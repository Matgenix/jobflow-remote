def test_upload_cleanup_error(job_controller, runner, monkeypatch):
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    flow = Flow([j1])

    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j1_path = Path(j1_info.run_dir)
    assert j1_info.state == JobState.COMPLETED

    assert set(job_controller.rerun_jobs(job_ids=(j1.uuid, j1.index), force=True)) == {
        j1_info.db_id
    }
    assert j1_path.exists()

    check_file_j1 = j1_path / "test_test.json"
    check_file_j1.touch(exist_ok=True)
    assert check_file_j1.exists()

    # remove the jfremote_in.json and check that the remove fails
    jfremote_in = j1_path / "jfremote_in.json"
    jfremote_in.unlink(missing_ok=True)

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    assert j1_info.state == JobState.REMOTE_ERROR

    # since it fails immediately, step attempts is left to zero
    assert j1_info.remote.step_attempts == 0
    assert (
        "Error while performing cleanup of the run_dir folder for job 1"
        in j1_info.remote.error
    )
    assert (
        f"Could not delete folder {j1_info.run_dir} since it may not contain a jobflow-remote execution."
        in j1_info.remote.error
    )

    # rerun the job create again the file and mock to have it failed in a different way
    job_controller.rerun_job(db_id="1")
    jfremote_in.touch(exist_ok=True)

    def raise_rmtree(*args, **kwargs):
        raise RuntimeError("FAKE ERROR")

    with monkeypatch.context() as m:
        m.setattr(LocalHost, "rmtree", raise_rmtree)
        m.setattr(runner.runner_options, "max_step_attempts", 2)
        assert runner.run_one_job(max_seconds=20, job_id=[j1.uuid, j1.index])

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    assert j1_info.state == JobState.REMOTE_ERROR
    assert j1_info.remote.step_attempts == 2
    assert (
        "Error while performing cleanup of the run_dir folder for job 1"
        in j1_info.remote.error
    )
    assert "FAKE ERROR" in j1_info.remote.error


def test_delay_download(job_controller, runner, monkeypatch, one_job):
    from datetime import datetime

    from jobflow_remote.jobs.state import JobState

    j = one_job.jobs[0]
    # since this is a local worker the state after RUNNING is DOWNLOADED, not TERMINATED
    with monkeypatch.context() as m:
        m.setattr(runner.workers["test_local_worker"], "delay_download", 5)
        assert runner.run_one_job(
            max_seconds=10, job_id=[j.uuid, j.index], target_state=JobState.DOWNLOADED
        )
    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.remote.retry_time_limit is not None
    assert j_info.remote.retry_time_limit > datetime.utcnow()

    # verify that it can properly complete after waiting
    assert runner.run_one_job(max_seconds=20, job_id=[j.uuid, j.index])


def test_ping_runner_runner(job_controller, runner, monkeypatch, caplog):
    from datetime import datetime

    from jobflow_remote.jobs import runner as runner_module

    assert not job_controller.ping_running_runner()
    runner.ping_running_runner()
    # check that the ping does not create the document
    assert job_controller.get_running_runner() is None

    # create a fake running runner document
    t0 = datetime.now()
    job_controller.auxiliary.find_one_and_update(
        {"running_runner": {"$exists": True}},
        {"$set": {"running_runner": {"last_pinged": t0}}},
    )
    # Here there could be a small difference in timings, even if the runner is not started
    assert (
        abs((job_controller.get_running_runner()["last_pinged"] - t0).total_seconds())
        < 0.1
    )
    with monkeypatch.context() as m:
        m.setattr(runner_module, "PING_RUNNER_DELAY", 3)
        runner.run(ticks=5)

    assert (
        abs((job_controller.get_running_runner()["last_pinged"] - t0).total_seconds())
        > 2
    )
