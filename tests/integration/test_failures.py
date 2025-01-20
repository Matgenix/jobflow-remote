import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)

# only test with local and slurm. Other queue managers should not make a difference
WORKERS = ["test_local_worker", "test_remote_slurm_worker"]

MAX_TRY_SECONDS = 120


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_missing_store_files(worker, job_controller, runner):
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker=worker)

    # if the worker is local the TERMINATED state is skipped
    # get the Job after the RUNNING state
    target_state = (
        JobState.DOWNLOADED
        if runner.workers[worker].type == "local"
        else JobState.TERMINATED
    )
    assert runner.run_one_job(
        max_seconds=10, job_id=[j.uuid, j.index], target_state=target_state
    )

    # remove the file with the output store
    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    host = runner.get_host(worker)
    host.remove(str(Path(j_info.run_dir) / "remote_job_data.json"))

    # Finish the Job. It should fail during completion, not during download
    assert runner.run_one_job(max_seconds=10, job_id=[j.uuid, j.index])
    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.FAILED
    assert (
        "output store file remote_job_data.json is missing in the downloaded folder"
        in j_info.error
    )


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_queue_files(worker, job_controller, runner, monkeypatch):
    from pathlib import Path
    from typing import NoReturn

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.remote.data import get_local_data_path
    from jobflow_remote.testing import add, always_fails

    w = job_controller.project.workers[worker]

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker=worker, exec_config="some_pre_run")

    assert runner.run_one_job(max_seconds=20, job_id=[j.uuid, j.index])

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    local_data_path = Path(
        get_local_data_path(
            job_controller.project,
            worker=w,
            job_id=j.uuid,
            index=j.index,
            run_dir=j_info.run_dir,
        )
    )

    # since it complete correctly there should be no queue_out and queue_err data
    assert j_info.remote.queue_out is None
    assert j_info.remote.queue_err is None

    # If remote the local path should be removed in case of completed job.
    # For a local worker the local path is equivalent to the run dir and should not be removed
    if w.is_local:
        assert local_data_path.exists()
    else:
        assert not local_data_path.exists()

    j = always_fails()
    flow = Flow([j])
    submit_flow(flow, worker=worker, exec_config="some_pre_run")

    assert runner.run_one_job(max_seconds=20, job_id=[j.uuid, j.index])

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    local_data_path = Path(
        get_local_data_path(
            job_controller.project,
            worker=w,
            job_id=j.uuid,
            index=j.index,
            run_dir=j_info.run_dir,
        )
    )

    # since it fails these should be present
    assert "This is a pre_run" in j_info.remote.queue_out
    assert "This is a pre_run" in j_info.remote.queue_err

    if w.is_local:
        assert local_data_path.exists()
    else:
        assert not local_data_path.exists()

    j = always_fails()
    flow = Flow([j])
    submit_flow(flow, worker=worker, exec_config="long_pre_run")

    with monkeypatch.context() as m:
        # avoid deleting the tmp folder, so it is always present
        m.setattr(runner.runner_options, "delete_tmp_folder", False)
        assert runner.run_one_job(max_seconds=20, job_id=[j.uuid, j.index])

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    local_data_path = Path(
        get_local_data_path(
            job_controller.project,
            worker=w,
            job_id=j.uuid,
            index=j.index,
            run_dir=j_info.run_dir,
        )
    )

    # since it fails these should be present. Check that it is cut
    assert "XXXXX" in j_info.remote.queue_out
    assert "The content was cut." in j_info.remote.queue_out
    assert len(j_info.remote.queue_out) < 3500
    assert "XXXXX" in j_info.remote.queue_err
    assert "The content was cut." in j_info.remote.queue_err
    assert len(j_info.remote.queue_err) < 3500

    assert local_data_path.exists()

    # When rerunning the folder should be deleted, in case it is a remote worker
    assert job_controller.rerun_job(db_id=j_info.db_id)
    if w.is_local:
        assert local_data_path.exists()
    else:
        assert not local_data_path.exists()

    # check that the queue_out/err was cleaned up
    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.remote.queue_out is None
    assert j_info.remote.queue_err is None

    # Now run one that finishes but end up in REMOTE_ERROR during completion
    # (it is easier to do that during completion with respect to
    # download) and check that it is queue_out/queue_err are still added

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker=worker, exec_config="some_pre_run")

    # patch the complete method of the runner to trigger a remote error
    def complete_error(self, lock) -> NoReturn:
        raise RuntimeError("FAKE ERROR")

    with monkeypatch.context() as m:
        m.setattr(Runner, "complete_job", complete_error)
        # patch this to 1 to avoid retrying multiple times
        m.setattr(runner.runner_options, "max_step_attempts", 1)
        with pytest.warns(match="FAKE ERROR"):
            assert runner.run_one_job(max_seconds=20, job_id=[j.uuid, j.index])

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.REMOTE_ERROR

    assert "This is a pre_run" in j_info.remote.queue_out
    assert "This is a pre_run" in j_info.remote.queue_err
