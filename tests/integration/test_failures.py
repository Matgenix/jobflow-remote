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
    submit_flow(flow, worker="test_local_worker")

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
