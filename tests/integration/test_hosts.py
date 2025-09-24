import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)

WORKERS = [
    "test_local_worker",
    "test_remote_slurm_worker",
]


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_hosts(worker, job_controller) -> None:
    my_worker = job_controller.project.workers[worker]
    work_dir = my_worker.work_dir
    host = my_worker.get_host()

    host.write_text_file(work_dir / "somefile.txt", "somecontent")
    content = host.read_text_file(work_dir / "somefile.txt")

    assert content == "somecontent"
