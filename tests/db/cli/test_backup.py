import tempfile
from shutil import which

import pytest


def check_files(files: list[str], meta: bool, compress: bool):
    ext = ".gz" if compress else ""
    assert "flows.bson" + ext in files
    assert "jf_auxiliary.bson" + ext in files
    assert "jobs.bson" + ext in files
    if meta:
        assert "flows.metadata.json" + ext in files
        assert "jf_auxiliary.metadata.json" + ext in files
        assert "jobs.metadata.json" + ext in files

        assert len(files) == 6
    else:
        assert len(files) == 3


@pytest.mark.parametrize(
    "python",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                not which("mongodump"), reason="mongodump missing"
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "compress",
    [True, False],
)
def test_reset(job_controller, two_flows_four_jobs, python, compress) -> None:
    from pathlib import Path

    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.count_jobs() == 4
    db_name = job_controller.queue_store.database

    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)
        required_out = [
            "Backup created",
            "flows collection: 2 documents",
            "jf_auxiliary collection: 3 documents",
            "jobs collection: 4 documents",
        ]
        cmd = ["backup", "create"]
        if compress:
            cmd.append("--compress")
        if python:
            cmd.append("--python")
        cmd.append(dir_name)
        run_check_cli(cmd, required_out=required_out)
        files = [str(p.name) for p in (dir_path / db_name).glob("*")]
        check_files(files, meta=not python, compress=compress)
        job_controller.reset(max_limit=0)

        assert job_controller.count_jobs() == 0

        cmd = ["backup", "restore"]
        if python:
            cmd.append("--python")
        cmd.append(str(dir_path / db_name))
        run_check_cli(cmd, required_out="Backup restored")

        assert job_controller.count_jobs() == 4
        assert job_controller.count_flows() == 2
        aux_docs = list(job_controller.auxiliary.find({}))
        assert len(aux_docs) == 3
        assert aux_docs[0]["next_id"] == 5
