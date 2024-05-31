import pytest


def test_reset(job_controller, four_jobs) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["admin", "reset", "-m", "1"],
        required_out="The database was NOT reset",
        cli_input="y",
    )
    assert job_controller.count_jobs() == 4

    run_check_cli(["admin", "reset", "-m", "10"], cli_input="n")
    assert job_controller.count_jobs() == 4

    run_check_cli(
        ["admin", "reset", "-m", "10"],
        required_out="The database was reset",
        cli_input="y",
    )
    assert job_controller.count_jobs() == 0


def test_unlock(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    j = one_job.jobs[0]
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_job(filter={"uuid": j.uuid}),
    ):
        run_check_cli(
            ["admin", "unlock", "-did", "1"],
            required_out="1 jobs were unlocked",
            cli_input="y",
        )

    with job_controller.lock_job(filter={"uuid": j.uuid}):
        run_check_cli(
            ["admin", "unlock", "-did", "1"],
            excluded_out="1 jobs were unlocked",
            cli_input="n",
        )

    run_check_cli(
        ["admin", "unlock", "-did", "10"],
        required_out="No data matching the request",
        error=True,
    )


def test_unlock_flow(job_controller, one_job) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_flow(filter={"uuid": one_job.uuid}),
    ):
        run_check_cli(
            ["admin", "unlock-flow", "-fid", one_job.uuid],
            required_out="1 flows were unlocked",
            cli_input="y",
        )

    with job_controller.lock_flow(filter={"uuid": one_job.uuid}):
        run_check_cli(
            ["admin", "unlock-flow", "-fid", one_job.uuid],
            excluded_out="1 flows were unlocked",
            cli_input="n",
        )

    run_check_cli(
        ["admin", "unlock-flow", "-fid", "xxxx"],
        required_out="No data matching the request",
        error=True,
    )
