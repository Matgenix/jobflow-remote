import pytest


def test_reset(job_controller, one_job) -> None:
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["admin", "reset"],
        required_out="The database was reset",
        cli_input="y",
    )
    assert job_controller.count_jobs() == 0

    for _ in range(26):
        f = Flow(add(1, 2))
        submit_flow(f, worker="test_local_worker")

    run_check_cli(
        ["admin", "reset"],
        required_out="The database was NOT reset",
        cli_input="y",
    )
    assert job_controller.count_jobs() == 26

    run_check_cli(["admin", "reset"], cli_input="n")
    assert job_controller.count_jobs() == 26

    run_check_cli(["admin", "reset", "1220-01-01"], cli_input="y")
    assert job_controller.count_jobs() == 26

    run_check_cli(
        ["admin", "reset", datetime.now().strftime("%Y-%m-%d")],
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


def test_running_runner(job_controller) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    with job_controller.lock_auxiliary(filter={"running_runner": {"$exists": True}}):
        run_check_cli(
            ["admin", "unlock-runner"],
            required_out="The runner document has been unlocked",
        )

    run_check_cli(
        ["admin", "unlock-runner"],
        required_out="The runner document was not locked. Nothing changed.",
    )

    job_controller.auxiliary.delete_many({"running_runner": {"$exists": True}})
    run_check_cli(
        ["admin", "unlock-runner"],
        required_out=[
            "No runner document... ",
            "Consider upgrading your database using 'jf admin upgrade'",
        ],
    )
    job_controller.auxiliary.insert_one({"running_runner": None})
    job_controller.auxiliary.insert_one({"running_runner": None})
    run_check_cli(
        ["admin", "unlock-runner"],
        required_out=[
            "2 runner documents found...",
            "There should be only one runner document.",
            "Consider fixing this problem (manually)...",
        ],
    )


def test_upgrade_from_before_0_1_3(job_controller, version_candidate, caplog) -> None:
    import jobflow_remote
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.get_running_runner() is None
    job_controller.auxiliary.delete_many({"running_runner": {"$exists": True}})
    with pytest.raises(TypeError, match=r"'NoneType' object is not subscriptable"):
        assert job_controller.get_running_runner()

    job_controller.auxiliary.delete_many({"jobflow_remote_version": {"$exists": True}})

    run_check_cli(
        ["admin", "upgrade", "--test-version-upgrade", "0.1.3"],
        required_out=[
            "Possible issues in upgrade of the database.",
            "No information about jobflow version in the database.",
            "The database is likely from before version 0.1.3 of jobflow-remote.",
            "No information about environment (all packages versions) in the database.",
            "The database is likely from before version 0.1.3 of jobflow-remote.",
            "Carefully check this is ok and use 'jf admin upgrade --force'",
        ],
        error=True,
    )
    with pytest.raises(TypeError, match=r"'NoneType' object is not subscriptable"):
        assert job_controller.get_running_runner()
    running_runner_doc = job_controller.auxiliary.find_one(
        {"running_runner": {"$exists": True}}
    )
    assert running_runner_doc is None
    versions_info = job_controller.auxiliary.find_one(
        {"jobflow_remote_version": {"$exists": True}}
    )
    assert versions_info is None
    run_check_cli(
        ["admin", "upgrade", "--test-version-upgrade", "0.1.3", "--force"],
        required_out=["The database has been upgraded"],
    )
    running_runner_doc = job_controller.auxiliary.find_one(
        {"running_runner": {"$exists": True}}
    )
    assert running_runner_doc is not None
    versions_info = job_controller.auxiliary.find_one(
        {"jobflow_remote_version": {"$exists": True}}
    )
    assert versions_info is not None
    assert versions_info["jobflow_remote_version"] == jobflow_remote.__version__
    assert job_controller.get_running_runner() is None


def test_index_rebuild(job_controller, one_job):
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.count_jobs() == 1

    # use foreground to avoid checking before the DB created the index
    run_check_cli(
        ["admin", "index", "rebuild", "-fg"],
        required_out="Indexes rebuilt",
    )
    job_indexes = list(job_controller.jobs.list_indexes())
    flows_indexes = list(job_controller.flows.list_indexes())
    assert len(job_indexes) == 11
    assert len(flows_indexes) == 7


def test_index_create(job_controller, one_job):
    from jobflow_remote.testing.cli import run_check_cli

    assert job_controller.count_jobs() == 1

    run_check_cli(
        ["admin", "index", "create", "-fg", "test_ind", "desc"],
        required_out="Index created",
    )

    run_check_cli(
        ["admin", "index", "create", "-c", "flows", "-fg", "test_ind_2"],
        required_out="Index created",
    )

    job_indexes = list(job_controller.jobs.list_indexes())
    flows_indexes = list(job_controller.flows.list_indexes())
    assert len(job_indexes) == 12
    assert len(flows_indexes) == 8

    run_check_cli(
        ["admin", "index", "rebuild", "-fg"],
        required_out="Indexes rebuilt",
    )

    job_indexes = list(job_controller.jobs.list_indexes())
    flows_indexes = list(job_controller.flows.list_indexes())
    assert len(job_indexes) == 11
    assert len(flows_indexes) == 7
