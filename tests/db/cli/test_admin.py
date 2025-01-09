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


@pytest.mark.filterwarnings("ignore:Could not release lock for document")
def test_unlock_runner(job_controller) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    with job_controller.lock_auxiliary(filter={"running_runner": {"$exists": True}}):
        rr_before = job_controller.auxiliary.find_one(
            {"running_runner": {"$exists": True}}
        )
        assert rr_before.get("lock_id") is not None
        run_check_cli(
            ["admin", "unlock-runner"],
            required_out="The runner document has been unlocked",
        )
        rr_after = job_controller.auxiliary.find_one(
            {"running_runner": {"$exists": True}}
        )
        assert rr_after.get("lock_id") is None

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


def test_upgrade(job_controller, upgrade_test_dir, random_project_name) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    # Test upgrading from development version. Explicitly pass such a target version
    # This is the case if the target version is not specified and the code installed
    # from source. No upgrade performed here.
    run_check_cli(
        [
            "admin",
            "upgrade",
            "--target",
            "0.1.4.post95+gc325f4e.d20250103",
        ],
        cli_input="wrong_project_name",
        required_out=[
            "Target version 0.1.4.post95+gc325f4e.d20250103 is likely a development version. "
            "Explicitly specify the target version with the --target option if this is the case. "
            "Available upgrades larger than 0.1.4: 0.1.5",
            "No upgrade required for target version 0.1.4.post95+gc325f4e.d20250103",
        ],
    )


def test_upgrade_to_0_1_5(
    job_controller, upgrade_test_dir, random_project_name
) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    job_controller.backup_restore(upgrade_test_dir / "0.1.5", python=True)

    assert job_controller.count_jobs() == 1
    assert job_controller.count_flows() == 1

    assert job_controller.get_running_runner() == "NO_DOCUMENT"
    assert str(job_controller.get_current_db_version()) == "0.1.0"

    run_check_cli(
        ["admin", "upgrade", "--target", "0.1.5"],
        cli_input="wrong_project_name",
        required_out=[
            "No information about jobflow version in the database.",
            "The database is likely from before version 0.1.5 of jobflow-remote.",
            "In order to upgrade the DB to version 0.1.5 the following actions will be performed:",
            "Create a document for the running runner in the auxiliary collection",
            "Update database version number to 0.1.5",
            "It is advisable to perform a backup before proceeding",
            f"Insert the name of the project ({random_project_name}) to confirm that you want to proceed",
        ],
        error=True,
    )
    assert job_controller.get_running_runner() == "NO_DOCUMENT"
    versions_info = job_controller.auxiliary.find_one(
        {"jobflow_remote_version": {"$exists": True}}
    )
    assert versions_info is None
    run_check_cli(
        ["admin", "upgrade", "--target", "0.1.5"],
        cli_input=random_project_name,
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
    assert versions_info["jobflow_remote_version"] == "0.1.5"
    assert job_controller.get_running_runner() is None

    assert job_controller.count_jobs() == 1
    assert job_controller.count_flows() == 1

    # test upgrading again to check that it will not perform the upgrade
    run_check_cli(
        ["admin", "upgrade", "--target", "0.1.5"],
        required_out=[
            "Current DB version: 0.1.5. No upgrade required for target version 0.1.5"
        ],
    )


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
