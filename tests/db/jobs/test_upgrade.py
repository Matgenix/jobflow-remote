def test_upgrade_conditions(
    job_controller,
    two_flows_four_jobs,
    caplog,
):
    import packaging.version

    from jobflow_remote import __version__
    from jobflow_remote.jobs.upgrade import (
        DatabaseUpgrader,
        NoDocumentsIn,
        UpgradeCondition,
    )

    package_version = packaging.version.parse(__version__)
    assert job_controller.get_current_db_version() == package_version

    # Register a fake upgrade to test the upgrade conditions mechanism
    def fake_check(jc, cond):
        doc = jc.auxiliary.find_one({"next_id": {"$exists": True}})
        count = doc["next_id"]
        return {
            "condition": cond,
            "message": f"Found next_id = {count}",
            "count": count,
        }

    @DatabaseUpgrader.register_upgrade(
        version="98.0",
        upgrade_conditions=[
            UpgradeCondition(
                description="next_id",
                check_func=fake_check,
            )
        ],
    )
    def upgrade_to_98(*_, **__):
        return []

    @DatabaseUpgrader.register_upgrade(
        version="99.0",
        upgrade_conditions=[
            NoDocumentsIn(
                description="no_jobs_ready",
                collection="jobs",
                query={"state": "READY"},
            ),
            NoDocumentsIn(
                collection="flows",
            ),
            NoDocumentsIn(
                collection="batches",
            ),
        ],
    )
    def upgrade_to_99(*_, **__):
        return []

    db_upgrader = DatabaseUpgrader(job_controller)
    v98 = packaging.version.parse("98.0")
    v99 = packaging.version.parse("99.0")
    failed_conditions = db_upgrader.check_upgrade_conditions(versions=[v98, v99])
    assert len(failed_conditions) == 3
    version1, failed_condition1 = failed_conditions[0]
    version2, failed_condition2 = failed_conditions[1]
    version3, failed_condition3 = failed_conditions[2]
    assert version1 == v98
    assert failed_condition1["condition"].description == "next_id"
    assert failed_condition1["count"] == 5
    assert version2 == v99
    assert failed_condition2["condition"].description == "no_jobs_ready"
    assert failed_condition2["count"] == 2
    assert version3 == v99
    assert (
        failed_condition3["condition"].description
        == "There should be no document in the 'flows' collection"
    )
    assert failed_condition3["count"] == 2

    with caplog.at_level("ERROR"):
        assert db_upgrader.upgrade(from_version=None, target_version="99.0") is False
    assert job_controller.get_current_db_version() == package_version
    assert "Some upgrade conditions were not satisfied:" in caplog.text
    assert " - next_id (for version 98.0): Found next_id = 5" in caplog.text
    assert " - no_jobs_ready (for version 99.0): Found 2 document(s)" in caplog.text
    assert (
        " - There should be no document in the 'flows' collection (for version 99.0): Found 2 document(s)"
        in caplog.text
    )
