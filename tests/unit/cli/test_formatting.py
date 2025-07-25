from jobflow_remote.cli.formatting import get_job_info_table


def test_get_job_info_table():
    import datetime

    from jobflow_remote.jobs.data import JobInfo
    from jobflow_remote.jobs.state import JobState

    ji1 = JobInfo(
        uuid="fed516ab-796d-4904-85d6-7f933794b344",
        index=1,
        db_id="1",
        worker="test_worker",
        name="job1",
        state=JobState.READY,
        created_on=datetime.datetime.utcnow(),
        updated_on=datetime.datetime.utcnow(),
        hosts=["5e337e69-b153-45e0-82f2-4a7af8b45e44"],
    )

    ji2 = JobInfo(
        uuid="15b34018-9401-4b83-83e6-523e027bae29",
        index=1,
        db_id="2",
        worker="another_worker",
        name="job2",
        state=JobState.COMPLETED,
        created_on=datetime.datetime.utcnow(),
        updated_on=datetime.datetime.utcnow(),
        hosts=["5e337e69-b153-45e0-82f2-4a7af8b45e44"],
    )

    ji3 = JobInfo(
        uuid="d8e27025-7a7e-48d9-9528-0db31985536e",
        index=1,
        db_id="2",
        worker="another_worker",
        name="job3",
        state=JobState.COMPLETED,
        created_on=datetime.datetime.utcnow(),
        updated_on=datetime.datetime.utcnow(),
        hosts=["d490d691-8c96-4e93-93c0-8bb2a45e6746"],
    )

    table = get_job_info_table(jobs_info=[ji1, ji2, ji3], verbosity=0, color=True)
    cells = list(table.columns[1].cells)
    assert cells[0].plain == "job1"
    assert cells[0].style == "red"
    assert cells[1].plain == "job2"
    assert cells[1].style == "red"
    assert cells[2].plain == "job3"
    assert cells[2].style == "green"
