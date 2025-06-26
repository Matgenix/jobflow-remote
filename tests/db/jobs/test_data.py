def test_jobdoc_serialization(job_controller, runner) -> None:
    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import EnumMaker, TestEnum

    job = EnumMaker().make()
    assert isinstance(job.maker.e, TestEnum)
    submit_flow(job, worker="test_local_worker")

    jobdoc_from_db = job_controller.get_jobs_doc()[0]
    assert isinstance(jobdoc_from_db.job.maker.e, TestEnum)

    runner.run_all_jobs(max_seconds=10)

    jobdoc_from_db = job_controller.get_jobs_doc()[0]
    assert jobdoc_from_db.state == JobState.COMPLETED
