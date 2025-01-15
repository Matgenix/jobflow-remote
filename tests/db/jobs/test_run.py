def test_current_jobdoc(job_controller, runner):
    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.run import CURRENT_JOBDOC, JfrState
    from jobflow_remote.testing import current_jobdoc

    j = current_jobdoc()
    submit_flow([j], worker="test_local_worker")
    runner.run_one_job()

    job_output = job_controller.jobstore.get_output(uuid=j.uuid)
    job_doc = job_controller.get_job_doc(job_id=j.uuid).as_db_dict()
    for k in job_doc:
        # some keys do not match
        if k not in (
            "state",
            "end_time",
            "start_time",
            "updated_on",
            "remote",
            "run_dir",
            "created_on",
        ):
            assert job_doc[k] == job_output[k]

    # check that CURRENT_JOBDOC is a singleton and can be set
    s = JfrState()
    assert s.job_doc is None
    assert CURRENT_JOBDOC.job_doc is None
    s.job_doc = job_doc
    assert CURRENT_JOBDOC.job_doc == job_doc
    s.reset()
    assert CURRENT_JOBDOC.job_doc is None
