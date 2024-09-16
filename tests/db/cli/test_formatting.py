def test_job_info():
    from jobflow_remote.cli.formatting import JOB_INFO_ORDER
    from jobflow_remote.jobs.data import JobDoc, JobInfo

    # check that all the keys in JobInfo and JobDoc are present in JOB_INFO_ORDER
    assert set(JOB_INFO_ORDER).issuperset(set(JobInfo.model_fields.keys()))
    assert set(JOB_INFO_ORDER).issuperset(set(JobDoc.model_fields.keys()))
