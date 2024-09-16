def test_jobs_report(job_controller, two_flows_four_jobs):
    from datetime import datetime

    import dateutil

    from jobflow_remote.jobs.report import JobsReport
    from jobflow_remote.jobs.runner import Runner

    tzname = datetime.now(dateutil.tz.tzlocal()).tzname()

    runner = Runner()
    runner.run_one_job()

    report = JobsReport.generate_report(job_controller=job_controller, timezone=tzname)
    trends = report.trends
    assert trends.interval == "days"
    assert trends.num_intervals == 7
    assert len(trends.dates) == 7
    assert trends.completed[0] == 0
    assert trends.completed[-1] == 1
    assert trends.failed[-1] == 0
    assert trends.remote_error[-1] == 0

    assert report.completed == 1
    assert report.running == 0
    assert report.error == 0
    assert report.active == 0

    assert report.longest_running == []

    assert report.worker_utilization["test_local_worker"] == 4
