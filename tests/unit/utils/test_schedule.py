import logging


def test_scheduler(mocker, caplog):
    import datetime

    from jobflow_remote.utils.schedule import SafeScheduler

    def one_time_function():
        if hasattr(one_time_function, "_has_been_called"):
            raise RuntimeError("This function can only be called once.")
        one_time_function._has_been_called = True

    from schedule import Job

    # Patch the should_run so that we don't have to wait
    mocker.patch.object(Job, "should_run", property(lambda self: True))

    # First test without rescheduling when function fails
    scheduler = SafeScheduler(reschedule_on_failure=False)
    run_job_spy = mocker.spy(scheduler, "_run_job")
    cancel_job_spy = mocker.spy(scheduler, "cancel_job")
    schedule_next_run_spy = mocker.spy(Job, "_schedule_next_run")

    jb = scheduler.every(1).seconds.do(one_time_function)
    assert schedule_next_run_spy.call_count == 1
    schedule_next_run_spy.reset_mock()

    assert len(scheduler.jobs) == 1
    with caplog.at_level(logging.WARNING, logger="jobflow_remote.utils.schedule"):
        scheduler.run_pending()
    assert len(caplog.records) == 0
    assert len(scheduler.jobs) == 1
    assert schedule_next_run_spy.call_count == 1
    schedule_next_run_spy.reset_mock()
    caplog.clear()

    with caplog.at_level(logging.WARNING, logger="jobflow_remote.utils.schedule"):
        scheduler.run_pending()
    assert len(caplog.records) == 2
    assert caplog.records[0].levelname == "ERROR"
    assert caplog.records[0].message == "Error while running task one_time_function"
    assert caplog.records[1].levelname == "WARNING"
    assert caplog.records[1].message == "Task one_time_function canceled."
    assert len(scheduler.jobs) == 0
    assert run_job_spy.call_count == 2
    schedule_next_run_spy.assert_not_called()
    schedule_next_run_spy.reset_mock()
    cancel_job_spy.assert_called_once_with(jb)
    caplog.clear()

    # Second test with rescheduling when function fails
    scheduler = SafeScheduler(reschedule_on_failure=True)
    run_job_spy = mocker.spy(scheduler, "_run_job")
    cancel_job_spy = mocker.spy(scheduler, "cancel_job")

    scheduler.every(12).seconds.do(one_time_function)
    assert schedule_next_run_spy.call_count == 1
    schedule_next_run_spy.reset_mock()

    assert len(scheduler.jobs) == 1
    with caplog.at_level(logging.WARNING, logger="jobflow_remote.utils.schedule"):
        scheduler.run_pending()
    assert len(caplog.records) == 2
    assert len(scheduler.jobs) == 1
    assert schedule_next_run_spy.call_count == 1
    schedule_next_run_spy.reset_mock()

    assert caplog.records[0].levelname == "ERROR"
    assert caplog.records[0].message == "Error while running task one_time_function"
    assert caplog.records[1].levelname == "WARNING"
    assert caplog.records[1].message == "Task one_time_function rescheduled"
    assert len(scheduler.jobs) == 1
    assert run_job_spy.call_count == 1
    cancel_job_spy.assert_not_called()
    caplog.clear()

    # Third test with rescheduling after some time
    scheduler = SafeScheduler(reschedule_on_failure=True, seconds_after_failure=7)
    run_job_spy = mocker.spy(scheduler, "_run_job")
    cancel_job_spy = mocker.spy(scheduler, "cancel_job")

    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2001, 12, 31, 14, 2, 13)

    mocker.patch("jobflow_remote.utils.schedule.datetime", MockDateTime)

    scheduler.every(12).seconds.do(one_time_function)
    assert schedule_next_run_spy.call_count == 1
    schedule_next_run_spy.reset_mock()

    assert len(scheduler.jobs) == 1
    with caplog.at_level(logging.WARNING, logger="jobflow_remote.utils.schedule"):
        scheduler.run_pending()
    assert len(caplog.records) == 2
    assert len(scheduler.jobs) == 1
    assert schedule_next_run_spy.call_count == 0
    schedule_next_run_spy.reset_mock()

    assert caplog.records[0].levelname == "ERROR"
    assert caplog.records[0].message == "Error while running task one_time_function"
    assert caplog.records[1].levelname == "WARNING"
    assert (
        caplog.records[1].message == "Task one_time_function rescheduled in 7 seconds"
    )
    assert len(scheduler.jobs) == 1
    assert run_job_spy.call_count == 1
    cancel_job_spy.assert_not_called()
    assert scheduler.jobs[0].next_run == datetime.datetime(2001, 12, 31, 14, 2, 20)
    caplog.clear()
