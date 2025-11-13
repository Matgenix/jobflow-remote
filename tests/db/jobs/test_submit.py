import pytest


def test_submit_optional_jobstore(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])

    with pytest.raises(
        ValueError, match=".*No JobStore named wrong_jobstore defined in the project.*"
    ):
        submit_flow(flow, worker="test_local_worker", jobstore="wrong_jobstore")

    submit_flow(flow, worker="test_local_worker", jobstore="other_jobstore")

    runner.run_all_jobs(max_seconds=20)
    assert job_controller.count_jobs(states=JobState.COMPLETED) == 2

    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(add_first.uuid)
    assert (
        job_controller.optional_jobstores["other_jobstore"].get_output(add_first.uuid)
        == 6
    )

    assert job_controller.get_job_output(db_id="2") == 11
