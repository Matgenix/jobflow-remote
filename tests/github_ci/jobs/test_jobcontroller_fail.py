import pytest


def test_submit_flow_fail(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker="test_local_worker")

    runner.run_all_jobs(max_seconds=10)

    assert len(job_controller.get_jobs({})) == 2
    job_1, job_2 = job_controller.get_jobs({})
    assert job_1["job"]["function_args"] == [1, 5]
    assert job_1["job"]["name"] == "add"

    output_1 = job_controller.jobstore.get_output(uuid=job_1["uuid"])
    assert output_1 == 6
    output_2 = job_controller.jobstore.get_output(uuid=job_2["uuid"])
    assert output_2 == 11
    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 2
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    # Number of completed flows is 1. Explicitly make this test fail to test the save db artifact when an
    # error occurs.
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 0
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


@pytest.mark.parametrize(["x", "y"], [(1, 2), (3, 4)])
@pytest.mark.parametrize("z", [5, 6, 7])
def test_parametrized_failed(job_controller, runner, x, y, z) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker="test_local_worker")

    if not (z == 6 and x == 1):
        pytest.fail("Explicitly failing this test for testing CI save db artifact.")
