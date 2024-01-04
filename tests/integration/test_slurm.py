import pytest


def test_project_init(random_project_name):
    from jobflow_remote.config import ConfigManager

    cm = ConfigManager()
    assert len(cm.projects) == 1
    assert cm.projects[random_project_name]
    project = cm.get_project()
    assert len(project.workers) == 2


def test_project_check(job_controller, capsys):
    from jobflow_remote.cli.project import check

    check(print_errors=True)
    captured = capsys.readouterr()
    assert not captured.err
    expected = [
        "✓ Worker test_local_worker",
        "✓ Worker test_remote_worker",
        "✓ Jobstore",
        "✓ Queue store",
    ]
    for line in expected:
        assert line in captured.out


@pytest.mark.parametrize(
    "worker",
    ["test_local_worker", "test_remote_worker"],
)
def test_submit_flow(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=10)

    assert len(job_controller.get_jobs({})) == 2
    assert job_controller.count_jobs(state=JobState.COMPLETED) == 2
    assert job_controller.count_flows(state=FlowState.COMPLETED) == 1


@pytest.mark.parametrize(
    "worker",
    ["test_local_worker", "test_remote_worker"],
)
def test_submit_flow_with_dependencies(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, write_file

    add_parent_1 = add(1, 1)
    add_parent_2 = add(2, 2)
    add_children = add(add_parent_1.output, add_parent_2.output)
    write = write_file(add_children.output)

    flow = Flow([add_parent_1, add_parent_2, add_children, write])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=10)

    assert job_controller.count_jobs(state=JobState.COMPLETED) == 4
    assert len(job_controller.get_jobs({})) == 4
    assert job_controller.count_flows(state=FlowState.COMPLETED) == 1


@pytest.mark.parametrize(
    "worker",
    ["test_local_worker", "test_remote_worker"],
)
def test_expected_failure(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import always_fails

    job_1 = always_fails()
    job_2 = always_fails()

    flow = Flow([job_1, job_2])
    submit_flow(flow, worker=worker)

    assert job_controller.count_jobs({}) == 2
    assert len(job_controller.get_jobs({})) == 2
    assert job_controller.count_flows({}) == 1

    runner = Runner()
    runner.run(ticks=10)

    assert job_controller.count_jobs(state=JobState.FAILED) == 2
    assert job_controller.count_flows(state=FlowState.FAILED) == 1
