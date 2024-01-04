import pytest


def test_project_init(random_project_name):
    from jobflow_remote.config import ConfigManager

    cm = ConfigManager()
    assert len(cm.projects) == 1
    assert cm.projects[random_project_name]
    project = cm.get_project()
    assert len(project.workers) == 2


def test_project_check(job_controller):
    from jobflow_remote.cli.project import check

    assert check(print_errors=True) is None


@pytest.mark.parametrize("worker", ["test_local_worker", "test_remote_worker"])
def test_submit_flow(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=10)

    jobs = job_controller.get_jobs({})
    assert len(jobs) == 2


@pytest.mark.parametrize("worker", ["test_local_worker", "test_remote_worker"])
def test_submit_flow_with_dependencies(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing import add, write_file

    add_parent_1 = add(1, 1)
    add_parent_2 = add(2, 2)
    add_children = add(add_parent_1.output, add_parent_2.output)
    write = write_file(add_children.output)

    flow = Flow([add_parent_1, add_parent_2, add_children, write])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=10)

    assert len(job_controller.get_jobs({})) == 4
