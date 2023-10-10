def test_project_init(random_project_name):
    from jobflow_remote.config import ConfigManager

    cm = ConfigManager()
    assert len(cm.projects) == 1
    assert cm.projects[random_project_name]
    project = cm.get_project()
    assert len(project.workers) == 1


def test_project_check():
    from jobflow_remote.cli.project import check

    assert check(print_errors=True) is None


def test_submit_flow(runner):
    from jobflow_remote import submit_flow
    from jobflow import job, Flow
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow)
