def test_project_init(random_project_name):
    from jobflow_remote.config import ConfigManager

    cm = ConfigManager()
    assert len(cm.projects) == 1
    assert cm.projects[random_project_name]
    project = cm.get_project()
    assert len(project.workers) == 1


def test_project_check():
    from jobflow_remote.cli.project import check

    assert check(print_errors=True)
