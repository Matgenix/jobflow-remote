def test_list_projects(job_controller, random_project_name, monkeypatch, tmp_dir):
    import os

    from monty.serialization import dumpfn

    from jobflow_remote import SETTINGS
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
        print(tmp_dir, tmp_dir.__class__)
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        run_check_cli(["project", "list"], required_out="No project available in")

        dumpfn({"name": "testtest", "xxx": 1}, "testest.yaml")

        output = [
            "The following project names exist in files in the project",
            "testtest.",
        ]
        run_check_cli(["project", "list"], required_out=output)


def test_current_project(job_controller, random_project_name):
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["project"], required_out=f"The selected project is {random_project_name}"
    )


def test_generate(job_controller, random_project_name, monkeypatch, tmp_dir):
    import os

    from jobflow_remote import SETTINGS
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
        print(tmp_dir, tmp_dir.__class__)
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        run_check_cli(
            ["project", "generate", "test_proj_1"],
            required_out="Configuration file for project test_proj_1 created in",
        )
        run_check_cli(
            ["project", "generate", "--full", "test_proj_2"],
            required_out="Configuration file for project test_proj_2 created in",
        )

        run_check_cli(
            ["project", "generate", "test_proj_1"],
            required_out="Project with name test_proj_1 already exists",
            error=True,
        )


def test_check(job_controller):
    from jobflow_remote.testing.cli import run_check_cli

    output = [
        "✓ Worker test_local_worker",
        "✓ Worker test_local_worker_2",
        "✓ Jobstore",
        "✓ Queue store",
    ]
    run_check_cli(["project", "check"], required_out=output)


def test_remove(job_controller, random_project_name, monkeypatch, tmp_dir):
    import os

    from jobflow_remote import SETTINGS, ConfigManager
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
        print(tmp_dir, tmp_dir.__class__)
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        cm = ConfigManager()
        run_check_cli(
            ["project", "generate", "test_proj_1"],
            required_out="Configuration file for project test_proj_1 created in",
        )
        cm = ConfigManager()
        assert "test_proj_1" in cm.projects_data
        run_check_cli(
            ["project", "remove", "test_proj_1"],
            required_out="This will delete also the folders",
            cli_input="y",
        )

        cm = ConfigManager()
        assert "test_proj_1" not in cm.projects_data

        run_check_cli(
            ["project", "remove", "test_proj_1"],
            required_out="Project test_proj_1 does not exist",
            cli_input="y",
        )


def test_list_exec_config(job_controller):
    from jobflow_remote.testing.cli import run_check_cli

    output = ["Name", "modules", "export", "pre_run", "post_run", "test"]
    run_check_cli(["project", "exec_config", "list", "-v"], required_out=output)


def test_list_workers(job_controller):
    from jobflow_remote.testing.cli import run_check_cli

    output = ["Name", "type", "info", "test_local_worker", "test_local_worker_2"]
    run_check_cli(["project", "worker", "list", "-v"], required_out=output)
