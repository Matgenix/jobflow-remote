def test_list_projects(
    job_controller, random_project_name, monkeypatch, tmp_dir
) -> None:
    import os

    from monty.serialization import dumpfn

    from jobflow_remote import SETTINGS
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        run_check_cli(["project", "list"], required_out="No project available in")

        dumpfn({"name": "testtest", "xxx": 1}, "testest.yaml")

        output = [
            "The following project names exist in files in the project",
            "testtest.",
        ]
        run_check_cli(["project", "list"], required_out=output)


def test_current_project(job_controller, random_project_name) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(
        ["project"], required_out=f"The selected project is {random_project_name}"
    )


def test_generate(job_controller, random_project_name, monkeypatch, tmp_dir) -> None:
    import os

    from jobflow_remote import SETTINGS
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
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


def test_check(job_controller) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    output = [
        "✓ Worker test_local_worker",
        "✓ Worker test_local_worker_2",
        "✓ Jobstore",
        "✓ Queue store",
    ]
    run_check_cli(["project", "check"], required_out=output)


def test_check_fail(job_controller, monkeypatch, tmp_dir) -> None:
    import json
    import os

    from maggma.stores.mongolike import MongoStore
    from monty.serialization import dumpfn

    from jobflow_remote import SETTINGS
    from jobflow_remote.config import helper
    from jobflow_remote.remote.host.remote import RemoteHost
    from jobflow_remote.remote.queue import QueueManager
    from jobflow_remote.testing.cli import run_check_cli

    def return_none(*args, **kwargs):
        return None

    def exec_jobflow_version(*args, **kwargs):
        d = [
            {"name": "jobflow", "version": "0.1.0"},
            {"name": "jobflow-remote", "version": "0.1.0"},
        ]
        return json.dumps(d), "", 0

    # change project directory and test options there
    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        m.setattr(SETTINGS, "project", "testtest")
        run_check_cli(["project", "list"], required_out="No project available in")

        # create a project with a fake remote worker
        worker_dict = {
            "scheduler_type": "shell",
            "work_dir": "/fake/path",
            "type": "remote",
            "host": "fake_host",
            "timeout_execute": 1,
        }
        queue_dict = {"store": MongoStore("xxx", "yyy").as_dict()}
        dumpfn(
            {
                "name": "testtest",
                "workers": {"fake_remote_worker": worker_dict},
                "queue": queue_dict,
            },
            "testtest.yaml",
        )

        # project check fails as it cannot connect
        err_required = ["Errors:", "x Worker fake_remote_worker"]
        run_check_cli(
            ["project", "check", "-e", "-w", "fake_remote_worker"],
            required_out=err_required,
        )

        # mock all the functions to make the check succeed, except the mismatching jobflow versions
        m.setattr(RemoteHost, "connect", return_none)
        m.setattr(RemoteHost, "test", return_none)
        m.setattr(RemoteHost, "write_text_file", return_none)
        m.setattr(RemoteHost, "execute", exec_jobflow_version)
        m.setattr(QueueManager, "get_jobs_list", return_none)
        m.setattr(helper, "_check_workdir", return_none)
        warn_required = [
            "✓ Worker fake_remote_worker",
            "Errors:",
            "Mismatching versions: jobflow",
            "jobflow-remote",
        ]
        run_check_cli(
            ["project", "check", "-e", "-w", "fake_remote_worker"],
            required_out=warn_required,
        )


def test_remove(job_controller, random_project_name, monkeypatch, tmp_dir) -> None:
    import os

    from jobflow_remote import SETTINGS, ConfigManager
    from jobflow_remote.testing.cli import run_check_cli

    run_check_cli(["project", "list"], required_out=random_project_name)

    # change project directory and test options there
    with monkeypatch.context() as m:
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


def test_list_exec_config(job_controller) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    output = ["Name", "modules", "export", "pre_run", "post_run", "test"]
    run_check_cli(["project", "exec_config", "list", "-v"], required_out=output)


def test_list_workers(job_controller) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    output = ["Name", "type", "info", "test_local_worker", "test_local_worker_2"]
    run_check_cli(["project", "worker", "list", "-v"], required_out=output)
