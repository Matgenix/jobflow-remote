def test_list_projects(
    job_controller, random_project_name, monkeypatch, tmp_dir, run_check_cli
) -> None:
    import os

    from monty.serialization import dumpfn

    from jobflow_remote import SETTINGS

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


def test_current_project(job_controller, random_project_name, run_check_cli) -> None:
    run_check_cli(
        ["project"], required_out=f"The selected project is {random_project_name}"
    )


def test_generate(
    job_controller, random_project_name, monkeypatch, tmp_dir, run_check_cli
) -> None:
    import os

    from jobflow_remote import SETTINGS

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


def test_check(job_controller, monkeypatch, tmp_dir, run_check_cli) -> None:
    import os

    from jobflow_remote import SETTINGS
    from jobflow_remote.config.manager import ConfigManager

    output = [
        "✓ Worker test_local_worker",
        "✓ Worker test_local_worker_2",
        "✓ Jobstore",
        "✓ Queue store",
    ]
    run_check_cli(["project", "check"], required_out=output)

    # create a copy of the project and it so that the jobstore and queue
    # store have the same db and collection
    project = ConfigManager().get_project()
    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "projects_folder", os.getcwd())
        project.jobstore["docs_store"]["collection_name"] = "same_collection"
        project.queue.store["collection_name"] = "same_collection"
        ConfigManager().create_project(project)

        duplicated_msg = "It seems that the main docs_store of the JobStore and the queue store point to the same database and collection"
        run_check_cli(["project", "check"], required_out=[*output, duplicated_msg])


def test_check_env_vars(run_check_cli, monkeypatch):
    # explicit typo in variable (should be with an "S" on PROJECT)
    monkeypatch.setenv("JFREMOTE_PROJECT_FOLDER", "my project folder")
    # random variable (should not be suggested)
    monkeypatch.setenv("JFREMOTE_ZZZZZZZZZZZZZZZ", "zzz")

    output = [
        "The following environment variables with the JFREMOTE_ prefix were found",
        " - JFREMOTE_PROJECT_FOLDER",
        " - JFREMOTE_ZZZZZZZZZZZZZZZ",
        "Suggested environment variables",
        "JFREMOTE_PROJECT_FOLDER -> JFREMOTE_PROJECTS_FOLDER",
    ]
    run_check_cli(
        ["project", "check"],
        required_out=output,
        excluded_out="JFREMOTE_ZZZZZZZZZZZZZZZ -> ",
    )


def test_check_fail(job_controller, monkeypatch, tmp_dir, run_check_cli) -> None:
    import json
    import os

    from maggma.stores.mongolike import MongoStore
    from monty.serialization import dumpfn

    from jobflow_remote import SETTINGS
    from jobflow_remote.config import helper
    from jobflow_remote.remote.host.remote import RemoteHost
    from jobflow_remote.remote.queue import QueueManager

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


def test_remove(
    job_controller, random_project_name, monkeypatch, tmp_dir, run_check_cli
) -> None:
    import os

    from jobflow_remote import SETTINGS, ConfigManager

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


def test_list_exec_config(job_controller, run_check_cli) -> None:
    output = ["Name", "modules", "export", "pre_run", "post_run", "test"]
    run_check_cli(["project", "exec_config", "list", "-v"], required_out=output)


def test_list_workers(job_controller, run_check_cli) -> None:
    output = ["Name", "type", "info", "test_local_worker", "test_local_worker_2"]
    run_check_cli(["project", "worker", "list", "-v"], required_out=output)


def test_edit_replace(
    job_controller, random_project_name, monkeypatch, tmp_dir, run_check_cli
) -> None:
    from monty.serialization import loadfn

    from jobflow_remote import SETTINGS
    from jobflow_remote.config.manager import ConfigManager

    cm_orig = ConfigManager()
    original_project = cm_orig.get_project()

    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "projects_folder", str(tmp_dir))

        # cases with empty projects folder
        run_check_cli(
            ["project", "edit", "replace", "old_text", "new_text"],
            required_out=f"The selected project {random_project_name} does not exist or could not be parsed correctly."
            f" You can use the command 'jf project list -w' to get the parsing errors.",
            error=True,
        )

        run_check_cli(
            ["project", "edit", "replace", "old_text", "new_text", "--all"],
            required_out="No valid project files found",
            error=True,
        )

        # create some projects
        cm_patch = ConfigManager()
        proj1 = original_project.copy()
        proj1.name = "test_project_1"
        proj1.workers["test_local_worker"].work_dir = "/path/to/old_workdir"
        proj1.workers["test_local_worker"].resources = {"old_resource": "value"}
        proj1.queue.store["collection_name"] = "old_collection"
        cm_patch.create_project(proj1)

        proj2 = original_project.copy()
        proj2.name = "test_project_2"
        proj2.workers["test_local_worker"].work_dir = "/different/old_workdir"
        proj2.workers["test_local_worker"].resources = {"some_resource": "value"}
        proj2.queue.store["collection_name"] = "old_collection"
        cm_patch.create_project(proj2)

        proj3 = original_project.copy()
        proj3.name = "test_project_3"
        proj3.workers["test_local_worker"].work_dir = "/different/old_workdir"
        proj3.workers["test_local_worker"].resources = {
            "different_key": "different_value"
        }
        proj3.queue.store["collection_name"] = "different_collection"
        cm_patch.create_project(proj3)

        m.setattr(SETTINGS, "project", "test_project_1")

        # Replace in single project with confirmation
        run_check_cli(
            ["project", "edit", "replace", "old_workdir", "new_workdir"],
            required_out=[
                "Apply these changes to test_project_1?",
                "✓ Modified: test_project_1",
            ],
            cli_input="y",
        )

        assert (tmp_dir / "test_project_1.yaml.bak.1").exists()

        updated_project1 = loadfn(tmp_dir / "test_project_1.yaml")
        assert (
            "new_workdir"
            in updated_project1["workers"]["test_local_worker"]["work_dir"]
        )
        assert (
            "old_workdir"
            not in updated_project1["workers"]["test_local_worker"]["work_dir"]
        )

        # Replace in single project with no confirmation
        run_check_cli(
            ["project", "edit", "replace", "old_resource", "new_resource"],
            required_out=["Apply these changes to test_project_1?"],
            cli_input="n",
        )

        project1_check = loadfn(tmp_dir / "test_project_1.yaml")
        assert (
            "old_resource"
            in project1_check["workers"]["test_local_worker"]["resources"]
        )
        assert (
            "new_resource"
            not in project1_check["workers"]["test_local_worker"]["resources"]
        )

        # --yes, no backup
        run_check_cli(
            [
                "project",
                "edit",
                "replace",
                "old_resource",
                "new_resource",
                "--yes",
                "--no-backup",
            ],
            required_out="✓ Modified: test_project_1",
            excluded_out="Apply these changes",
        )
        assert (tmp_dir / "test_project_1.yaml.bak.1").exists()
        assert not (tmp_dir / "test_project_1.yaml.bak.2").exists()

        updated_project1_force = loadfn(tmp_dir / "test_project_1.yaml")
        assert (
            "new_resource"
            in updated_project1_force["workers"]["test_local_worker"]["resources"]
        )
        assert (
            "old_resource"
            not in updated_project1_force["workers"]["test_local_worker"]["resources"]
        )

        # Replace with --all, mixed responses
        run_check_cli(
            ["project", "edit", "replace", "old_collection", "new_collection", "--all"],
            required_out=[
                "Apply these changes to test_project_1?",
                "Apply these changes to test_project_2?",
                "- No changes: test_project_3",
                "✓ Modified: test_project_1",
            ],
            cli_input="y\nn",
        )

        assert (tmp_dir / "test_project_1.yaml.bak.1").exists()
        assert (tmp_dir / "test_project_1.yaml.bak.2").exists()

        final_project1 = loadfn(tmp_dir / "test_project_1.yaml")
        final_project2 = loadfn(tmp_dir / "test_project_2.yaml")
        final_project3 = loadfn(tmp_dir / "test_project_3.yaml")

        assert "new_collection" in final_project1["queue"]["store"]["collection_name"]
        assert (
            "old_collection" in final_project2["queue"]["store"]["collection_name"]
        )  # Should not change
        assert (
            "different_collection"
            in final_project3["queue"]["store"]["collection_name"]
        )  # Should not change

        # --all --yes
        run_check_cli(
            [
                "project",
                "edit",
                "replace",
                "localhost",
                "127.0.0.1",
                "--all",
                "--yes",
            ],
            required_out=[
                "✓ Modified: test_project_1",
                "✓ Modified: test_project_2",
                "✓ Modified: test_project_3",
                "Successfully modified 3 project file(s)",
            ],
            excluded_out="Apply these changes",
        )

        for proj_file in [
            "test_project_1.yaml",
            "test_project_2.yaml",
            "test_project_3.yaml",
        ]:
            proj_data = loadfn(tmp_dir / proj_file)
            assert "127.0.0.1" in proj_data["queue"]["store"]["host"]
            assert "localhost" not in proj_data["queue"]["store"]["host"]

        # Try to replace something that is not present in the files
        run_check_cli(
            [
                "project",
                "edit",
                "replace",
                "nonexistent_text",
                "replacement_text",
                "--all",
                "--yes",
            ],
            required_out=[
                "- No changes: test_project_1",
                "- No changes: test_project_2",
                "- No changes: test_project_3",
                "No replacements were made in any files",
            ],
        )

        # Project with invalid YAML to test error handling
        invalid_project_path = tmp_dir / "invalid_project.yaml"
        invalid_project_path.write_text(
            "name: invalid_project\nworkers:\n  - this is not valid yaml structure:\n      bad_indent"
        )

        run_check_cli(
            [
                "project",
                "edit",
                "replace",
                "new_collection",
                "old_collection",
                "--all",
                "--yes",
            ],
            required_out=[
                "✓ Modified: test_project_1",
            ],
            excluded_out="invalid_project",
        )

        # Invalid modification
        run_check_cli(
            ["project", "edit", "replace", "workers", "wrong_field"],
            required_out=[
                "WARNING: The modification to the project file will result in an invalid file/project",
                "wrong_field",
                "No replacements were made in any files",
            ],
            cli_input="n",
        )
