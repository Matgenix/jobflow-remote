def test_jobs_list(run_check_cli) -> None:
    outputs = ["job", "set", "resources", "admin"]
    excluded = ["─ execution", "start_date"]  # hidden, option
    run_check_cli(["--tree"], required_out=outputs, excluded_out=excluded)

    # test also on subcommands
    outputs = ["job", "resources"]
    excluded = ["─ execution", "start_date", "admin"]
    run_check_cli(["job", "--tree"], required_out=outputs, excluded_out=excluded)


def test_lazy_config_manager(
    job_controller, random_project_name, run_check_cli, monkeypatch, tmp_path
) -> None:
    import shutil
    from pathlib import Path

    from jobflow_remote import SETTINGS
    from jobflow_remote.config.manager import ConfigManager

    selected_msg = f"The selected project is {random_project_name}"

    # the commands using the project print the selected one
    run_check_cli(["job", "list"], required_out=selected_msg)
    run_check_cli(["project"], required_out=selected_msg)

    # the help does not need the configuration
    run_check_cli(["job", "list", "--help"], excluded_out="The selected project is")

    # Two projects with the same name in the folder make the creation of the
    # ConfigManager fail. Only commands that need it should be affected.
    project_file = Path(ConfigManager().get_project_data().filepath)
    for i in range(2):
        shutil.copy(project_file, tmp_path / f"project_{i}{project_file.suffix}")

    with monkeypatch.context() as m:
        m.setattr(SETTINGS, "projects_folder", str(tmp_path))
        run_check_cli(
            ["job", "list", "--help"],
            required_out="Usage",
            excluded_out="The selected project is",
        )
        run_check_cli(["plugin", "list"], excluded_out="The selected project is")
        run_check_cli(
            ["job", "list"],
            required_out="Two projects with the same name",
            error=True,
        )

    # an unknown project still fails for commands requiring it.
    # The -p option sets SETTINGS.project: restore it at the end.
    monkeypatch.setattr(SETTINGS, "project", SETTINGS.project)
    run_check_cli(
        ["-p", "not_a_project", "job", "list"],
        required_out="The selected project not_a_project does not exist",
        excluded_out="The selected project is",
        error=True,
    )
