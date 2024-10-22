def test_jobs_list() -> None:
    from jobflow_remote.testing.cli import run_check_cli

    outputs = ["job", "set", "resources", "admin"]
    excluded = ["─ execution", "start_date"]  # hidden, option
    run_check_cli(["--tree"], required_out=outputs, excluded_out=excluded)

    # test also on subcommands
    outputs = ["job", "resources"]
    excluded = ["─ execution", "start_date", "admin"]
    run_check_cli(["job", "--tree"], required_out=outputs, excluded_out=excluded)
