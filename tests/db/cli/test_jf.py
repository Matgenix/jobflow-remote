def test_jobs_list() -> None:
    from jobflow_remote.testing.cli import run_check_cli

    outputs = ["job", "set", "resources", "admin"]
    excluded = ["execution", "start_date"]  # hidden, option
    run_check_cli(["tree"], required_out=outputs, excluded_out=excluded)

    # max depth
    outputs = ["job", "set"]
    excluded = ["execution", "start_date", "resources"]
    run_check_cli(["tree", "-d", "2"], required_out=outputs, excluded_out=excluded)

    # hidden
    outputs = ["job", "set", "resources", "execution"]
    excluded = ["start_date"]
    run_check_cli(["tree", "-h"], required_out=outputs, excluded_out=excluded)

    # options
    outputs = ["job", "set", "resources", "start_date"]
    excluded = ["execution"]
    run_check_cli(["tree", "-o"], required_out=outputs, excluded_out=excluded)

    # start from
    outputs = ["job set", "resources"]
    excluded = ["execution", "start_date", "admin"]
    run_check_cli(["tree", "job", "set"], required_out=outputs, excluded_out=excluded)
