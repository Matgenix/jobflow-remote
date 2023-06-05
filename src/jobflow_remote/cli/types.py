from datetime import datetime
from typing import List, Optional

import typer
from typing_extensions import Annotated

from jobflow_remote.cli.utils import LogLevel, SortOption
from jobflow_remote.jobs.state import JobState, RemoteState

job_ids_opt = Annotated[
    Optional[List[str]],
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more job ids (i.e. uuids)",
    ),
]


db_ids_opt = Annotated[
    Optional[List[int]],
    typer.Option(
        "--db-id",
        "-did",
        help="One or more db ids",
    ),
]


flow_ids_opt = Annotated[
    Optional[List[str]],
    typer.Option(
        "--flow-id",
        "-fid",
        help="One or more flow ids",
    ),
]


job_state_opt = Annotated[
    Optional[JobState],
    typer.Option(
        "--state",
        "-s",
        help="One of the Job states",
    ),
]


remote_state_opt = Annotated[
    Optional[RemoteState],
    typer.Option(
        "--remote-state",
        "-rs",
        help="One of the remote states",
    ),
]


remote_state_arg = Annotated[
    RemoteState, typer.Argument(help="One of the remote states")
]


start_date_opt = Annotated[
    Optional[datetime],
    typer.Option(
        "--start-date",
        "-sdate",
        help="Initial date for last update field",
    ),
]


end_date_opt = Annotated[
    Optional[datetime],
    typer.Option(
        "--end-date",
        "-edate",
        help="Final date for last update field",
    ),
]


days_opt = Annotated[
    Optional[int],
    typer.Option(
        "--days",
        "-ds",
        help="Last update field is in the last days",
    ),
]


verbosity_opt = Annotated[
    int,
    typer.Option(
        "--verbosity", "-v", help="Set the verbosity of the output", count=True
    ),
]


log_level_opt = Annotated[
    LogLevel,
    typer.Option(
        "--log-level",
        "-log",
        help="Set the log level of the runner",
    ),
]

runner_num_procs_opt = Annotated[
    int,
    typer.Option(
        "--num-procs",
        "-n",
        help="The number of Runner processes started",
    ),
]

max_results_opt = Annotated[
    int,
    typer.Option(
        "--max-results",
        "-m",
        help="Limit the maximum number of returned results. Set 0 for no limit",
    ),
]


sort_opt = Annotated[
    SortOption,
    typer.Option(
        "--sort",
        help="The field on which the results will be sorted. In descending order",
    ),
]


reverse_sort_flag_opt = Annotated[
    bool,
    typer.Option(
        "--reverse-sort",
        "-revs",
        help=("Reverse the sorting order"),
    ),
]


job_id_arg = Annotated[str, typer.Argument(help="The ID of the job (i.e. the uuid)")]


db_id_flag_opt = Annotated[
    bool,
    typer.Option(
        "--db-id",
        "-db",
        help=(
            "If set the id passed would be considered to be the DB id (i.e. an integer)"
        ),
    ),
]


force_opt = Annotated[
    bool,
    typer.Option(
        "--force",
        "-f",
        help=("No confirmation will be asked before proceeding"),
    ),
]
