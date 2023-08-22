import json
from datetime import datetime
from typing import List, Optional

import click
import typer
from typing_extensions import Annotated

from jobflow_remote.cli.utils import SerializeFileFormat, SortOption
from jobflow_remote.config.base import LogLevel
from jobflow_remote.jobs.state import JobState, RemoteState

job_ids_indexes_opt = Annotated[
    Optional[List[str]],
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more pair of job ids (i.e. uuids) and job index formatted "
        "as UUID:INDEX (e.g. e1d66c4f-81db-4fff-bda2-2bf1d79d5961:2). "
        "The index is mandatory",
    ),
]


job_ids_opt = Annotated[
    Optional[List[str]],
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more job ids (i.e. uuids). Only the id is needed since "
        "jobs with the same uuid belong to the same flow",
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
        help="Reverse the sorting order",
    ),
]


job_db_id_arg = Annotated[
    str,
    typer.Argument(
        help="The ID of the job can the db id (i.e. an integer) or a string (i.e. the uuid)",
        metavar="ID",
    ),
]
job_index_arg = Annotated[
    Optional[int],
    typer.Argument(
        help="The index of the job. If not defined the job with the largest index is selected",
        metavar="INDEX",
    ),
]


force_opt = Annotated[
    bool,
    typer.Option(
        "--force",
        "-f",
        help="No confirmation will be asked before proceeding",
    ),
]


locked_opt = Annotated[
    bool,
    typer.Option(
        "--locked",
        "-l",
        help="Select locked Jobs",
    ),
]


serialize_file_format_opt = Annotated[
    SerializeFileFormat,
    typer.Option(
        "--format",
        "-f",
        help="File format",
    ),
]


# as of typer version 0.9.0 the dict is not a supported type. Define a custom one
class DictType(dict):
    pass


class DictTypeParser(click.ParamType):
    name = "DictType"

    def convert(self, value, param, ctx):
        try:
            value = json.loads(value)
        except Exception as e:
            raise typer.BadParameter(
                f"Error while converting JSON: {getattr(e, 'message', str(e))}"
            )
        return DictType(value)


query_opt = Annotated[
    Optional[DictType],
    typer.Option(
        "--query",
        "-q",
        help="A JSON string representing a generic query in the form of a dictionary. Overrides all other query options. Requires knowledge of the internal structure of the DB. ",
        click_type=DictTypeParser(),
    ),
]
