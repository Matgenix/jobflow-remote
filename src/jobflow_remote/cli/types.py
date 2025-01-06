from datetime import datetime
from typing import Annotated, Optional

import click
import typer

from jobflow_remote.cli.utils import (
    IndexDirection,
    SerializeFileFormat,
    SortOption,
    str_to_dict,
    tree_callback,
)
from jobflow_remote.config.base import LogLevel
from jobflow_remote.jobs.state import FlowState, JobState

tree_opt = Annotated[
    bool,
    typer.Option(
        "--tree",
        help="Display a tree representation of the CLI command structure",
        is_eager=True,
        callback=tree_callback,
    ),
]

job_ids_indexes_opt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more pair of job ids (i.e. uuids) and job index formatted "
        "as UUID:INDEX (e.g. e1d66c4f-81db-4fff-bda2-2bf1d79d5961:2). "
        "The index is mandatory",
    ),
]


job_ids_opt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more job ids (i.e. uuids). Only the id is needed since "
        "jobs with the same uuid belong to the same flow",
    ),
]


db_ids_opt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--db-id",
        "-did",
        help="One or more db ids",
    ),
]


flow_ids_opt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--flow-id",
        "-fid",
        help="One or more flow ids",
    ),
]


job_state_opt = Annotated[
    Optional[list[JobState]],
    typer.Option(
        "--state",
        "-s",
        help="One or more of the Job states",
    ),
]


flow_state_opt = Annotated[
    Optional[list[FlowState]],
    typer.Option(
        "--state",
        "-s",
        help="One or more of the Flow states",
    ),
]


name_opt = Annotated[
    Optional[str],
    typer.Option(
        "--name",
        "-n",
        help="The name. Default is an exact match, but all conventions from "
        "python fnmatch can be used (e.g. *test*). Using * wildcard may require"
        "enclosing the search string in quotation marks.",
    ),
]

worker_name_opt = Annotated[
    Optional[list[str]],
    typer.Option(
        "--worker",
        "-wk",
        help="One or more worker names",
    ),
]


job_state_arg = Annotated[JobState, typer.Argument(help="One of the job states")]


start_date_opt = Annotated[
    Optional[datetime],
    typer.Option(
        "--start-date",
        "-sdate",
        help="Initial date for last update field",
        formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%H:%M:%S", "%H:%M:%S"],
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


hours_opt = Annotated[
    Optional[int],
    typer.Option(
        "--hours",
        "-hs",
        help="Last update field is in the last hours",
    ),
]


verbosity_opt = Annotated[
    int,
    typer.Option(
        "--verbosity",
        "-v",
        help="Set the verbosity of the output. Multiple -v options "
        "increase the verbosity. (e.g. -vvv)",
        count=True,
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
        help="The ID of the job. Can be the db id (i.e. an integer) or a string (i.e. the uuid)",
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

job_index_opt = Annotated[
    Optional[int],
    typer.Option(
        "--index",
        "-i",
        help="The index of the job. If not defined the job with the largest index is selected",
    ),
]


flow_db_id_arg = Annotated[
    str,
    typer.Argument(
        help="The ID of the flow. Can the db id (i.e. an integer) or a string (i.e. the uuid)",
        metavar="ID",
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


job_flow_id_flag_opt = Annotated[
    bool,
    typer.Option(
        "--job",
        "-j",
        help="The passed ID will be the ID of one of the jobs"
        " belonging to the flow, instead of the ID of the flow.",
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


locked_flow_opt = Annotated[
    bool,
    typer.Option(
        "--locked",
        "-l",
        help="Select locked Flows",
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


wait_lock_opt = Annotated[
    int,
    typer.Option(
        "--wait",
        "-w",
        help="When trying to acquire the lock on the documents that need to "
        "be modified, wait an amount of seconds equal to the value specified",
    ),
]


break_lock_opt = Annotated[
    bool,
    typer.Option(
        "--break-lock",
        "-bl",
        help="Forcibly break the lock for the documents that need to be modified. "
        "Use with care and possibly when the runner is stopped. Can lead to "
        "inconsistencies",
    ),
]


raise_on_error_opt = Annotated[
    bool,
    typer.Option(
        "--raise-on-error",
        "-re",
        help="If an error arises during any of the operations raise an exception "
        "and stop the execution",
    ),
]


delete_output_opt = Annotated[
    bool,
    typer.Option(
        "--output",
        "-o",
        help="Also delete the outputs of the Jobs in the output Store",
    ),
]


delete_files_opt = Annotated[
    bool,
    typer.Option(
        "--files",
        "-fi",
        help="Also delete the files on the worker",
    ),
]


delete_all_opt = Annotated[
    bool,
    typer.Option(
        "--all",
        "-a",
        help="enable --output and --files",
    ),
]

foreground_index_opt = Annotated[
    bool,
    typer.Option(
        "--foreground",
        "-fg",
        help="The build of the indexes will not be executed in the background",
    ),
]

index_key_arg = Annotated[
    str,
    typer.Argument(
        help="The field on which the index will be created",
        metavar="INDEX",
    ),
]

index_direction_arg = Annotated[
    Optional[IndexDirection],
    typer.Argument(
        help="The direction of the index",
        metavar="DIRECTION",
    ),
]


# as of typer version 0.9.0 the dict is not a supported type. Define a custom one
class DictType(dict):
    pass


# Similarly, Python 3.10 union types are not supported,
# e.g., `str | None` vs `Optional[str]`
# ruff enforces PEP 604
# but will leave things in if they are explicit type aliases
OptionalStr = Optional[str]
OptionalDictType = Optional[DictType]


class DictTypeParser(click.ParamType):
    name = "DictType"

    def convert(self, value, param, ctx):
        value = str_to_dict(value)
        return DictType(value)


query_opt = Annotated[
    OptionalDictType,
    typer.Option(
        "--query",
        "-q",
        help="A JSON string representing a generic query in the form of a dictionary. "
        "Overrides all other query options. Requires knowledge of the internal structure of the DB. "
        "Can be either a list of comma separated key=value pairs or a string with the JSON"
        " representation of a dictionary containing the mongoDB query that "
        'should be performed (e.g \'{"key1.key2": 1, "key3": "test"}\')',
        click_type=DictTypeParser(),
    ),
]


metadata_opt = Annotated[
    OptionalDictType,
    typer.Option(
        "--metadata",
        "-meta",
        help="A string representing the metadata to be queried. Can be either"
        " a list of comma separated key=value pairs or a string with the JSON"
        " representation of a dictionary containing the mongoDB query for "
        'the metadata subdocument (e.g \'{"key1.key2": 1, "key3": "test"}\')',
        click_type=DictTypeParser(),
    ),
]
