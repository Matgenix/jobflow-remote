from datetime import datetime
from typing import Annotated

import click
import typer

from jobflow_remote.cli.formatting import header_name_data_getter_map
from jobflow_remote.cli.utils import (
    IndexDirection,
    SerializeFileFormat,
    SortOption,
    str_to_dict,
    tree_callback,
)
from jobflow_remote.config.base import LogLevel
from jobflow_remote.jobs.state import BatchState, FlowState, JobState


def deprecated_option(old_name: str, new_name: str):
    """Callback that warns about deprecated options and exits."""

    def callback(value: str | None):
        from jobflow_remote.cli.utils import exit_with_error_msg

        if value:
            exit_with_error_msg(
                f"Error: The '{old_name}' option is deprecated. "
                f"Please use '{new_name}' instead.",
            )
        return value

    return callback


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
    list[str] | None,
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more pair of job ids (i.e. uuids) and job index formatted "
        "as UUID:INDEX (e.g. e1d66c4f-81db-4fff-bda2-2bf1d79d5961:2). "
        "The index is mandatory",
    ),
]


job_ids_opt = Annotated[
    list[str] | None,
    typer.Option(
        "--job-id",
        "-jid",
        help="One or more job ids (i.e. uuids). Only the id is needed since "
        "jobs with the same uuid belong to the same flow",
    ),
]


db_ids_opt = Annotated[
    list[str] | None,
    typer.Option(
        "--db-id",
        "-did",
        help="One or more db ids",
    ),
]


flow_ids_opt = Annotated[
    list[str] | None,
    typer.Option(
        "--flow-id",
        "-fid",
        help="One or more flow ids. Can the db id (i.e. an integer) or a string (i.e. the uuid)",
    ),
]


job_state_opt = Annotated[
    list[JobState] | None,
    typer.Option(
        "--state",
        "-s",
        help="One or more of the Job states",
    ),
]


flow_state_opt = Annotated[
    list[FlowState] | None,
    typer.Option(
        "--state",
        "-s",
        help="One or more of the Flow states",
    ),
]


batch_state_opt = Annotated[
    list[BatchState] | None,
    typer.Option(
        "--state",
        "-s",
        help="One or more of the batch states",
    ),
]


name_opt = Annotated[
    str | None,
    typer.Option(
        "--name",
        "-n",
        help="The name. Default is an exact match, but all conventions from "
        "python fnmatch can be used (e.g. *test*). Using * wildcard may require"
        "enclosing the search string in quotation marks.",
    ),
]


worker_name_opt = Annotated[
    list[str] | None,
    typer.Option(
        "--worker",
        "-wk",
        help="One or more worker names",
    ),
]


job_state_arg = Annotated[JobState, typer.Argument(help="One of the job states")]


start_date_opt = Annotated[
    datetime | None,
    typer.Option(
        "--start-date",
        "-sdate",
        help="Initial date for last update field",
        formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%H:%M:%S", "%H:%M:%S"],
    ),
]


end_date_opt = Annotated[
    datetime | None,
    typer.Option(
        "--end-date",
        "-edate",
        help="Final date for last update field",
    ),
]


days_opt = Annotated[
    int | None,
    typer.Option(
        "--days",
        "-ds",
        help="Last update field is in the last days",
    ),
]


hours_opt = Annotated[
    int | None,
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
    int | None,
    typer.Argument(
        help="The index of the job. If not defined the job with the largest index is selected",
        metavar="INDEX",
    ),
]

job_index_opt = Annotated[
    int | None,
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

# This should not be used and has been replaced by yes_opt.
# Do not remove for the time being as it may be still used by some other
# package with plugins.
force_opt = Annotated[
    bool,
    typer.Option(
        "--force",
        "-f",
        help="No confirmation will be asked before proceeding",
    ),
]


# Option to deprecate the usage of the --force option when used instead
# of --yes. Can be removed in the future after a deprecation period.
force_opt_deprecated = Annotated[
    bool,
    typer.Option(
        "--force",
        "-f",
        help="No confirmation will be asked before proceeding",
        callback=deprecated_option("--force", "--yes"),
        hidden=True,
    ),
]


yes_opt = Annotated[
    bool,
    typer.Option(
        "--yes",
        "-y",
        help="Sets any confirmation values to 'yes' automatically",
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

show_all_batches_opt = Annotated[
    bool, typer.Option("--all", "-a", help="Show all batches (running and stopped)")
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
    IndexDirection | None,
    typer.Argument(
        help="The direction of the index",
        metavar="DIRECTION",
    ),
]

count_opt = Annotated[
    bool,
    typer.Option(
        "--count",
        help="Just return the count of the selected elements",
    ),
]


stored_data_keys_opt = Annotated[
    list[str] | None,
    typer.Option(
        "--stored-data-key",
        "-sdk",
        help="Key to be shown from the stored_data field.",
    ),
]
cli_output_keys_opt = Annotated[
    str | None,
    typer.Option(
        "--output",
        "-o",
        help=f"Table columns to be shown. Needs to be specified as string with comma separated keys, e.g."
        f"'state,db_id,name'. Overrides the verbosity option. Can also be set in the config file. "
        f"Available options are: {', '.join(header_name_data_getter_map)}",
    ),
]


# as of typer version 0.9.0 the dict is not a supported type. Define a custom one
class DictType(dict):
    pass


# Python 3.10+ union types are fully supported now
# These type aliases are kept for backward compatibility with typer's click integration
OptionalStr = str | None
OptionalDictType = DictType | None


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
        "Keys must not overlap with those from other specified query options. "
        "Requires knowledge of the internal structure of the DB. "
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
