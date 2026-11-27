from typing import Annotated

import typer

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.jobs.run import run_batch_jobs, run_cloud_job, run_remote_job

app_execution = JFRTyper(
    name="execution",
    help="Commands for executing the jobs locally",
    no_args_is_help=True,
    hidden=True,
)
app.add_typer(app_execution)


@app_execution.command()
def run(
    run_dir: Annotated[
        str | None,
        typer.Argument(
            help="The path to the folder where the files of the job to run will be executed",
        ),
    ] = ".",
) -> None:
    """Run the Job in the selected folder based on the job files."""
    run_remote_job(run_dir)


@app_execution.command(name="run-cloud")
def run_cloud(
    storage_uri: Annotated[
        str,
        typer.Argument(
            help="Cloud storage URI of the job folder (e.g. s3://bucket/jf_work/<job folder>)",
        ),
    ],
    local_dir: Annotated[
        str,
        typer.Option(
            "--local-dir",
            help="Folder in the container under which the job folder is created, "
            "with the same path as in the storage. The job folder is kept after the job.",
        ),
    ],
    storage_options: Annotated[
        list[str] | None,
        typer.Option(
            "--storage-option",
            help="Option of the storage class as KEY=VALUE, e.g. region_name=us-east-2. "
            "Can be repeated.",
        ),
    ] = None,
) -> None:
    """Download the input of a job from cloud storage, run the job, and upload the folder."""
    options = {}
    for option in storage_options or []:
        key, sep, value = option.partition("=")
        if not sep:
            raise typer.BadParameter(
                f"'{option}' is not KEY=VALUE.", param_hint="--storage-option"
            )
        options[key] = value
    run_cloud_job(storage_uri=storage_uri, local_dir=local_dir, storage_options=options)


@app_execution.command()
def run_batch(
    base_run_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the base folder where the jobs will be executed",
        ),
    ],
    files_dir: Annotated[
        str,
        typer.Argument(
            help="The path to the folder where files for handling the batch jobs will be stored",
        ),
    ],
    batch_uid: Annotated[
        str,
        typer.Argument(
            help="A uuid representing the batch process",
        ),
    ],
    max_time: Annotated[
        float | None,
        typer.Option(
            "--max-time",
            "-mt",
            help=(
                "The maximum time after which no more jobs will be executed (seconds)"
            ),
        ),
    ] = None,
    max_wait: Annotated[
        float | None,
        typer.Option(
            "--max-wait",
            "-mw",
            help=(
                "The maximum time the job will wait before stopping if no jobs are available to run (seconds)"
            ),
        ),
    ] = 60,
    max_jobs: Annotated[
        int | None,
        typer.Option(
            "--max-jobs",
            "-mj",
            help=("The maximum number of jobs that will be executed by the batch job"),
        ),
    ] = None,
    parallel_jobs: Annotated[
        int | None,
        typer.Option(
            "--parallel-jobs",
            "-pj",
            help=("Number of jobs executed in parallel"),
        ),
    ] = None,
    sleep_time: Annotated[
        float | None,
        typer.Option(
            "--sleep-time",
            "-st",
            help=(
                "Sleep time when no submitted job is available to run before checking again (seconds)"
            ),
        ),
    ] = None,
) -> None:
    """Run Jobs in batch mode."""
    run_batch_jobs(
        base_run_dir,
        files_dir,
        batch_uid,
        max_time=max_time,
        max_wait=max_wait,
        max_jobs=max_jobs,
        parallel_jobs=parallel_jobs,
        sleep_time=sleep_time,
    )
