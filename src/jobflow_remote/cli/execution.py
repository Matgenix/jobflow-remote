from typing import Annotated, Optional

import typer

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.jobs.run import run_batch_jobs, run_remote_job

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
        Optional[str],
        typer.Argument(
            help="The path to the folder where the files of the job to run will be executed",
        ),
    ] = ".",
):
    """
    Run the Job in the selected folder based on the
    """
    run_remote_job(run_dir)


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
    process_uuid: Annotated[
        str,
        typer.Argument(
            help="A uuid representing the batch process",
        ),
    ],
    max_time: Annotated[
        Optional[int],
        typer.Option(
            "--max-time",
            "-mt",
            help=(
                "The maximum time after which no more jobs will be executed (seconds)"
            ),
        ),
    ] = None,
    max_wait: Annotated[
        Optional[int],
        typer.Option(
            "--max-wait",
            "-mw",
            help=(
                "The maximum time the job will wait before stopping if no jobs are available to run (seconds)"
            ),
        ),
    ] = 60,
    max_jobs: Annotated[
        Optional[int],
        typer.Option(
            "--max-jobs",
            "-mj",
            help=("The maximum number of jobs that will be executed by the batch job"),
        ),
    ] = None,
):
    """
    Run Jobs in batch mode
    """
    run_batch_jobs(
        base_run_dir,
        files_dir,
        process_uuid,
        max_time=max_time,
        max_wait=max_wait,
        max_jobs=max_jobs,
    )
