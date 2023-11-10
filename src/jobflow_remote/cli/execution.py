from typing import Optional

import typer
from typing_extensions import Annotated

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.jobs.run import run_remote_job

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
            help="The path to the folder where the files to job will be executed",
        ),
    ] = ".",
):
    """
    Run the Job in the selected folder based on the
    """
    run_remote_job(run_dir)
