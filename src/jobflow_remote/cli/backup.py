from typing import Annotated, Optional

import typer

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import (
    check_stopped_runner,
    get_job_controller,
    loading_spinner,
    out_console,
)

app_backup = JFRTyper(
    name="backup",
    help="Commands for handling backup of the database",
    no_args_is_help=True,
)
app.add_typer(app_backup)


@app_backup.command()
def create(
    backup_dir: Annotated[
        str,
        typer.Argument(
            help="The path of the directory where the output files will be saved",
            metavar="BACKUP_DIR",
        ),
    ] = ".",
    compress: Annotated[
        bool,
        typer.Option(
            "--compress",
            "-c",
            help="Compress the output files",
        ),
    ] = False,
    mongo_path: Annotated[
        Optional[str],
        typer.Option(
            "--mongo-path",
            "-m",
            help=(
                "The path to a folder containing the mongodump executable, if not present in the PATH"
            ),
        ),
    ] = None,
    python: Annotated[
        bool,
        typer.Option(
            "--python",
            "-p",
            help="If selected a python implementation will be used to create a backup. "
            "WARNING: In this case metadata of the collections will not be saved",
        ),
    ] = False,
) -> None:
    """
    Create a backup of the queue database using either mongodump or a python implementation.
    The mongodump version is faster and stores metadata, but requires the mongodump executable
    and may not support all the connection options defined in the project configuration.
    """
    check_stopped_runner(error=True)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Creating backup...", total=None)
        jc = get_job_controller()
        n_docs = jc.backup_dump(
            dir_path=backup_dir,
            mongo_bin_path=mongo_path,
            compress=compress,
            python=python,
        )

    out_console.print("Backup created for ")
    for collection in sorted(n_docs):
        out_console.print(f"\t{collection} collection: {n_docs[collection]} documents")
    out_console.print("Verify that these numbers fit the expected values.")


@app_backup.command()
def restore(
    backup_dir: Annotated[
        str,
        typer.Argument(
            help="The path of the directory where the backup files are located",
            metavar="BACKUP_DIR",
        ),
    ] = ".",
    mongo_path: Annotated[
        Optional[str],
        typer.Option(
            "--mongo-path",
            "-m",
            help=(
                "The path to a folder containing the mongorestore executable, if not present in the PATH"
            ),
        ),
    ] = None,
    python: Annotated[
        bool,
        typer.Option(
            "--python",
            "-p",
            help="If selected a python implementation will be used to restore a backup. "
            "WARNING: In this case metadata of the collections will not be restored",
        ),
    ] = False,
) -> None:
    """
    Recreate the queue database from a previous backup using either mongorestore or a python implementation.
    """
    check_stopped_runner(error=True)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Restoring backup...", total=None)
        jc = get_job_controller()
        jc.backup_restore(
            dir_path=backup_dir,
            mongo_bin_path=mongo_path,
            python=python,
        )

    out_console.print("Backup restored")
    if python:
        out_console.print(
            "Python version does not restore indexes in the DB. It is advisable to run "
            "'jf admin index rebuild' to create them."
        )
