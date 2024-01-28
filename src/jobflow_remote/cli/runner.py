import os
from typing import Annotated

import typer
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import log_level_opt
from jobflow_remote.cli.utils import (
    exit_with_error_msg,
    exit_with_warning_msg,
    get_config_manager,
    loading_spinner,
    out_console,
)
from jobflow_remote.config.base import LogLevel
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
from jobflow_remote.jobs.runner import Runner

app_runner = JFRTyper(
    name="runner", help="Commands for handling the Runner", no_args_is_help=True
)
app.add_typer(app_runner)


@app_runner.command()
def run(
    log_level: log_level_opt = LogLevel.INFO.value,
    set_pid: Annotated[
        bool,
        typer.Option(
            "--set-pid",
            "-pid",
            help="Set the runner id to the current process pid",
        ),
    ] = False,
    transfer: Annotated[
        bool,
        typer.Option(
            "--transfer",
            "-t",
            help="Enable the transfer option in the runner",
        ),
    ] = False,
    complete: Annotated[
        bool,
        typer.Option(
            "--complete",
            "-com",
            help="Enable the complete option in the runner",
        ),
    ] = False,
    queue: Annotated[
        bool,
        typer.Option(
            "--queue",
            "-q",
            help="Enable the queue option in the runner",
        ),
    ] = False,
    checkout: Annotated[
        bool,
        typer.Option(
            "--checkout",
            "-cho",
            help="Enable the checkout option in the runner",
        ),
    ] = False,
):
    """
    Execute the Runner in the foreground.
    Do NOT execute this to start as a daemon.
    Should be used by the daemon or for testing purposes.
    """
    runner_id = os.getpid() if set_pid else None
    runner = Runner(log_level=log_level, runner_id=str(runner_id))
    if not (transfer or complete or queue or checkout):
        transfer = complete = queue = checkout = True

    runner.run(transfer=transfer, complete=complete, queue=queue, checkout=checkout)


@app_runner.command()
def start(
    transfer: Annotated[
        int,
        typer.Option(
            "--transfer",
            "-t",
            help="The number of processes dedicated to completing jobs",
        ),
    ] = 1,
    complete: Annotated[
        int,
        typer.Option(
            "--complete",
            "-com",
            help="The number of processes dedicated to completing jobs",
        ),
    ] = 1,
    single: Annotated[
        bool,
        typer.Option(
            "--single",
            "-s",
            help="Use a single process for the runner",
        ),
    ] = False,
    log_level: log_level_opt = LogLevel.INFO.value,
):
    """
    Start the Runner as a daemon
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(False) as progress:
        progress.add_task(description="Starting the daemon...", total=None)
        try:
            dm.start(
                num_procs_transfer=transfer,
                num_procs_complete=complete,
                single=single,
                log_level=log_level.value,
                raise_on_error=True,
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while starting the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def stop(
    wait: Annotated[
        bool,
        typer.Option(
            "--wait",
            "-w",
            help=(
                "Wait until the daemon has stopped. NOTE: this may take a while if a large file is being transferred!"
            ),
        ),
    ] = False
):
    """
    Send a stop signal to the Runner processes.
    Each of the Runner processes will stop when finished the task being executed.
    By default, return immediately
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(False) as progress:
        progress.add_task(description="Stopping the daemon...", total=None)
        try:
            dm.stop(wait=wait, raise_on_error=True)
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon: {getattr(e, 'message', e)}"
            )
    from jobflow_remote import SETTINGS

    if SETTINGS.cli_suggestions:
        out_console.print(
            "The stop signal has been sent to the Runner. Run 'jf runner status' to verify if it stopped",
            style="yellow",
        )


@app_runner.command()
def kill():
    """
    Send a kill signal to the Runner processes.
    Return immediately, does not wait for processes to be killed.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(False) as progress:
        progress.add_task(description="Killing the daemon...", total=None)
        try:
            dm.kill(raise_on_error=True)
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while killing the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def shutdown():
    """
    Shuts down the supervisord process.
    Note that if the daemon is running it will wait for the daemon to stop.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(False) as progress:
        progress.add_task(description="Shutting down supervisor...", total=None)
        try:
            dm.shut_down(raise_on_error=True)
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while shutting down supervisor: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def status():
    """
    Fetch the status of the daemon runner
    """
    from jobflow_remote import SETTINGS

    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner():
        try:
            current_status = dm.check_status()
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while checking the status of the daemon: {getattr(e, 'message', e)}"
            )
    color = {
        DaemonStatus.STOPPED: "red",
        DaemonStatus.STOPPING: "gold1",
        DaemonStatus.SHUT_DOWN: "red",
        DaemonStatus.PARTIALLY_RUNNING: "gold1",
        DaemonStatus.RUNNING: "green",
    }[current_status]
    text = Text()
    text.append("Daemon status: ")
    text.append(current_status.value.lower(), style=color)
    out_console.print(text)
    if current_status == DaemonStatus.PARTIALLY_RUNNING and SETTINGS.cli_suggestions:
        out_console.print(
            f"The {current_status.value.lower()} may be present due to the "
            "runner stopping or signal a problem with one of the processes "
            "of the runner. If the state should be RUNNING, check the detailed"
            " status with the 'info'  command and consider restarting the runner.",
            style="yellow",
        )


@app_runner.command()
def info():
    """
    Fetch the information about the process of the daemon.
    Contain the supervisord process and the processes running the Runner.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    procs_info_dict = None
    try:
        with loading_spinner():
            procs_info_dict = dm.get_processes_info()
    except DaemonError as e:
        exit_with_error_msg(
            f"Error while stopping the daemon: {getattr(e, 'message', e)}"
        )
    if not procs_info_dict:
        exit_with_warning_msg("Daemon is not running")
    table = Table()
    table.add_column("Process")
    table.add_column("PID")
    table.add_column("State")

    for name, proc_info in procs_info_dict.items():
        table.add_row(name, str(proc_info["pid"]), str(proc_info["state"]))

    out_console.print(table)
