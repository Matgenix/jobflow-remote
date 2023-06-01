import os

import typer
from rich.table import Table
from rich.text import Text
from typing_extensions import Annotated

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.types import log_level_opt, runner_num_procs_opt
from jobflow_remote.cli.utils import (
    LogLevel,
    exit_with_error_msg,
    exit_with_warning_msg,
    loading_spinner,
    out_console,
)
from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
from jobflow_remote.jobs.runner import Runner

app_runner = typer.Typer(
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
    ] = True,
):
    """
    Execute the Runner in the foreground.
    Do NOT execute this to start as a daemon.
    Should be used by the daemon or for testing purposes.
    """
    runner_id = os.getpid() if set_pid else None
    runner = Runner(log_level=log_level.to_logging(), runner_id=runner_id)
    runner.run()


@app_runner.command()
def start(
    num_procs: runner_num_procs_opt = 1,
    log_level: log_level_opt = LogLevel.INFO.value,
):
    """
    Start the Runner as a daemon
    """
    dm = DaemonManager()
    with loading_spinner(False) as progress:
        progress.add_task(description="Starting the daemon...", total=None)
        try:
            dm.start(
                log_level=log_level.value, num_procs=num_procs, raise_on_error=True
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
    dm = DaemonManager()
    with loading_spinner(False) as progress:
        progress.add_task(description="Stopping the daemon...", total=None)
        try:
            dm.stop(wait=wait, raise_on_error=True)
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def kill():
    """
    Send a kill signal to the Runner processes.
    Return immediately, does not wait for processes to be killed.
    """
    dm = DaemonManager()
    with loading_spinner(False) as progress:
        progress.add_task(description="Killing the daemon...", total=None)
        try:
            dm.kill(raise_on_error=True)
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while killing the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def shut_down():
    """
    Shuts down the supervisord process.
    Note that if the daemon is running it will wait for the daemon to stop.
    """
    dm = DaemonManager()
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
    dm = DaemonManager()
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
        DaemonStatus.RUNNING: "green",
    }[current_status]
    text = Text()
    text.append("Daemon status: ")
    text.append(current_status.value.lower(), style=color)
    out_console.print(text)


@app_runner.command()
def pids():
    """
    Fetch the process ids of the daemon.
    Both the supervisord process and the processing running the Runner.
    """
    dm = DaemonManager()
    with loading_spinner():
        try:
            pids_dict = dm.get_pids()
            if not pids_dict:
                exit_with_warning_msg("Daemon is not running")
            table = Table()
            table.add_column("Process")
            table.add_column("PID")

            for name, pid in pids_dict.items():
                table.add_row(name, str(pid))

        except DaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon: {getattr(e, 'message', e)}"
            )

    out_console.print(table)
