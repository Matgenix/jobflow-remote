import datetime
import os
from typing import Annotated

import typer
from rich.prompt import Confirm
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    break_lock_opt,
    force_opt,
    log_level_opt,
    verbosity_opt,
)
from jobflow_remote.cli.utils import (
    exit_with_error_msg,
    exit_with_warning_msg,
    fmt_datetime,
    get_config_manager,
    get_job_controller,
    loading_spinner,
    out_console,
)
from jobflow_remote.config.base import LogLevel
from jobflow_remote.jobs.daemon import (
    DaemonError,
    DaemonManager,
    DaemonStatus,
    RunningDaemonError,
)
from jobflow_remote.jobs.runner import Runner
from jobflow_remote.utils.data import convert_utc_time

app_runner = JFRTyper(
    name="runner", help="Commands for handling the Runner", no_args_is_help=True
)
app.add_typer(app_runner)


_running_daemon_error_msg = (
    "\nIf no runner is active on that machine clean the DB with `jf runner reset`"
)


@app_runner.command()
def run(
    log_level: log_level_opt = LogLevel.INFO,
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
    connect_interactive: Annotated[
        bool,
        typer.Option(
            "--connect-interactive",
            "-i",
            help="Activate the connection for interactive remote host",
        ),
    ] = False,
) -> None:
    """
    Execute the Runner in the foreground.
    Do NOT execute this to start as a daemon.
    Should be used by the daemon or for testing purposes.
    """
    runner_id = os.getpid() if set_pid else None
    runner = Runner(
        log_level=log_level,
        runner_id=str(runner_id),
        connect_interactive=connect_interactive,
    )
    if not (transfer or complete or queue or checkout):
        transfer = complete = queue = checkout = True

    try:
        runner.run(transfer=transfer, complete=complete, queue=queue, checkout=checkout)
    finally:
        runner.cleanup()


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
    log_level: log_level_opt = LogLevel.INFO,
    connect_interactive: Annotated[
        bool,
        typer.Option(
            "--connect-interactive",
            "-i",
            help="Wait for the daemon to start and manually log in the "
            "connection for interactive remote host. Requires --single.",
        ),
    ] = False,
) -> None:
    """Start the Runner as a daemon."""
    # This is not a strict requirement, but for the moment only allow the single
    # process daemon
    if connect_interactive and not single:
        exit_with_error_msg("--connect-interactive option requires --single")
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        task_id = progress.add_task(description="Starting the daemon...", total=None)
        try:
            dm.start(
                num_procs_transfer=transfer,
                num_procs_complete=complete,
                single=single,
                log_level=log_level.value,
                raise_on_error=True,
                connect_interactive=connect_interactive,
            )
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while starting the daemon: {getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while starting the daemon: {getattr(e, 'message', e)}"
            )
        if connect_interactive:
            progress.update(task_id, description="Waiting for processes to start...")
            try:
                dm.wait_start()
            except DaemonError as e:
                exit_with_error_msg(
                    f"Error while waiting the processes to start: {getattr(e, 'message', e)}"
                )
    if connect_interactive:
        dm.foreground_processes(print_function=out_console.print)


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
    ] = False,
) -> None:
    """
    Send a stop signal to the Runner processes.
    Each of the Runner processes will stop when finished the task being executed.
    By default, return immediately.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Stopping the daemon...", total=None)
        try:
            dm.stop(wait=wait, raise_on_error=True)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon: {getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
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
def kill() -> None:
    """
    Send a kill signal to the Runner processes.
    Return immediately, does not wait for processes to be killed.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Killing the daemon...", total=None)
        try:
            dm.kill(raise_on_error=True)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while killing the daemon: {getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while killing the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def shutdown() -> None:
    """
    Shuts down the supervisord process.
    Note that if the daemon is running it will wait for the daemon to stop.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Shutting down supervisor...", total=None)
        try:
            dm.shut_down(raise_on_error=True)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while shutting down supervisor: {getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while shutting down supervisor: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def status() -> None:
    """Fetch the status of the daemon runner."""
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
        DaemonStatus.STARTING: "gold1",
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
def info(verbosity: verbosity_opt = 0) -> None:
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
            f"Error while fetching information from the daemon: {getattr(e, 'message', e)}"
        )
    if not procs_info_dict:
        out_console.print("Daemon is not running", style="gold1")
    else:
        table = Table()
        table.add_column("Process")
        table.add_column("PID")
        table.add_column("State")

        for name, proc_info in procs_info_dict.items():
            table.add_row(name, str(proc_info["pid"]), str(proc_info["statename"]))

        out_console.print(table)

    # add the information about the running_runner according to the DB
    jc = get_job_controller()
    running_runner_doc = jc.get_running_runner()

    # empty line
    out_console.print("")
    if running_runner_doc:
        out_console.print("Data about running runner in the DB:")
        if verbosity == 0:
            running_runner_doc.pop("processes_info", None)
        # convert dates at the first level and for the remote error
        for k, v in running_runner_doc.items():
            if isinstance(v, datetime.datetime):
                running_runner_doc[k] = convert_utc_time(v).strftime(fmt_datetime)
        out_console.print(render_scope(running_runner_doc))
    else:
        out_console.print("No running runner defined in the DB")


@app_runner.command()
def foreground() -> None:
    """Connect to the daemon processes in the foreground."""
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    procs_info_dict = None
    try:
        with loading_spinner():
            procs_info_dict = dm.get_processes_info()
    except DaemonError as e:
        exit_with_error_msg(
            f"Error while fetching information from the daemon: {getattr(e, 'message', e)}"
        )
    if not procs_info_dict:
        exit_with_warning_msg("Daemon is not running")
    dm.foreground_processes(print_function=out_console.print)


@app_runner.command()
def reset(
    force: force_opt = False,
    break_lock: break_lock_opt = False,
) -> None:
    """
    Reset the value of the machine executing the runner from the database.
    Should be executed only if it is certain that the runner is not active on that
    machine anymore.
    """
    jc = get_job_controller()

    running_runner = jc.get_running_runner()
    if running_runner in ("NO_DOCUMENT", None):
        exit_with_warning_msg("No running runner present in the database")
        raise typer.Exit(0)
    if not force:
        text = Text.from_markup(
            "[red]This operation will remove the information about the current "
            "running runner from the database:[/red]\n"
            f"- hostname: {running_runner['hostname']}\n"
            f"- project_name: {running_runner['project_name']}\n"
            f"- start_time: {running_runner['start_time']}\n"
            f"- last_pinged: {running_runner['last_pinged']}\n"
            f"- daemon_dir: {running_runner['daemon_dir']}\n"
            f"- user: {running_runner['user']}\n"
            "[red]Do you want to proceed?[\red]"
        )

        confirmed = Confirm.ask(text, default=False)
        if not confirmed:
            raise typer.Exit(0)

    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Resetting runner information...", total=None)
        jc.clean_running_runner(break_lock=break_lock)

    out_console.print("The running runner document was reset")
