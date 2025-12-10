import datetime
import os
from typing import Annotated

import typer
from rich.prompt import Confirm
from rich.scope import render_scope
from rich.table import Table
from rich.text import Text

from jobflow_remote.cli.formatting import get_runner_pings_table
from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.types import (
    break_lock_opt,
    force_opt_deprecated,
    log_level_opt,
    verbosity_opt,
    yes_opt,
)
from jobflow_remote.cli.utils import (
    check_stopped_runner,
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
    "\nRun `jf runner info` to get more details about the active runner reported in the database.\n"
    "If you are [bold]absolutely sure that no other runner is active on another machine "
    "or for another project[/bold], clean the DB with `jf runner reset`.\n"
    "[bold]If you are unsure about the meaning of this message check the documentation:[/bold] [link=https://matgenix.github.io/jobflow-remote/user/troubleshooting.html]https://matgenix.github.io/jobflow-remote/user/troubleshooting.html[/link]"
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
    daemonized: Annotated[
        bool,
        typer.Option(
            "--daemonized",
            "-d",
            help="If selected, indicates that the daemon is started inside a daemon. The parent PID will be added.",
        ),
    ] = False,
) -> None:
    """
    Execute the Runner in the foreground.
    Do NOT execute this to start as a daemon.
    Should be used by the daemon or for testing purposes.
    """
    runner_id = os.getpid() if set_pid else None
    daemon_id = None
    if daemonized:
        daemon_id = os.getppid()
    runner = Runner(
        log_level=log_level,
        runner_id=str(runner_id),
        connect_interactive=connect_interactive,
        daemon_id=str(daemon_id),
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
                f"Error while starting the daemon:\n{getattr(e, 'message', e)}{_running_daemon_error_msg}"
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


@app_runner.command(hidden=True)
def stop_processes(
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
    stop_sent = False
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Stopping the daemon...", total=None)
        try:
            stop_sent = dm.stop(wait=wait, raise_on_error=True)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon:\n{getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while stopping the daemon: {getattr(e, 'message', e)}"
            )
    from jobflow_remote import SETTINGS

    if stop_sent and not wait and SETTINGS.cli_suggestions:
        out_console.print(
            "The stop signal has been sent to the Runner. Run 'jf runner status' to verify if it stopped",
            style="yellow",
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
    ] = False,
) -> None:
    """
    Shuts down the supervisord process.
    Note that the supervisord process will stop after all the runner processes have finished.
    """
    shutdown(wait=wait)


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
                f"Error while killing the daemon:\n{getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while killing the daemon: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def shutdown(
    wait: Annotated[
        bool,
        typer.Option(
            "--wait",
            "-w",
            help=(
                "Wait until the daemon has shut down. NOTE: this may take a while if a large file is being transferred!"
            ),
        ),
    ] = False,
) -> None:
    """
    Shuts down the supervisord process.
    Note that the supervisord process will stop after all the runner processes have finished
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Shutting down supervisor...", total=None)
        try:
            dm.shut_down(raise_on_error=True, wait=wait)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while shutting down supervisor:\n{getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while shutting down supervisor: {getattr(e, 'message', e)}"
            )


@app_runner.command()
def restart() -> None:
    """
    Restart the runner. Send a stop signal, wait for the runner to stop and
    restart it. The options to start the runner (e.g. --single) remain the same.
    """
    cm = get_config_manager()
    dm = DaemonManager.from_project(cm.get_project())
    with loading_spinner(processing=False) as progress:
        progress.add_task(description="Restarting the daemon...", total=None)
        try:
            dm.restart(raise_on_error=True)
        except RunningDaemonError as e:
            exit_with_error_msg(
                f"Error while restarting the daemon:\n{getattr(e, 'message', e)}{_running_daemon_error_msg}"
            )
        except DaemonError as e:
            exit_with_error_msg(
                f"Error while restaring the daemon: {getattr(e, 'message', e)}"
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
def info(
    verbosity: verbosity_opt = 0,
    pings: Annotated[
        bool,
        typer.Option(
            "--pings",
            "-p",
            help=("Also show all the ping entries"),
        ),
    ] = False,
) -> None:
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

    # in any case fetch the data from the pings and warn if the last ping
    # is not from the active runner.
    pings_data = jc.get_runner_pings()

    if pings:
        out_console.print("")
        pings_table = get_runner_pings_table(pings_data)
        out_console.print(pings_table)

    if pings_data:
        last_ping = pings_data[-1]
        diffs = []
        if running_runner_doc:
            data_to_check = [
                "hostname",
                "project_name",
                "user",
                "daemon_dir",
            ]
            diffs = [
                d
                for d in data_to_check
                if running_runner_doc.get(d) != last_ping.get(d)
            ]
        if procs_info_dict and str(
            procs_info_dict.get("supervisord", {}).get("pid")
        ) != str(last_ping["daemon_id"]):
            diffs.append("daemon_id")

        if diffs:
            warn_msg = (
                "There is an inconsistency between the actual runner information "
                "and the last ping in the database.\n"
                "This suggests that another runner process associated to this database "
                "may be already running\n"
            )
            helper_sentences = {
                "daemon_id": "running under another supervisor process",
                "hostname": "active on another machine",
                "project_name": "associated to another Project",
                "user": "associated to another user",
                "daemon_dir": "using a different project folder",
            }
            for d in diffs:
                warn_msg += f"- It seems to be {helper_sentences[d]}: {last_ping[d]}\n"
            warn_msg += f"- The last time it pinged the database was: {last_ping['time']} (UTC)\n"

            warn_msg += (
                "Run the command with the --pings option to get a list of all the last pings "
                "from the runners in the database and consult the documentation "
                "[link=https://matgenix.github.io/jobflow-remote/user/troubleshooting.html]https://matgenix.github.io/jobflow-remote/user/troubleshooting.html[/link]"
            )
            out_console.print(warn_msg, style="gold1")


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
    yes_all: yes_opt = False,
    force_deprecated: force_opt_deprecated = False,
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
    if not yes_all:
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

        # Also clean supervisord files, if they exist.
        cm = get_config_manager()
        dm = DaemonManager.from_project(cm.get_project())
        dm.clean_files()

    out_console.print("The running runner document was reset")


@app_runner.command()
def update_status(
    log_level: log_level_opt = LogLevel.INFO,
    set_pid: Annotated[
        bool,
        typer.Option(
            "--set-pid",
            "-pid",
            help="Set the runner id to the current process pid",
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
    Update the "submitted" and "running" states of jobs, flows and batches.

    This command updates the `"submitted"` and `"running"` states of jobs, flows, and batches.
    It does not perform submissions, uploads, downloads, or any other side-effecting actions.

    - Only jobs in a `SUBMITTED`, `BATCH_SUBMITTED`, `RUNNING`, or `BATCH_RUNNING` state are updated.
    - Flows currently do not change state (though this may be extended in the future).
    - Batch jobs in `SUBMITTED` or `RUNNING` states are updated.

    This command is primarily intended for upgrade scenarios where:
    - The runner must reflect the latest job statuses.
    - No new jobs should be submitted.
    - Actions such as uploads or database insertions are intentionally skipped.

    Note that this command should be run only when the runner is stopped to avoid inconsistent states.
    """
    check_stopped_runner(error=True)

    runner_id = os.getpid() if set_pid else None
    runner = Runner(
        log_level=log_level,
        runner_id=str(runner_id),
        connect_interactive=connect_interactive,
    )

    try:
        runner.check_run_status()
        if runner.batch_workers:
            runner.update_batch_jobs(submit=False)
    finally:
        runner.cleanup()
