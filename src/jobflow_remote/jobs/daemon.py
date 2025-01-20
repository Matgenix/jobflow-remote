from __future__ import annotations

import contextlib
import datetime
import getpass
import logging
import os
import re
import socket
import subprocess
import time
from enum import Enum
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Callable
from xmlrpc.client import Fault

import psutil
from monty.os import makedirs_p
from supervisor import childutils, states, xmlrpc
from supervisor.compat import xmlrpclib
from supervisor.options import ClientOptions
from supervisor.states import RUNNING_STATES, STOPPED_STATES, ProcessStates
from supervisor.supervisorctl import Controller, fgthread
from supervisor.xmlrpc import Faults

from jobflow_remote.config import ConfigManager, Project
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.utils.db import MongoLock, RunnerLockedError

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)


supervisord_conf_str = """
[unix_http_server]
file=$sock_file

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisord]
logfile=$log_file
logfile_maxbytes=10MB
logfile_backups=5
loglevel=$loglevel
pidfile=$pid_file
nodaemon=$nodaemon

[supervisorctl]
serverurl=unix://$sock_file

[program:runner_daemon]
priority=100
command=jf -p $project runner run -pid -log $loglevel $connect_interactive
autostart=true
autorestart=false
numprocs=1
process_name=run_jobflow%(process_num)s
stopwaitsecs=86400
"""


supervisord_conf_str_split = """
[unix_http_server]
file=$sock_file

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisord]
logfile=$log_file
logfile_maxbytes=10MB
logfile_backups=5
loglevel=$loglevel
pidfile=$pid_file
nodaemon=$nodaemon

[supervisorctl]
serverurl=unix://$sock_file

[program:runner_daemon_checkout]
priority=100
command=jf -p $project runner run -pid --checkout -log $loglevel $connect_interactive
autostart=true
autorestart=false
numprocs=1
process_name=run_jobflow_checkout
stopwaitsecs=86400

[program:runner_daemon_transfer]
priority=100
command=jf -p $project runner run -pid --transfer -log $loglevel $connect_interactive
autostart=true
autorestart=false
numprocs=$num_procs_transfer
process_name=run_jobflow_transfer%(process_num)s
stopwaitsecs=86400

[program:runner_daemon_queue]
priority=100
command=jf -p $project runner run -pid --queue -log $loglevel $connect_interactive
autostart=true
autorestart=false
numprocs=1
process_name=run_jobflow_queue
stopwaitsecs=86400

[program:runner_daemon_complete]
priority=100
command=jf -p $project runner run -pid --complete -log $loglevel $connect_interactive
autostart=true
autorestart=false
numprocs=$num_procs_complete
process_name=run_jobflow_complete%(process_num)s
stopwaitsecs=86400
"""


class DaemonError(Exception):
    pass


class RunningDaemonError(Exception):
    pass


class DaemonStatus(Enum):
    """
    Possible states of the daemon.
    """

    SHUT_DOWN = "SHUT_DOWN"
    STOPPED = "STOPPED"
    STOPPING = "STOPPING"
    PARTIALLY_RUNNING = "PARTIALLY_RUNNING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"


class DaemonManager:
    """
    A manager for handling the daemonized Runner processes.

    Checks are performed to avoid starting a daemon in two different machines.
    To perform the actions on the daemon process the information about the runner
    in the DB should match with the one of the current machine.
    """

    conf_template_single = Template(supervisord_conf_str)
    conf_template_split = Template(supervisord_conf_str_split)

    def __init__(
        self,
        daemon_dir: str | Path,
        log_dir: str | Path,
        project: Project,
        job_controller: JobController | None = None,
    ) -> None:
        """
        Parameters
        ----------
        daemon_dir
            Directory where the supervisord configuration file is written.
        log_dir
            Directory where the supervisord log file is written.
        project
            Project configuration.
        job_controller
            JobController instance, used internally to handle the job queue.
        """
        self.project = project
        self.daemon_dir = Path(daemon_dir).absolute()
        self.log_dir = Path(log_dir).absolute()
        self._job_controller = job_controller

    @classmethod
    def from_project(cls, project: Project) -> DaemonManager:
        """
        Generate a DaemonManager instance from a Project configuration.

        Parameters
        ----------
        project
            Project configuration.

        Returns
        -------
        DaemonManager
            An instance of DaemonManager associated with the project.
        """
        daemon_dir = project.daemon_dir
        log_dir = project.log_dir
        return cls(daemon_dir, log_dir, project)

    @classmethod
    def from_project_name(cls, project_name: str | None = None) -> DaemonManager:
        """
        Generate a DaemonManager instance from a project name.

        Parameters
        ----------
        project_name
            Name of the project to use. If None, the default project will be used.

        Returns
        -------
        DaemonManager
            An instance of DaemonManager associated with the project.
        """
        config_manager = ConfigManager()
        project = config_manager.get_project(project_name)
        return cls.from_project(project)

    @property
    def conf_filepath(self) -> Path:
        """
        Path to the supervisord configuration file.
        """
        return self.daemon_dir / "supervisord.conf"

    @property
    def pid_filepath(self) -> Path:
        """
        Path to the supervisord PID file.
        """
        return self.daemon_dir / "supervisord.pid"

    @property
    def log_filepath(self) -> Path:
        """
        Path to the supervisord log file.
        """
        return self.log_dir / "supervisord.log"

    @property
    def sock_filepath(self) -> Path:
        """
        Path to the supervisord socket file.

        This path is used by supervisord to communicate with the client.
        The length of the path is checked to ensure it is not too long for UNIX systems.
        """
        path = self.daemon_dir / "s.sock"
        if len(str(path)) > 97:
            msg = f"socket path {path} is too long for UNIX systems. Set the daemon_dir value "
            "in the project configuration so that the socket path is shorter"
            raise DaemonError(msg)
        return path

    @property
    def job_controller(self) -> JobController:
        """
        JobController instance associated with the project.
        """
        if self._job_controller is None:
            self._job_controller = JobController.from_project(self.project)
        return self._job_controller

    def clean_files(self) -> None:
        """
        Clean up the supervisord PID and socket files.
        """
        self.pid_filepath.unlink(missing_ok=True)
        self.sock_filepath.unlink(missing_ok=True)

    def get_interface(self):
        """
        Return an interface to the supervisord RPC server using the socket file.

        Returns
        -------
        xmlrpc.ServerProxy
            Interface to the supervisord RPC server.
        """
        env = {
            "SUPERVISOR_SERVER_URL": f"unix://{self.sock_filepath!s}",
            "SUPERVISOR_USERNAME": "",
            "SUPERVISOR_PASSWORD": "",
        }
        return childutils.getRPCInterface(env)

    def get_supervisord_pid(self) -> int | None:
        """
        Get the PID of the supervisord process from the PID file.

        If the PID file does not exist or can not be parsed, return None.

        Returns
        -------
        int | None
            PID of the supervisord process.
        """
        pid_fp = self.pid_filepath

        if not pid_fp.is_file():
            return None

        try:
            with open(pid_fp) as f:
                pid = int(f.read().strip())
        except ValueError:
            logger.warning(f"The pid file {pid_fp} could not be parsed")
            return None
        return pid

    def check_supervisord_process(self) -> bool:
        """
        Check if the supervisord process is running and belongs to the same user.

        If the supervisord process is not running but the daemon files are present, clean them up.

        Returns
        -------
        bool
            True if the supervisord process is running and belongs to the same user, False otherwise.
        """
        pid = self.get_supervisord_pid()

        running = True
        if pid is None:
            running = False

        try:
            process = psutil.Process(pid)

            for cmdline_element in process.cmdline():
                if cmdline_element.endswith("supervisord"):
                    break
            else:
                running = False

            if process.username() != psutil.Process().username():
                logger.warning(
                    f"pid {pid} is running supervisord, but belongs to a different user"
                )
                running = False
        except psutil.NoSuchProcess:
            running = False

        if not running and pid is not None:
            logger.warning(
                f"Process with pid {pid} is not running but daemon files are present. Cleaning them up."
            )
            self.clean_files()

        return running

    def check_status(self) -> DaemonStatus:
        """
        Get the current status of the daemon based on the state of the supervisord
        process and the running processes.

        Returns
        -------
        DaemonStatus
            Status of the daemon. Can be one of the following:
        """
        process_active = self.check_supervisord_process()

        if not process_active:
            return DaemonStatus.SHUT_DOWN

        if not self.sock_filepath.is_socket():
            raise DaemonError(
                "the supervisord process is alive, but the socket is missing"
            )

        interface = self.get_interface()
        try:
            proc_info = interface.supervisor.getAllProcessInfo()
        except Fault as exc:
            # catch this exception as it may be raised if the status is queried while
            # the supervisord process is shutting down. The error is quite cryptic, so
            # replace with one that is clearer. Also see a related issue in supervisord:
            # https://github.com/Supervisor/supervisor/issues/48
            if exc.faultString == "SHUTDOWN_STATE":
                raise DaemonError(
                    "The daemon is likely shutting down and the actual state cannot be determined"
                ) from exc
            raise
        if not proc_info:
            raise DaemonError(
                "supervisord process is running but no daemon process is present"
            )

        if all(pi.get("state") in RUNNING_STATES for pi in proc_info):
            if any(pi.get("state") == ProcessStates.STARTING for pi in proc_info):
                return DaemonStatus.STARTING
            return DaemonStatus.RUNNING

        if any(pi.get("state") in RUNNING_STATES for pi in proc_info):
            return DaemonStatus.PARTIALLY_RUNNING

        if all(pi.get("state") in STOPPED_STATES for pi in proc_info):
            return DaemonStatus.STOPPED

        if all(
            pi.get("state") in (ProcessStates.STOPPED, ProcessStates.STOPPING)
            for pi in proc_info
        ):
            return DaemonStatus.STOPPING

        raise DaemonError("Could not determine the current status of the daemon")

    def get_processes_info(self) -> dict[str, dict] | None:
        """
        Get the information about the processes of the daemon.

        None if the daemon is not running.

        Returns
        -------
        dict
            A dictionary with the information about the processes.
            The keys are the process names and the values are dictionaries
            with the process information.
        """
        process_active = self.check_supervisord_process()

        if not process_active:
            return None

        pids = {
            "supervisord": {
                "pid": self.get_supervisord_pid(),
                "statename": "RUNNING",
                "state": ProcessStates.RUNNING,
                "group": None,
            }
        }

        if not self.sock_filepath.is_socket():
            raise DaemonError(
                "the supervisord process is alive, but the socket is missing"
            )

        interface = self.get_interface()
        proc_info = interface.supervisor.getAllProcessInfo()
        if not proc_info:
            raise DaemonError(
                "supervisord process is running but no daemon process is present"
            )

        for pi in proc_info:
            name = f"{pi.get('group')}:{pi.get('name')}"
            pids[name] = pi

        return pids

    def write_config(
        self,
        num_procs_transfer: int = 1,
        num_procs_complete: int = 1,
        single: bool = True,
        log_level: str = "info",
        nodaemon: bool = False,
        connect_interactive: bool = False,
    ) -> None:
        """
        Write the configuration file for the daemon.

        If `single` is True, a configuration to execute all the Runner tasks
        in a single process is written. Otherwise, a split configuration is
        written with separate sections for each task of the Runner.

        Parameters
        ----------
        num_procs_transfer : int
            Number of processes to use for the transfer step.
        num_procs_complete : int
            Number of processes to use for the complete step.
        single : bool, optional
            Write a single configuration file instead of a split one.
        log_level : str, optional
            The log level to use for the daemon.
        nodaemon : bool, optional
            Run the daemon in foreground.
        connect_interactive : bool, optional
            Allow the daemon to perform an interactive initial setup.
        """
        if single:
            conf = self.conf_template_single.substitute(
                sock_file=str(self.sock_filepath),
                pid_file=str(self.pid_filepath),
                log_file=str(self.log_filepath),
                nodaemon="true" if nodaemon else "false",
                project=self.project.name,
                loglevel=log_level,
                connect_interactive=(
                    "--connect-interactive" if connect_interactive else ""
                ),
            )
        else:
            conf = self.conf_template_split.substitute(
                sock_file=str(self.sock_filepath),
                pid_file=str(self.pid_filepath),
                log_file=str(self.log_filepath),
                num_procs_transfer=num_procs_transfer,
                num_procs_complete=num_procs_complete,
                nodaemon="true" if nodaemon else "false",
                project=self.project.name,
                loglevel=log_level,
                connect_interactive=(
                    "--connect-interactive" if connect_interactive else ""
                ),
            )
        with open(self.conf_filepath, "w") as f:
            f.write(conf)

    def start_supervisord(
        self,
        num_procs_transfer: int = 1,
        num_procs_complete: int = 1,
        single: bool = True,
        log_level: str = "info",
        nodaemon: bool = False,
        connect_interactive: bool = False,
    ) -> str | None:
        """
        Start the supervisord daemon and all the processes.

        Parameters
        ----------
        num_procs_transfer
            Number of processes to use for the transfer step.
        num_procs_complete
            Number of processes to use for the complete step.
        single
            Write a single configuration file instead of a split one.
        log_level
            The log level to use for the daemon.
        nodaemon : bool, optional
            Run the daemon in foreground.
        connect_interactive : bool, optional
            Allow the daemon to perform an interactive initial setup.

        Returns
        -------
        str | None
            An error message if the daemon could not be started, otherwise None.
        """
        makedirs_p(self.daemon_dir)
        makedirs_p(self.log_dir)
        self.write_config(
            num_procs_transfer=num_procs_transfer,
            num_procs_complete=num_procs_complete,
            single=single,
            log_level=log_level,
            nodaemon=nodaemon,
            connect_interactive=connect_interactive,
        )
        cp = subprocess.run(
            f"supervisord -c {self.conf_filepath!s}",
            shell=True,
            capture_output=True,
            text=True,
            check=False,
        )
        if cp.returncode != 0:
            return f"Error staring the supervisord process. stdout: {cp.stdout}. stderr: {cp.stderr}"

        # TODO check if actually started?

        return None

    def start_processes(self) -> str | None:
        """
        Start all the processes of the daemon when the supervisord processes
        is running but the daemon processes are stopped.

        Returns
        -------
        str | None
            An error message if not all the processes started correctly, otherwise None.
        """
        interface = self.get_interface()
        result = interface.supervisor.startAllProcesses()
        if not result:
            return "No process started"

        failed = [r for r in result if r.get("status") != Faults.SUCCESS]
        if len(failed) == 0:
            return None

        msg = "Not all the daemon processes started correctly. Details: \n"
        for f in failed:
            msg += f"  - {f.get('description')} - {f.get('status')}\n"
        return msg

    def start(
        self,
        num_procs_transfer: int = 1,
        num_procs_complete: int = 1,
        single: bool = True,
        log_level: str = "info",
        raise_on_error: bool = False,
        connect_interactive: bool = False,
    ) -> bool:
        """
        Start the daemon by starting the supervisord process and all the processes of the daemon.

        Parameters
        ----------
        num_procs_transfer
            Number of processes to use for the transfer of jobs.
        num_procs_complete
            Number of processes to use for the completion of jobs.
        single
            If True, the runner will be started with a single process.
        log_level : str
            Log level of the daemon.
        raise_on_error : bool
            If True, raise an exception if an error occurs.
        connect_interactive : bool
            Allow the daemon to perform an interactive initial setup.

        Returns
        -------
        bool
            True if the daemon is started correctly, False otherwise.
        """
        with self.lock_runner_doc() as lock:
            doc = lock.locked_document
            doc_error = self._check_running_runner(doc, raise_on_error=raise_on_error)
            if doc_error:
                logger.error(doc_error)
                return False
            status = self.check_status()
            if status == DaemonStatus.RUNNING:
                error = "Daemon process is already running"
            elif status == DaemonStatus.SHUT_DOWN:
                error = self.start_supervisord(
                    num_procs_transfer=num_procs_transfer,
                    num_procs_complete=num_procs_complete,
                    single=single,
                    log_level=log_level,
                    connect_interactive=connect_interactive,
                )
            elif status == DaemonStatus.STOPPED:
                # Supervisor sets the processes configuration when the supervisord
                # process is started. In the STOPPED state it is not possible to switch
                # them. Initially the code used to perform a shut_down here before
                # restarting, but this would require handling the lock mechanism in
                # shut_down() and made pointless the concept of STOP.
                # Now the user is warned that the runner restarted with the same
                # old configuration.
                old_config = self.parse_config_file()
                new_config = {
                    "num_procs_transfer": num_procs_transfer,
                    "num_procs_complete": num_procs_complete,
                    "single": single,
                    "log_level": log_level,
                    "connect_interactive": connect_interactive,
                }
                diffs_config = []
                for config_name, config_value in new_config.items():
                    if config_value != old_config.get(config_name):
                        diffs_config.append(config_name)
                if diffs_config:
                    logger.warning(
                        f"Daemon is {DaemonStatus.STOPPED.value}, but the options {', '.join(diffs_config)} "
                        "differ from the values used to activate supervisor. The daemon will start with the initial "
                        "configurations. To change the configuration shut down the daemon and start it again."
                    )
                error = self.start_processes()
            elif status == DaemonStatus.STOPPING:
                error = "Daemon process are stopping. Cannot start."
            else:
                error = f"Daemon status {status} could not be handled"

            if error is not None:
                if raise_on_error:
                    raise DaemonError(error)
                logger.error(error)
                return False
            lock.update_on_release = {
                "$set": {"running_runner": self._get_runner_info()}
            }
            return True

    def stop(self, wait: bool = False, raise_on_error: bool = False) -> bool:
        """
        Stop the daemon.

        Parameters
        ----------
        wait
            If True wait until the daemon has stopped.
        raise_on_error
            Raise an exception if the daemon cannot be stopped.

        Returns
        -------
        bool
            True if the daemon was stopped successfully, False otherwise.
        """
        with self.lock_runner_doc(allow_missing=True) as lock:
            # This check is done to allow users to switch off the runner
            # even if the running_runner document is not present.
            # If the document is locked the error will be raised by lock_runner_doc
            if lock.locked_document:
                doc_error = self._check_running_runner(
                    lock.locked_document, raise_on_error=raise_on_error
                )
                if doc_error:
                    logger.error(doc_error)
                    return False
            status = self.check_status()
            if status in (
                DaemonStatus.STOPPED,
                DaemonStatus.STOPPING,
                DaemonStatus.SHUT_DOWN,
            ):
                lock.update_on_release = {"$set": {"running_runner": None}}
                return True

            if status in (DaemonStatus.RUNNING, DaemonStatus.PARTIALLY_RUNNING):
                interface = self.get_interface()
                if wait:
                    result = interface.supervisor.stopAllProcesses()
                else:
                    result = interface.supervisor.signalAllProcesses(15)

                error = self._verify_call_result(result, "stop", raise_on_error)

                if error is None:
                    lock.update_on_release = {"$set": {"running_runner": None}}
                return error is None

            raise DaemonError(f"Daemon status {status} could not be handled")

    def _verify_call_result(
        self, result, action: str, raise_on_error: bool = False
    ) -> str | None:
        """
        Verify that the result of the XML-RPC call to send a signal to the
        processes completed correctly.

        Parameters
        ----------
        result
            The result of the XML-RPC call.
        action
            The action that was requested.
        raise_on_error
            If True, raise an exception if the result is not correct.

        Returns
        -------
        str | None
            The error message if the operation was not executed correctly, None otherwise.
        """
        error = None
        if not result:
            error = f"The action {action} was not applied to the processes"
        else:
            failed = [r for r in result if r.get("status") == Faults.SUCCESS]
            if len(failed) != len(result):
                error = f"The action {action} was not applied to all the processes. Details: \n"
                for f in failed:
                    error += f"  - {f.get('description')}\n"

        if error is not None:
            if raise_on_error:
                raise DaemonError(error)
            logger.error(error)
            return error

        return None

    def kill(self, raise_on_error: bool = False) -> bool:
        with self.lock_runner_doc() as lock:
            doc_error = self._check_running_runner(
                lock.locked_document, raise_on_error=raise_on_error
            )
            if doc_error:
                logger.error(doc_error)
                return False
            # If the daemon is shutting down supervisord may not be able to identify
            # the state. Try proceeding in that case, since we really want to kill
            # the process
            status = None
            try:
                status = self.check_status()
                if status == DaemonStatus.SHUT_DOWN:
                    logger.info("supervisord is not running. No process is running")
                    lock.update_on_release = {"$set": {"running_runner": None}}
                    return True
                if status == DaemonStatus.STOPPED:
                    logger.info("Processes are already stopped.")
                    lock.update_on_release = {"$set": {"running_runner": None}}
                    return True
            except DaemonError as e:
                msg = (
                    f"Error while determining the state of the runner: {getattr(e, 'message', str(e))}."
                    f"Proceeding with the kill command."
                )
                logger.warning(msg)

            if status in (
                None,
                DaemonStatus.RUNNING,
                DaemonStatus.STOPPING,
                DaemonStatus.PARTIALLY_RUNNING,
            ):
                interface = self.get_interface()
                result = interface.supervisor.signalAllProcesses(9)
                error = self._verify_call_result(result, "kill", raise_on_error)

                if error is None:
                    lock.update_on_release = {"$set": {"running_runner": None}}
                return error is None

        raise DaemonError(f"Daemon status {status} could not be handled")

    def shut_down(self, raise_on_error: bool = False) -> bool:
        """
        Shut down the supervisord process and all the processes of the daemon.

        Parameters
        ----------
        raise_on_error
            If True, raise an exception if an error occurs.

        Returns
        -------
        bool
            True if the daemon is shut down correctly, False otherwise.
        """
        with self.lock_runner_doc(allow_missing=True) as lock:
            # This check is done to allow users to switch off the runner
            # even if the running_runner document is not present.
            # If the document is locked the error will be raised by lock_runner_doc
            if lock.locked_document:
                doc_error = self._check_running_runner(
                    lock.locked_document, raise_on_error=raise_on_error
                )
                if doc_error:
                    logger.error(doc_error)
                    return False
            status = self.check_status()
            if status == DaemonStatus.SHUT_DOWN:
                logger.info("supervisord is already shut down.")
                lock.update_on_release = {"$set": {"running_runner": None}}
                return True
            interface = self.get_interface()
            try:
                interface.supervisor.shutdown()
            except Exception:
                if raise_on_error:
                    raise
                return False
            lock.update_on_release = {"$set": {"running_runner": None}}
            return True

    def wait_start(self, timeout: int = 30) -> None:
        """
        Wait for all processes of the daemon to start.

        Parameters
        ----------
        timeout
            Maximum time in seconds to wait for all processes to start.
        """
        time_limit = time.time() + timeout
        while True:
            processes_info = self.get_processes_info()
            all_started = True
            for name, proc_info in processes_info.items():
                if proc_info["state"] not in RUNNING_STATES:
                    raise DaemonError(f"Process {name} is not in a running state")
                if proc_info["state"] != ProcessStates.RUNNING:
                    all_started = False
                    break
            if all_started:
                break
            if time.time() > time_limit:
                raise DaemonError(
                    f"The processes did not start within {timeout} seconds"
                )
            time.sleep(2)

    def foreground_processes(
        self,
        processes_names: list | None = None,
        print_function: Callable | None = None,
    ) -> None:
        """
        Attach to the foreground of the processes of the daemon.

        Parameters
        ----------
        processes_names
            Names of the processes to attach to. If None, all processes are
            attached to.
        print_function
            A function to use to print the output of the processes. If None,
            the default print function is used.
        """
        processes_info = self.get_processes_info()
        if processes_names is None:
            processes_names = [pn for pn in processes_info if pn != "supervisord"]

        for name in processes_names:
            if name not in processes_info:
                raise ValueError(
                    f"Process with name {name} is not among the available processes"
                )
            if processes_info[name]["state"] != ProcessStates.RUNNING:
                raise RuntimeError(
                    f"Process {name} is not running. Cannot attach to it."
                )
            self.foreground_process(name, print_function)

    def foreground_process(self, name, print_function: Callable | None = None) -> None:
        """
        Attach to the foreground of one process based on the process name.

        Parameters
        ----------
        name
            Name of the process to attach to.
        print_function
            A function to use to print the output of the process. If None,
            the default print function is used.
        """
        # This is adapted from supervisor.supervisorctl.DefaultControllerPlugin.do_fg
        a = None
        if print_function is None:
            print_function = print
        try:
            ctl = self.get_controller()

            supervisor = ctl.get_supervisor()

            print_function(f"Entering foreground for process {name} (CTRL+C to exit)")

            # this thread takes care of the output/error messages
            a = fgthread(name, ctl)
            a.start()

            # this takes care of the user input
            while True:
                # Always avoid echoing the output. It may not be a password, but since
                # the daemon process cannot control the choice here, it is safer to
                # hide everything
                # inp = raw_input() + '\n'
                inp = getpass.getpass("") + "\n"
                try:
                    supervisor.sendProcessStdin(name, inp)
                except xmlrpclib.Fault as e:
                    if e.faultCode == xmlrpc.Faults.NOT_RUNNING:
                        print_function("Process got killed")
                    else:
                        print_function("ERROR: " + str(e))
                    print_function("Exiting foreground")
                    a.kill()
                    return

                info = supervisor.getProcessInfo(name)
                if info["state"] != states.ProcessStates.RUNNING:
                    print_function("Process got killed")
                    print_function("Exiting foreground")
                    a.kill()
                    return
        except (KeyboardInterrupt, EOFError):
            print_function("Exiting foreground")
            if a:
                a.kill()

    def _get_runner_info(self) -> dict:
        """
        Generate a dictionary with the information about the runner and
        the system where it is being executed.
        """
        try:
            user = os.getlogin()
        except OSError:
            user = os.environ.get("USER", None)
        # Note that this approach may give a different MAC address for the
        # same machine, if more than one network device is present (this
        # may also include local virtual machines). Consider replacing this
        # with a more stable choice (e.g. the first one in alphabetical order,
        # excluding virtual interfaces)
        mac_address = None
        found = False
        for addrs in psutil.net_if_addrs().values():
            if found:
                break
            for addr in addrs:
                if (
                    addr.family == psutil.AF_LINK
                    and addr.address != "00:00:00:00:00:00"
                ):
                    mac_address = addr.address
                    found = True
                    break
        return {
            "processes_info": self.get_processes_info(),
            "hostname": socket.gethostname(),
            "mac_address": mac_address,
            "user": user,
            "daemon_dir": self.project.daemon_dir,
            "project_name": self.project.name,
            "start_time": datetime.datetime.now(),
            "last_pinged": datetime.datetime.now(),
            "runner_options": self.project.runner.model_dump(mode="json"),
        }

    def get_controller(self) -> Controller:
        """
        Return a Supervisor `supervisor.supervisorctl.Controller`
        connected to the supervisord configured in the project.
        """
        options = ClientOptions()
        args = ["-c", self.conf_filepath]
        options.realize(args, doc="")
        return Controller(options)

    @contextlib.contextmanager
    def lock_runner_doc(
        self, allow_missing: bool = False
    ) -> Generator[MongoLock, None, None]:
        """
        Context manager to lock a document in the auxiliary collection that is used to keep
        track of the currently running runner.

        Parameters
        ----------
        allow_missing
            Determine if it is acceptable that the document is not present in the DB.
            If not, raise an error. Mainly present to handle databases that have been
            created with version <=0.1.4.
        """
        db_filter = {"running_runner": {"$exists": True}}
        with self.job_controller.lock_auxiliary(
            filter=db_filter, get_locked_doc=True
        ) as lock:
            doc = lock.locked_document
            if not doc:
                if lock.unavailable_document:
                    raise RunnerLockedError.from_runner_doc(lock.unavailable_document)
                if not allow_missing:
                    raise ValueError(
                        "No daemon runner document.\n"
                        "The auxiliary collection does not contain information about running daemon. "
                        "Your database was likely set up or reset using an old version of Jobflow Remote. "
                        'You can upgrade the database using the command "jf admin upgrade".'
                    )
            yield lock

    def _check_running_runner(
        self, doc: dict, raise_on_error: bool = False
    ) -> str | None:
        """
        Check if the information in the DB about the running runner matches the information about the local machine.

        Parameters
        ----------
        doc
            The document from the DB with the information about the running runner.
        raise_on_error
            If True, raise an error if the information does not match, otherwise return a string with the error message.

        Returns
        -------
        str | None
            If the information matches, return None. Otherwise, return a string with the error message.
        """
        if not doc["running_runner"]:
            return None

        db_data = doc["running_runner"]

        local_data = self._get_runner_info()
        # not testing on the MAC address as it may change due to identifying
        # different network devices on the same machine or changing in
        # cloud VMs.
        data_to_check = [
            "hostname",
            "project_name",
            "user",
            "daemon_dir",
        ]
        for data in data_to_check:
            if local_data[data] != db_data[data]:
                break
        else:
            logger.info(
                "The DB reports that there is a running runner and it corresponds to this machine"
            )
            return None

        error = (
            "A daemon runner process may be running on a different machine.\n"
            "Here is the information retrieved from the database:\n"
            f"- hostname: {db_data['hostname']}\n"
            f"- project_name: {db_data['project_name']}\n"
            f"- start_time: {db_data['start_time']}\n"
            f"- last_pinged: {db_data['last_pinged']}\n"
            f"- daemon_dir: {db_data['daemon_dir']}\n"
            f"- user: {db_data['user']}"
        )
        if raise_on_error:
            raise RunningDaemonError(error)
        return error

    def check_matching_runner(
        self, raise_on_error: bool = False, allow_missing: bool = False
    ) -> str | None:
        """
        Checks if the information stored in the runner document in the auxiliary collection
        matches the current machine.

        Parameters
        ----------
        raise_on_error
            If True and the information does not match, raise an error.
        allow_missing
            If True, return None instead of raising an error if the document is not present.

        Returns
        -------
        str | None
            If the information matches, return None. Otherwise, return a string with the error message.
        """
        with self.lock_runner_doc(allow_missing=allow_missing) as lock:
            doc = lock.locked_document
            if allow_missing and not doc:
                return None
            return self._check_running_runner(doc, raise_on_error=raise_on_error)

    def parse_config_file(self) -> dict:
        """
        Parses a supervisord config file and returns the extracted input variables.
        This includes values for `num_procs_transfer`, `num_procs_complete`, `single`,
        `log_level`, `nodaemon`, and `connect_interactive`.

        Returns
        -------
        dict
            Dictionary containing the extracted input variables.
        """
        variables = {
            "num_procs_transfer": 1,
            "num_procs_complete": 1,
            "single": False,
            "log_level": "info",
            "nodaemon": False,
            "connect_interactive": False,
        }

        with open(self.conf_filepath) as f:
            content = f.read()

        # Check if this is the 'single' configuration
        if "[program:runner_daemon]" in content:
            variables["single"] = True
        else:
            variables["single"] = False

            # Extract num_procs_transfer and num_procs_complete if they exist in the split configuration
            match_transfer = re.search(r"numprocs\s*=\s*(\d+)", content, re.MULTILINE)
            if match_transfer:
                variables["num_procs_transfer"] = int(match_transfer.group(1))

            match_complete = re.search(
                r"\[program:runner_daemon_complete\][^[]*numprocs\s*=\s*(\d+)",
                content,
                re.MULTILINE,
            )
            if match_complete:
                variables["num_procs_complete"] = int(match_complete.group(1))

        # Extract loglevel
        match_loglevel = re.search(r"loglevel\s*=\s*(\w+)", content)
        if match_loglevel:
            variables["log_level"] = match_loglevel.group(1)

        # Extract nodaemon
        match_nodaemon = re.search(r"nodaemon\s*=\s*(\w+)", content)
        if match_nodaemon:
            variables["nodaemon"] = match_nodaemon.group(1) == "true"

        # Extract connect_interactive
        if "--connect-interactive" in content:
            variables["connect_interactive"] = True

        return variables
