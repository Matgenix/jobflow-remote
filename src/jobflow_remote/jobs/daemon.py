from __future__ import annotations

import logging
import subprocess
from enum import Enum
from pathlib import Path
from string import Template
from xmlrpc.client import Fault

import psutil
from monty.os import makedirs_p
from supervisor import childutils
from supervisor.states import RUNNING_STATES, STOPPED_STATES, ProcessStates
from supervisor.xmlrpc import Faults

from jobflow_remote.config import ConfigManager, Project

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
command=jf -p $project runner run -pid -log $loglevel
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
command=jf -p $project runner run -pid --checkout -log $loglevel
autostart=true
autorestart=false
numprocs=1
process_name=run_jobflow_checkout
stopwaitsecs=86400

[program:runner_daemon_transfer]
priority=100
command=jf -p $project runner run -pid --transfer -log $loglevel
autostart=true
autorestart=false
numprocs=$num_procs_transfer
process_name=run_jobflow_transfer%(process_num)s
stopwaitsecs=86400

[program:runner_daemon_queue]
priority=100
command=jf -p $project runner run -pid --queue -log $loglevel
autostart=true
autorestart=false
numprocs=1
process_name=run_jobflow_queue
stopwaitsecs=86400

[program:runner_daemon_complete]
priority=100
command=jf -p $project runner run -pid --complete -log $loglevel
autostart=true
autorestart=false
numprocs=$num_procs_complete
process_name=run_jobflow_complete%(process_num)s
stopwaitsecs=86400
"""


class DaemonError(Exception):
    pass


class DaemonStatus(Enum):
    SHUT_DOWN = "SHUT_DOWN"
    STOPPED = "STOPPED"
    STOPPING = "STOPPING"
    PARTIALLY_RUNNING = "PARTIALLY_RUNNING"
    RUNNING = "RUNNING"


class DaemonManager:
    conf_template_single = Template(supervisord_conf_str)
    conf_template_split = Template(supervisord_conf_str_split)

    def __init__(
        self,
        daemon_dir: str | Path,
        log_dir: str | Path,
        project: Project,
    ):
        self.project = project
        self.daemon_dir = Path(daemon_dir).absolute()
        self.log_dir = Path(log_dir).absolute()

    @classmethod
    def from_project(cls, project: Project):
        daemon_dir = project.daemon_dir
        log_dir = project.log_dir
        return cls(daemon_dir, log_dir, project)

    @classmethod
    def from_project_name(cls, project_name: str | None = None):
        config_manager = ConfigManager()
        project = config_manager.get_project(project_name)
        return cls.from_project(project)

    @property
    def conf_filepath(self) -> Path:
        return self.daemon_dir / "supervisord.conf"

    @property
    def pid_filepath(self) -> Path:
        return self.daemon_dir / "supervisord.pid"

    @property
    def log_filepath(self) -> Path:
        return self.log_dir / "supervisord.log"

    @property
    def sock_filepath(self) -> Path:
        path = self.daemon_dir / "s.sock"
        if len(str(path)) > 97:
            msg = f"socket path {path} is too long for UNIX systems. Set the daemon_dir value in the project configuration so that the socket path is shorter"
            raise DaemonError(msg)
        return path

    def clean_files(self):
        self.pid_filepath.unlink(missing_ok=True)
        self.sock_filepath.unlink(missing_ok=True)

    def get_interface(self):
        env = {
            "SUPERVISOR_SERVER_URL": f"unix://{str(self.sock_filepath)}",
            "SUPERVISOR_USERNAME": "",
            "SUPERVISOR_PASSWORD": "",
        }
        interface = childutils.getRPCInterface(env)
        return interface

    def get_supervisord_pid(self) -> int | None:
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

        if not running:
            if pid is not None:
                logger.warning(
                    f"Process with pid {pid} is not running but daemon files are present. Cleaning them up."
                )
            self.clean_files()

        return running

    def check_status(self) -> DaemonStatus:
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
        except Fault as e:
            # catch this exception as it may be raised if the status is queried while
            # the supervisord process is shutting down. The error is quite cryptic, so
            # replace with one that is clearer. Also see a related issue in supervisord:
            # https://github.com/Supervisor/supervisor/issues/48
            if e.faultString == "SHUTDOWN_STATE":
                raise DaemonError(
                    "The daemon is likely shutting down and the actual state cannot be determined"
                )
            raise
        if not proc_info:
            raise DaemonError(
                "supervisord process is running but no daemon process is present"
            )

        if all(pi.get("state") in RUNNING_STATES for pi in proc_info):
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
        process_active = self.check_supervisord_process()

        if not process_active:
            return None

        pids = {"supervisord": {"pid": self.get_supervisord_pid(), "state": "RUNNING"}}

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
            pids[pi.get("name")] = {"pid": pi.get("pid"), "state": pi.get("statename")}

        return pids

    def write_config(
        self,
        num_procs_transfer: int = 1,
        num_procs_complete: int = 1,
        single: bool = True,
        log_level: str = "info",
        nodaemon: bool = False,
    ):
        if single:
            conf = self.conf_template_single.substitute(
                sock_file=str(self.sock_filepath),
                pid_file=str(self.pid_filepath),
                log_file=str(self.log_filepath),
                nodaemon="true" if nodaemon else "false",
                project=self.project.name,
                loglevel=log_level,
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
    ) -> str | None:
        makedirs_p(self.daemon_dir)
        makedirs_p(self.log_dir)
        self.write_config(
            num_procs_transfer=num_procs_transfer,
            num_procs_complete=num_procs_complete,
            single=single,
            log_level=log_level,
            nodaemon=nodaemon,
        )
        cp = subprocess.run(
            f"supervisord -c {str(self.conf_filepath)}",
            shell=True,
            capture_output=True,
            text=True,
        )
        if cp.returncode != 0:
            return f"Error staring the supervisord process. stdout: {cp.stdout}. stderr: {cp.stderr}"

        # TODO check if actually started?

        return None

    def start_processes(self) -> str | None:
        interface = self.get_interface()
        result = interface.supervisor.startAllProcesses()
        if not result:
            return "No process started"

        failed = [r for r in result if r.get("status") == Faults.SUCCESS]
        if len(failed) == 0:
            return None
        elif len(failed) != len(result):
            msg = "Not all the daemon processes started correctly. Details: \n"
            for f in failed:
                msg += f"  - {f.get('description')}\n"
            return msg

        return None

    def start(
        self,
        num_procs_transfer: int = 1,
        num_procs_complete: int = 1,
        single: bool = True,
        log_level: str = "info",
        raise_on_error: bool = False,
    ) -> bool:
        status = self.check_status()
        if status == DaemonStatus.RUNNING:
            error = "Daemon process is already running"
        elif status == DaemonStatus.SHUT_DOWN:
            error = self.start_supervisord(
                num_procs_transfer=num_procs_transfer,
                num_procs_complete=num_procs_complete,
                single=single,
                log_level=log_level,
            )
        elif status == DaemonStatus.STOPPED:
            self.shut_down(raise_on_error=raise_on_error)
            error = self.start_supervisord(
                num_procs_transfer=num_procs_transfer,
                num_procs_complete=num_procs_complete,
                single=single,
                log_level=log_level,
            )
            # else:
            #     error = self.start_processes()
        elif status == DaemonStatus.STOPPING:
            error = "Daemon process are stopping. Cannot start."
        else:
            error = f"Daemon status {status} could not be handled"

        if error is not None:
            if raise_on_error:
                raise DaemonError(error)
            else:
                logger.error(error)
                return False
        return True

    def stop(self, wait: bool = False, raise_on_error: bool = False) -> bool:
        status = self.check_status()
        if status in (
            DaemonStatus.STOPPED,
            DaemonStatus.STOPPING,
            DaemonStatus.SHUT_DOWN,
        ):
            return True

        if status == DaemonStatus.RUNNING:
            interface = self.get_interface()
            if wait:
                result = interface.supervisor.stopAllProcesses()
            else:
                result = interface.supervisor.signalAllProcesses(15)

            error = self._verify_call_result(result, "stop", raise_on_error)

            return error is None

        raise DaemonError(f"Daemon status {status} could not be handled")

    def _verify_call_result(
        self, result, action: str, raise_on_error: bool = False
    ) -> str | None:
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
            else:
                logger.error(error)
                return error

        return None

    def kill(self, raise_on_error: bool = False) -> bool:
        status = self.check_status()
        if status == DaemonStatus.SHUT_DOWN:
            logger.info("supervisord is not running. No process is running")
            return True

        if status in (DaemonStatus.RUNNING, DaemonStatus.STOPPING):
            interface = self.get_interface()
            result = interface.supervisor.signalAllProcesses(9)
            error = self._verify_call_result(result, "kill", raise_on_error)

            return error is None

        raise DaemonError(f"Daemon status {status} could not be handled")

    def shut_down(self, raise_on_error: bool = False) -> bool:
        status = self.check_status()
        if status == DaemonStatus.SHUT_DOWN:
            logger.info("supervisord is already shut down.")
            return True
        interface = self.get_interface()
        try:
            interface.supervisor.shutdown()
        except Exception:
            if raise_on_error:
                raise
            return False
        return True
