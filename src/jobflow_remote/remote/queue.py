from __future__ import annotations

from pathlib import Path

from qtoolkit.core.data_objects import CancelResult, QJob, QResources, SubmissionResult
from qtoolkit.io.base import BaseSchedulerIO
from qtoolkit.io.shell import ShellIO

from jobflow_remote.remote.host import BaseHost

OUT_FNAME = "queue.out"
ERR_FNAME = "queue.err"


def set_name_out(
    resources: dict | QResources,
    name: str,
    out_fpath: str | Path = OUT_FNAME,
    err_fpath: str | Path = ERR_FNAME,
):
    # sanitize the name
    name = name.replace(" ", "_")
    if isinstance(resources, QResources):
        resources.job_name = name
        resources.output_filepath = out_fpath
        resources.error_filepath = err_fpath
    else:
        resources["job_name"] = name
        resources["qout_path"] = out_fpath
        resources["qerr_path"] = err_fpath


class QueueManager:
    """Base class for job queues.

    Attributes
    ----------
    scheduler_io : str
        Name of the queue
    host : BaseHost
        Host where the command should be executed.
    """

    def __init__(
        self,
        scheduler_io: BaseSchedulerIO,
        host: BaseHost,
        timeout_exec: int | None = None,
    ):
        self.scheduler_io = scheduler_io
        self.host = host
        self.timeout_exec = timeout_exec

    def execute_cmd(
        self, cmd: str, workdir: str | Path | None = None, timeout: int | None = None
    ):
        """Execute a command.

        Parameters
        ----------
        cmd : str
            Command to be executed
        workdir: str or None
            path where the command will be executed.

        Returns
        -------
        stdout : str
        stderr : str
        exit_code : int
        """
        timeout = timeout if timeout is not None else self.timeout_exec
        return self.host.execute(cmd, workdir, timeout)

    def get_submission_script(
        self,
        commands: str | list[str] | None,
        options: dict | QResources | None = None,
        work_dir: str | Path | None = None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
    ) -> str:
        """ """
        commands_list = []
        if change_dir := self.get_change_dir(work_dir):
            commands_list.append(change_dir)
        if pre_run := self.get_pre_run(pre_run):
            commands_list.append(pre_run)
        if export_str := self.get_export(export):
            commands_list.append(export_str)
        if modules_str := self.get_modules(modules):
            commands_list.append(modules_str)
        if run_commands := self.get_run_commands(commands):
            commands_list.append(run_commands)
        if post_run := self.get_post_run(post_run):
            commands_list.append(post_run)
        return self.scheduler_io.get_submission_script(commands_list, options)

    def get_change_dir(self, dir_path: str | Path | None) -> str:
        if dir_path:
            return f"cd {dir_path}"
        return ""

    def get_pre_run(self, pre_run: str | list[str] | None) -> str:
        if isinstance(pre_run, (list, tuple)):
            return "\n".join(pre_run)
        return pre_run

    def get_export(self, exports: dict | None) -> str | None:
        if not exports:
            return None
        exports_str = []
        for k, v in exports.items():
            exports_str.append(f"export {k}={v}")
        return "\n".join(exports_str)

    def get_modules(self, modules: list[str] | None) -> str | None:
        if not modules:
            return None
        modules_str = []
        for m in modules:
            modules_str.append(f"module load {m}")
        return "\n".join(modules_str)

    def get_run_commands(self, commands) -> str:
        if isinstance(commands, str):
            return commands
        elif isinstance(commands, list):
            return "\n".join(commands)
        else:
            raise ValueError("commands should be a str or a list of str.")

    def get_post_run(self, post_run: str | list[str] | None) -> str:
        if isinstance(post_run, (list, tuple)):
            return "\n".join(post_run)
        return post_run

    def submit(
        self,
        commands: str | list[str] | None,
        options=None,
        work_dir=None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
        script_fname="submit.sh",
        create_submit_dir: bool = False,
        timeout: int | None = None,
    ) -> SubmissionResult:
        script_fpath = self.write_submission_script(
            commands=commands,
            options=options,
            work_dir=work_dir,
            pre_run=pre_run,
            post_run=post_run,
            export=export,
            modules=modules,
            script_fname=script_fname,
            create_submit_dir=create_submit_dir,
        )
        submit_cmd = self.scheduler_io.get_submit_cmd(script_fpath)
        stdout, stderr, returncode = self.execute_cmd(
            submit_cmd, work_dir, timeout=timeout
        )
        return self.scheduler_io.parse_submit_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def write_submission_script(
        self,
        commands: str | list[str] | None,
        options=None,
        work_dir=None,
        pre_run: str | list[str] | None = None,
        post_run: str | list[str] | None = None,
        export: dict | None = None,
        modules: list[str] | None = None,
        script_fname="submit.sh",
        create_submit_dir: bool = False,
    ):
        script_str = self.get_submission_script(
            commands=commands,
            options=options,
            work_dir=work_dir,
            pre_run=pre_run,
            post_run=post_run,
            export=export,
            modules=modules,
        )
        if create_submit_dir and work_dir:
            created = self.host.mkdir(work_dir, recursive=True, exist_ok=True)
            if not created:
                raise RuntimeError("failed to create directory")
        if work_dir:
            script_fpath = Path(work_dir, script_fname)
        else:
            script_fpath = Path(script_fname)
        self.host.write_text_file(script_fpath, script_str)
        return script_fpath

    def cancel(self, job: QJob | int | str, timeout: int | None = None) -> CancelResult:
        cancel_cmd = self.scheduler_io.get_cancel_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(cancel_cmd, timeout=timeout)
        return self.scheduler_io.parse_cancel_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_job(self, job: QJob | int | str, timeout: int | None = None) -> QJob | None:
        job_cmd = self.scheduler_io.get_job_cmd(job)
        stdout, stderr, returncode = self.execute_cmd(job_cmd, timeout=timeout)
        return self.scheduler_io.parse_job_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_jobs_list(
        self,
        jobs: list[QJob | int | str] | None = None,
        user: str | None = None,
        timeout: int | None = None,
    ) -> list[QJob]:
        job_cmd = self.scheduler_io.get_jobs_list_cmd(jobs, user)
        stdout, stderr, returncode = self.execute_cmd(job_cmd, timeout=timeout)
        return self.scheduler_io.parse_jobs_list_output(
            exit_code=returncode, stdout=stdout, stderr=stderr
        )

    def get_shell_manager(self):
        return QueueManager(
            scheduler_io=ShellIO(), host=self.host, timeout_exec=self.timeout_exec
        )
