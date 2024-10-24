from __future__ import annotations

import importlib.metadata
import json
import logging
import traceback
from typing import TYPE_CHECKING

from jobflow_remote.config.base import (
    ExecutionConfig,
    LocalWorker,
    Project,
    RemoteWorker,
    WorkerBase,
)

if TYPE_CHECKING:
    from jobflow import JobStore
    from maggma.core import Store

    from jobflow_remote.remote.host import BaseHost

logger = logging.getLogger(__name__)


def generate_dummy_project(name: str, full: bool = False) -> Project:
    remote_worker = generate_dummy_worker(scheduler_type="slurm", host_type="remote")
    workers = {"example_worker": remote_worker}
    exec_config = {}
    if full:
        local_worker = generate_dummy_worker(scheduler_type="shell", host_type="local")
        workers["example_local"] = local_worker
        exec_config = {"example_config": generate_dummy_exec_config()}

    queue = {"store": generate_dummy_queue()}

    jobstore = generate_dummy_jobstore()

    return Project(
        name=name,
        jobstore=jobstore,
        queue=queue,
        workers=workers,
        exec_config=exec_config,
    )


def generate_dummy_worker(
    scheduler_type: str = "slurm", host_type: str = "remote"
) -> WorkerBase:
    d: dict = dict(
        scheduler_type=scheduler_type,
        work_dir="/path/to/run/folder",
        pre_run="source /path/to/python/environment/activate",
    )
    if host_type == "local":
        d.update(
            type="local",
            timeout_execute=60,
        )
        return LocalWorker(**d)
    if host_type == "remote":
        d.update(
            type="remote",
            host="remote.host.net",
            user="bob",
            timeout_execute=60,
        )
        return RemoteWorker(**d)

    raise ValueError(f"Unknown/unhandled host type: {host_type}")


def generate_dummy_jobstore() -> dict:
    return {
        "docs_store": {
            "type": "MongoStore",
            "database": "db_name",
            "host": "host.mongodb.com",
            "port": 27017,
            "username": "bob",
            "password": "secret_password",
            "collection_name": "outputs",
        },
        "additional_stores": {
            "data": {
                "type": "GridFSStore",
                "database": "db_name",
                "host": "host.mongodb.com",
                "port": 27017,
                "username": "bob",
                "password": "secret_password",
                "collection_name": "outputs_blobs",
            }
        },
    }


def generate_dummy_exec_config() -> ExecutionConfig:
    return ExecutionConfig(
        modules=["GCC/10.2.0", "OpenMPI/4.0.5-GCC-10.2.0"],
        export={"PATH": "/path/to/binaries:$PATH"},
        pre_run="conda activate env_name",
    )


def generate_dummy_queue() -> dict:
    return dict(
        type="MongoStore",
        host="localhost",
        database="db_name",
        username="bob",
        password="secret_password",
        collection_name="jobs",
    )


def _check_workdir(worker: WorkerBase, host: BaseHost) -> str | None:
    """Check that the configured workdir exists or is writable on the worker.

    Parameters
    ----------
        worker: The worker configuration.
        host: A connected host.

    """
    try:
        host_error = host.test()
        if host_error:
            return host_error
    except Exception:
        exc = traceback.format_exc()
        return f"Error while testing worker:\n {exc}"

    canary_file = worker.work_dir / ".jf_heartbeat"
    try:
        # First try to create the folder. The runner will create is anyway and
        # it should be less confusing for the user.
        host.mkdir(worker.work_dir)
        host.write_text_file(canary_file, "\n")
        return None  # noqa: TRY300
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Could not write to {canary_file}. Does the folder exist on the remote?\nThe folder should be specified as an absolute path with no shell expansions or environment variables."
        ) from exc
    except PermissionError as exc:
        raise PermissionError(
            f"Could not write to {canary_file}. Do you have the rights to access that folder?"
        ) from exc
    finally:
        # Must be enclosed in quotes with '!r' as the path may contain spaces
        host.execute(f"rm {str(canary_file)!r}")


def check_worker(
    worker: WorkerBase, full_check: bool = False
) -> tuple[str | None, str | None]:
    """Check that a connection to the configured worker can be made."""
    host = worker.get_host()
    worker_warn = None
    try:
        host.connect()
        host_error = host.test()
        if host_error:
            return host_error, None

        from jobflow_remote.remote.queue import QueueManager

        qm = QueueManager(scheduler_io=worker.get_scheduler_io(), host=host)
        qm.get_jobs_list()

        workdir_err = _check_workdir(worker=worker, host=host)
        if workdir_err:
            return workdir_err, None

        # don't perform the environment check, as they will be equivalent
        if worker.type != "local":
            worker_warn = _check_environment(
                worker=worker, host=host, full_check=full_check
            )

    except Exception:
        exc = traceback.format_exc()
        return f"Error while testing worker:\n {exc}", worker_warn
    finally:
        try:
            host.close()
        except Exception:
            logger.warning(f"error while closing connection to host {host}")

    return None, worker_warn


def _check_store(store: Store) -> str | None:
    try:
        store.connect()
        store.query_one()
    except Exception:
        return traceback.format_exc()
    finally:
        store.close()

    return None


def check_queue_store(queue_store: Store) -> str | None:
    err = _check_store(queue_store)
    if err:
        return f"Error while checking queue store:\n{err}"
    return None


def check_jobstore(jobstore: JobStore) -> str | None:
    err = _check_store(jobstore.docs_store)
    if err:
        return f"Error while checking docs_store store:\n{err}"
    for store_name, store in jobstore.additional_stores.items():
        err = _check_store(store)
        if err:
            return f"Error while checking additional store {store_name}:\n{err}"
    return None


def _check_environment(
    worker: WorkerBase, host: BaseHost, full_check: bool = False
) -> str | None:
    """Check that the worker has a python environment with the same versions of libraries.

    Parameters
    ----------
        host: A connected host.
        full_check: Whether to check the entire environment and not just jobflow and jobflow-remote.

    Returns
    -------
    str | None
        A message describing the environment mismatches. None if no mismatch is found.
    """
    installed_packages = importlib.metadata.distributions()
    local_package_versions = {
        package.metadata["Name"]: package.version for package in installed_packages
    }
    cmd = "pip list --format=json"
    if worker.pre_run:
        cmd = "; ".join(worker.pre_run.strip().splitlines()) + "; " + cmd

    stdout, stderr, errcode = host.execute(cmd)
    if errcode != 0:
        return f"Error while checking the compatibility of the environments: {stderr}"
    host_package_versions = {
        package_dict["name"]: package_dict["version"]
        for package_dict in json.loads(stdout)
    }
    if full_check:
        packages_to_check = list(local_package_versions.keys())
    else:
        packages_to_check = ["jobflow", "jobflow-remote"]
    missing = []
    mismatch = []
    for package in packages_to_check:
        if package not in host_package_versions:
            missing.append((package, local_package_versions[package]))
            continue
        if local_package_versions[package] != host_package_versions[package]:
            mismatch.append(
                (
                    package,
                    local_package_versions[package],
                    host_package_versions[package],
                )
            )
    msg = None
    if mismatch or missing:
        msg = "Note: inconsistencies may be due to the proper python environment not being correctly loaded.\n"
    if missing:
        missing_str = [f"{m[0]} - {m[1]}" for m in missing]
        msg += f"Missing packages: {', '.join(missing_str)}. "
    if mismatch:
        mismatch_str = [f"{m[0]} - {m[1]} vs {m[2]}" for m in mismatch]
        msg += f"Mismatching versions: {', '.join(mismatch_str)}"

    return msg
