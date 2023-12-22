from __future__ import annotations

import logging
import traceback

from jobflow import JobStore
from maggma.core import Store

from jobflow_remote.config.base import (
    ExecutionConfig,
    LocalWorker,
    Project,
    RemoteWorker,
    WorkerBase,
)
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

    p = Project(
        name=name,
        jobstore=jobstore,
        queue=queue,
        workers=workers,
        exec_config=exec_config,
    )

    return p


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
    elif host_type == "remote":
        d.update(
            type="remote",
            host="remote.host.net",
            user="bob",
            timeout_execute=60,
        )
        return RemoteWorker(**d)


def generate_dummy_jobstore() -> dict:
    jobstore_dict = {
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

    return jobstore_dict


def generate_dummy_exec_config() -> ExecutionConfig:
    exec_config = ExecutionConfig(
        modules=["GCC/10.2.0", "OpenMPI/4.0.5-GCC-10.2.0"],
        export={"PATH": "/path/to/binaries:$PATH"},
        pre_run="conda activate env_name",
    )
    return exec_config


def generate_dummy_queue() -> dict:
    lp_config = dict(
        type="MongoStore",
        host="localhost",
        database="db_name",
        username="bob",
        password="secret_password",
        collection_name="jobs",
    )
    return lp_config


def _check_workdir(worker: WorkerBase, host: BaseHost) -> str | None:
    """Check that the configured workdir exists or is writable on the worker.

    Parameters:
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

    try:
        canary_file = worker.work_dir / ".jf_heartbeat"
        host.write_text_file(canary_file, "\n")
        return None
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Could not write to {canary_file} on {worker.host}. Does the folder exist on the remote?\nThe folder should be specified as an absolute path with no shell expansions or environment variables."
        ) from exc
    finally:
        # Must be enclosed in quotes with '!r' as the path may contain spaces
        host.execute(f"rm {str(canary_file)!r}")


def check_worker(worker: WorkerBase) -> str | None:
    """Check that a connection to the configured worker can be made."""
    host = worker.get_host()
    try:
        host.connect()
        host_error = host.test()
        if host_error:
            return host_error

        from jobflow_remote.remote.queue import QueueManager

        qm = QueueManager(scheduler_io=worker.get_scheduler_io(), host=host)
        qm.get_jobs_list()

        _check_workdir(worker=worker, host=host)

    except Exception:
        exc = traceback.format_exc()
        return f"Error while testing worker:\n {exc}"
    finally:
        try:
            host.close()
        except Exception:
            logger.warning(f"error while closing connection to host {host}")

    return None


def _check_store(store: Store) -> str | None:
    try:
        store.connect()
        store.query_one()
    except Exception:
        exc = traceback.format_exc()
        return exc
    finally:
        store.close()

    return None


def check_queue_store(queue_store: Store):
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
