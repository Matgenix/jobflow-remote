from __future__ import annotations

from jobflow_remote.config.base import (
    ExecutionConfig,
    LocalWorker,
    Project,
    RemoteWorker,
    WorkerBase,
)


def generate_dummy_project(name: str, full: bool = False) -> Project:

    remote_worker = generate_dummy_worker(scheduler_type="slurm", host_type="remote")
    workers = {"example_worker": remote_worker}
    exec_config = []
    if full:
        local_worker = generate_dummy_worker(scheduler_type="shell", host_type="local")
        workers["example_local"] = local_worker
        exec_config = [generate_dummy_exec_config()]

    queue = generate_dummy_queue()

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
        exec_config_id="example_config",
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
