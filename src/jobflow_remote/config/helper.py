from __future__ import annotations

import logging

from jobflow_remote.config.base import (
    ExecutionConfig,
    LaunchPadConfig,
    LocalHostConfig,
    Machine,
    Project,
    RemoteHostConfig,
)


def generate_dummy_project(name: str, full: bool = False) -> Project:

    rh = generate_dummy_host("remote")
    hosts = [rh]
    remote_machine = generate_dummy_machine(scheduler_type="slurm", host_id=rh.host_id)
    machines = [remote_machine]
    exec_config = []
    if full:
        lh = generate_dummy_host("local")
        hosts.append(lh)
        local_machine = generate_dummy_machine(
            scheduler_type="shell", host_id=lh.host_id
        )
        machines.append(local_machine)
        exec_config = [generate_dummy_exec_config()]

    lpad_config = generate_dummy_launchpad_config()

    jobstore = generate_dummy_jobstore()

    p = Project(
        name=name,
        log_level=logging.DEBUG,
        hosts=hosts,
        jobstore=jobstore,
        run_db=lpad_config,
        machines=machines,
        exec_config=exec_config,
    )

    return p


def generate_dummy_host(host_type: str) -> RemoteHostConfig | LocalHostConfig:
    if host_type == "local":
        return LocalHostConfig(
            host_type="local",
            host_id="test_local_host",
            timeout_execute=60,
        )
    elif host_type == "remote":
        return RemoteHostConfig(
            host_type="remote",
            host_id="test_remote_host",
            host="remote.host.net",
            user="bob",
            timeout_execute=60,
        )
    else:
        raise ValueError(f"Unknown host type {host_type}")


def generate_dummy_machine(
    scheduler_type: str = "slurm", host_id: str = "test_remote_host"
) -> Machine:
    return Machine(
        machine_id=f"machine_{host_id}",
        scheduler_type=scheduler_type,
        host_id=host_id,
        work_dir="/path/to/run/folder",
        pre_run="source /path/to/python/environment/activate",
    )


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


def generate_dummy_launchpad_config() -> LaunchPadConfig:
    lp_config = LaunchPadConfig(
        host="localhost",
        port=27017,
        name="db_name",
        username="bob",
        password="secret_password",
    )
    return lp_config
