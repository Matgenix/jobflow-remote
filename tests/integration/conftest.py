from __future__ import annotations

import os
import random
import shutil
import socket
import tempfile
import time
from functools import partialmethod
from pathlib import Path

import fabric
import pytest
from coverage import Coverage
from monty.os import cd
from python_on_whales import DockerClient
from python_on_whales import docker as docker_pow


@pytest.fixture(autouse=True)
def mock_fabric_run(monkeypatch) -> None:
    monkeypatch.setattr(
        fabric.Connection, "run", partialmethod(fabric.Connection.run, in_stream=False)
    )


def _get_free_port(upper_bound=90_000):
    """Returns a random free port, with an upper bound.

    The upper bound is required as Docker does not have
    permissions on high port numbers on some systems.

    """
    port = upper_bound + 1
    attempts = 0
    max_attempts = 10
    while port > upper_bound and attempts < max_attempts:
        sock = socket.socket()
        sock.bind(("", 0))
        port = sock.getsockname()[1]
        attempts += 1

    if attempts == max_attempts:
        raise RuntimeError(
            f"Could not find a free port to use with the provided {upper_bound=}."
        )

    return port


def _get_random_name(length=6):
    return "".join(random.choice("abcdef") for _ in range(length))


@pytest.fixture(scope="session")
def slurm_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def sge_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def pbs_ssh_port():
    """The exposed local port for SSH connections to the queue container."""
    return _get_free_port()


@pytest.fixture(scope="session")
def db_port():
    """The exposed local port for connections to the MongoDB stores."""
    return _get_free_port()


@pytest.fixture(scope="session", autouse=True)
def bake_containers():
    hcl_path = Path(__file__).parent.resolve() / "dockerfiles/docker-bake.hcl"
    docker_pow.buildx.bake(
        targets=["slurm", "sge", "pbs"],
        files=hcl_path,
        set={"*.context": str(Path(__file__).parent.parent.parent.resolve())},
    )


@pytest.fixture(scope="session", autouse=True)
def compose_containers(
    slurm_ssh_port, sge_ssh_port, pbs_ssh_port, db_port, bake_containers, coverage_file
):
    compose_yaml = f"""
name: jobflow_remote_testing
services:
  mongo:
    image: mongo:7
    ports:
      - "{db_port}:27017"
    restart: always
    container_name: mongo_container
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.runCommand('ping').ok"]
      interval: 1s
      timeout: 1s
      retries: 28
      start_period: 2s

  jobflow_remote_testing_slurm:
    image: ghcr.io/matgenix/jobflow-remote-testing-slurm:latest
    container_name: jobflow_testing_slurm
    ports:
      - "{slurm_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s

  jobflow_remote_testing_sge:
    image: ghcr.io/matgenix/jobflow-remote-testing-sge:latest
    container_name: jobflow_testing_sge
    ports:
      - "{sge_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s

  jobflow_remote_testing_pbs:
    image: ghcr.io/matgenix/jobflow-remote-testing-pbs:latest
    container_name: jobflow_testing_pbs
    ports:
      - "{pbs_ssh_port}:22"
    stdin_open: true
    tty: true
    healthcheck:
      test: ["CMD", "bash", "-c", "</dev/tcp/localhost/22"]
      interval: 1s
      timeout: 1s
      retries: 30
      start_period: 2s
"""
    with tempfile.NamedTemporaryFile("wt", suffix="compose.yaml", delete=False) as f:
        f.write(compose_yaml)
        f.flush()

        docker_client = DockerClient(compose_files=[f.name])
        try:
            print("\n * Launching compose...")

            compose_version = docker_client.compose.up(
                detach=True,
            )
            print(" * Waiting for container to be ready...", end="")
            max_retries = 30
            retries = 0
            while retries < max_retries:
                containers = docker_client.compose.ps()

                if all(
                    c.state.health and c.state.health.status == "healthy"
                    for c in containers
                ):
                    print(f"\n{docker_client.compose.logs()}\n")
                    print(f"\n * {compose_version} launched.")
                    break

                exited = [c.id for c in containers if c.state.status == "exited"]
                if any(exited):
                    logs = f"\n{docker_client.compose.logs()}\n"
                    pytest.fail(
                        f"Containers {', '.join(exited)} exited before being ready.\nFull logs: {logs}"
                    )

                print(".", end="")
                time.sleep(1)
                retries += 1
            else:
                not_started = [
                    c.id
                    for c in containers
                    if not c.state.health or c.state.health.status != "healthy"
                ]
                logs = f"\n{docker_client.compose.logs()}\n"
                pytest.fail(
                    f"Containers {', '.join(not_started)} did not start in time. Full logs: {logs}"
                )

            yield docker_client
            # After tests finish, copy coverage data from container(s) to local machine
            if coverage_file:
                coverage_dir = Path(coverage_file).parent
                integration_cov_dir = coverage_dir / "coverage_integration_remote"
                integration_cov_dir.mkdir(exist_ok=True)
                integration_cov_dir = Path(
                    tempfile.mkdtemp(prefix="pytest_run_", dir=integration_cov_dir)
                )
                print(" * Copying coverage data back...")
                coverage_container_paths = []
                for c in containers:
                    if c.name in ("mongo_container",):
                        continue
                    print(f"GETTING BACK FROM container {c.name}")
                    coverage_container_dir = integration_cov_dir / c.name
                    coverage_container_dir.mkdir(exist_ok=True)
                    coverage_container_paths.append(coverage_container_dir)
                    flist = c.execute(
                        ["ls", "-a", "/home/jobflow/coverage/"]
                    ).splitlines()
                    for file in flist:
                        if file.startswith(".coverage"):
                            print("DBG coverage file:", file)
                            c.copy_from(
                                f"/home/jobflow/coverage/{file}",
                                coverage_container_dir / file,
                            )
                    with cd(coverage_container_dir):
                        print("IN coverage_container_dir :", coverage_container_dir)
                        print("BEFORE combine:")
                        print(os.listdir(coverage_container_dir))
                        print(".")
                        cov = Coverage()
                        cov.combine()
                        cov.save()
                        print("AFTER combine and save:")
                        print(os.listdir(coverage_container_dir))
                        print(".")
                with cd(integration_cov_dir):
                    # cov = Coverage()
                    # data_paths = [
                    #     p.relative_to(integration_cov_dir) / ".coverage"
                    #     for p in coverage_container_paths
                    # ]
                    # print("DATA PATHS:")
                    # print(data_paths)
                    #
                    # data_paths = [str(p) for p in data_paths if p.exists()]
                    # cov.combine(data_paths=data_paths, keep=True)
                    # cov.save()
                    # print("INTEGRATION COV DIR AFTER combine and save")
                    # print(os.listdir(integration_cov_dir))
                    # for directory in os.listdir(integration_cov_dir):
                    #     print(f"List of files in directory {directory}")
                    #     if os.path.isdir(directory):
                    #         print(os.listdir(integration_cov_dir / directory))
                    #     else:
                    #         print("... not a directory!!")
                    # shutil.move(
                    #     ".coverage", coverage_dir / ".coverage-integration-remote"
                    # )
                    for directory in os.listdir(integration_cov_dir):
                        print(f"List of files in directory {directory}")
                        if os.path.isdir(directory):
                            print(os.listdir(integration_cov_dir / directory))
                        else:
                            print("... not a directory!!")
                    for cov_container_path in coverage_container_paths:
                        cov_dir = cov_container_path.relative_to(integration_cov_dir)
                        cov_file = cov_dir / ".coverage"
                        shutil.copy(cov_file, f".coverage.{cov_dir}")

                    cov = Coverage()
                    # data_paths = [
                    #     p.relative_to(integration_cov_dir) / ".coverage"
                    #     for p in coverage_container_paths
                    # ]
                    # print("DATA PATHS:")
                    # print(data_paths)
                    #
                    # data_paths = [str(p) for p in data_paths if p.exists()]
                    cov.combine()
                    cov.save()
                    # print("INTEGRATION COV DIR AFTER combine and save")
                    # print(os.listdir(integration_cov_dir))
                    # for directory in os.listdir(integration_cov_dir):
                    #     print(f"List of files in directory {directory}")
                    #     if os.path.isdir(directory):
                    #         print(os.listdir(integration_cov_dir / directory))
                    #     else:
                    #         print("... not a directory!!")
                    shutil.move(
                        ".coverage", coverage_dir / ".coverage-integration-remote"
                    )
        finally:
            try:
                print("\n * Stopping containers...")
                try:
                    docker_client.compose.stop()
                except Exception:
                    pass

                try:
                    docker_client.compose.kill()
                except Exception:
                    pass

                try:
                    docker_client.compose.rm(volumes=True)
                except Exception:
                    pass

                print(" * Done!")
            except Exception as exc:
                print(f" x Failed to stop container: {exc}")


@pytest.fixture(scope="session")
def store_database_name():
    return _get_random_name()


@pytest.fixture(scope="session", autouse=True)
def write_tmp_settings(
    random_project_name,
    store_database_name,
    slurm_ssh_port,
    sge_ssh_port,
    pbs_ssh_port,
    db_port,
    tmp_proj_work_dirs,
):
    """Collects the various sub-configs and writes them to a temporary file in a
    temporary directory."""
    tmp_proj_dir, workdir = tmp_proj_work_dirs

    os.environ["JFREMOTE_PROJECT"] = random_project_name
    # Set config file to a random path so that we don't accidentally load the default
    os.environ["JFREMOTE_CONFIG_FILE"] = _get_random_name(length=10) + ".json"
    # This import must come after setting the env vars as jobflow loads the default
    # config on import
    from jobflow_remote.config import Project

    prerun = (
        "source /home/jobflow/.venv/bin/activate; "
        "export COVERAGE_PROCESS_START=/home/jobflow/.coveragerc; "
        "export COVERAGE_FILE=/home/jobflow/coverage/.coverage"
    )
    project = Project(
        name=random_project_name,
        jobstore={
            "docs_store": {
                "type": "MongoStore",
                "database": store_database_name,
                "host": "localhost",
                "port": db_port,
                "collection_name": "docs",
            },
            "additional_stores": {
                "big_data": {
                    "type": "GridFSStore",
                    "database": store_database_name,
                    "host": "localhost",
                    "port": db_port,
                    "collection_name": "data",
                },
            },
        },
        queue={
            "store": {
                "type": "MongoStore",
                "database": store_database_name,
                "host": "localhost",
                "port": db_port,
                "collection_name": "jobs",
            },
            "flows_collection": "flows",
        },
        log_level="debug",
        workers={
            "test_local_worker": dict(
                type="local",
                scheduler_type="shell",
                work_dir=str(workdir),
                resources={},
            ),
            "test_sanitize_local_worker": dict(
                type="local",
                scheduler_type="shell",
                work_dir=str(workdir),
                resources={},
                sanitize_command=True,
            ),
            "test_remote_slurm_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
            ),
            "test_remote_sge_worker": dict(
                type="remote",
                host="localhost",
                port=sge_ssh_port,
                scheduler_type="sge",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                scheduler_username="jobflow",
                pre_run=prerun,
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
            ),
            "test_remote_pbs_worker": dict(
                type="remote",
                host="localhost",
                port=pbs_ssh_port,
                scheduler_type="pbs",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
                resources={"walltime": "00:05:00", "select": "nodes=1:ppn=1"},
            ),
            "test_remote_limited_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
                max_jobs=1,
            ),
            "test_batch_remote_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
                batch={
                    "jobs_handle_dir": "/home/jobflow/jfr/batch_handle",
                    "work_dir": "/home/jobflow/jfr/batch_work",
                    "max_wait": 10,
                },
                max_jobs=1,
            ),
            "test_batch_multi_remote_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
                batch={
                    "jobs_handle_dir": "/home/jobflow/jfr/batch_multi_handle",
                    "work_dir": "/home/jobflow/jfr/batch_multi_work",
                    "max_wait": 10,
                    "parallel_jobs": 2,
                },
                max_jobs=1,
            ),
            "test_max_jobs_worker": dict(
                type="local",
                scheduler_type="shell",
                work_dir=str(workdir),
                resources={},
                max_jobs=2,
            ),
            "test_sanitize_remote_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run=prerun,
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
                sanitize_command=True,
            ),
        },
        exec_config={
            "test": {"export": {"TESTING_ENV_VAR": random_project_name}},
            "some_pre_run": {
                "pre_run": "echo 'This is a pre_run'; echo 'This is a pre_run' 1>&2"
            },
            "long_pre_run": {"pre_run": f"echo {'X'*4000}; echo {'X'*4000} 1>&2"},
        },
        runner=dict(
            delay_checkout=1,
            delay_check_run_status=1,
            delay_advance_status=1,
            max_step_attempts=3,
            delta_retry=(1, 1, 1),
        ),
    )
    project_json = project.model_dump_json(indent=2)
    with open(tmp_proj_dir / f"{random_project_name}.json", "w") as f:
        f.write(project_json)

    # In some cases it seems that the SETTINGS have already been imported
    # and thus not taking the new configurations into account.
    # Regenerate the JobflowRemoteSettings after setting paths and project
    import jobflow_remote
    from jobflow_remote.config.settings import JobflowRemoteSettings

    jobflow_remote.SETTINGS = JobflowRemoteSettings()

    yield project

    if tmp_proj_dir.exists():
        shutil.rmtree(tmp_proj_dir)
