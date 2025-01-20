from __future__ import annotations

import os
import random
import shutil
import socket
import tempfile
import time
from functools import partialmethod
from pathlib import Path

import docker
import fabric
import pytest
from docker.models.containers import Container


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
def db_port():
    """The exposed local port for connections to the MongoDB stores."""
    return _get_free_port()


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


def build_and_launch_container(
    docker_client: docker.client.DockerClient,
    dockerfile: Path | None = None,
    image_name: str | None = None,
    ports: dict[str, int] | None = None,
    buildargs: dict[str, str] | None = None,
):
    """Builds and/or launches a container, returning the container object.

    Parameters
    ----------
        docker_client: The local docker client.
        dockerfile: An optional location of a dockerfile to build.
        image_name: Either the tag to attach to the built image, or an image
            name to pull from the web (may require authenticated docker client).
        ports: A port specification to use for the launched container.

    Yields
    ------
        The launched container object, then stops the container after use.

    """
    if dockerfile is not None:
        print(f" * Building {image_name}")
        _, logs = docker_client.images.build(
            path=str(Path(__file__).parent.parent.parent.resolve()),
            dockerfile=dockerfile,
            buildargs=buildargs,
            tag=image_name,
            rm=True,
            quiet=False,
        )

        for step in logs:
            if step.get("stream"):
                print(step["stream"], end="")

    try:
        print(f"\n * Launching container for {image_name}...")
        container = docker_client.containers.run(
            image_name,
            detach=True,
            remove=False,
            auto_remove=False,
            tty=True,
            ports=ports,
        )
        assert isinstance(container, Container)
        print(" * Waiting for container to be ready...", end="")
        max_retries = 30
        while (retries := 0) < max_retries:
            if container.status == "running":
                print(f"\n{container.logs().decode()}\n")
                print(f"\n * Container {container.id} launched.")
                break
            if container.status == "exited":
                logs = f"\n{container.logs().decode()}\n"
                pytest.fail(
                    f"Container {container.name!r} ({container.image}) exited before being ready.\nFull logs: {logs}"
                )
            print(".", end="")
            time.sleep(1)
            retries += 1
            container.reload()
        else:
            logs = f"\n{container.logs().decode()}\n"
            pytest.fail(
                f"Container {container.name!r} ({container.image}) did not start in time. Full\nlogs: {logs}"
            )

        yield container
    finally:
        try:
            print(f"\n * Stopping container {container.id}...")
            try:
                container.stop()
            except (docker.errors.APIError, docker.errors.NotFound):
                pass
            try:
                container.kill()
            except (docker.errors.APIError, docker.errors.NotFound):
                pass
            try:
                container.remove()
            except (docker.errors.APIError, docker.errors.NotFound):
                pass
            print(" * Done!")
        except Exception as exc:
            print(f" x Failed to stop container: {exc}")


@pytest.fixture(scope="session", autouse=True)
def slurm_container(docker_client, slurm_ssh_port):
    """Build and launch a container running Slurm and SSH, exposed on a random available
    port."""
    ports = {"22/tcp": slurm_ssh_port}
    yield from build_and_launch_container(
        docker_client,
        Path("./tests/integration/dockerfiles/Dockerfile"),
        "jobflow-remote-slurm:latest",
        ports=ports,
        buildargs={"QUEUE_SYSTEM": "slurm"},
    )


@pytest.fixture(scope="session", autouse=True)
def sge_container(docker_client, sge_ssh_port):
    """Build and launch a container running SGE and SSH, exposed on a random available
    port."""
    ports = {"22/tcp": sge_ssh_port}
    yield from build_and_launch_container(
        docker_client,
        Path("./tests/integration/dockerfiles/Dockerfile"),
        "jobflow-remote-sge:latest",
        ports=ports,
        buildargs={"QUEUE_SYSTEM": "sge"},
    )


@pytest.fixture(scope="session", autouse=True)
def mongo_container(docker_client, db_port):
    """Build and launch a container running MongoDB, exposed on a random available
    port."""
    ports = {"27017/tcp": db_port}
    yield from build_and_launch_container(
        docker_client,
        dockerfile=None,
        image_name="mongo:7",
        ports=ports,
    )


@pytest.fixture(scope="session")
def store_database_name():
    return _get_random_name()


@pytest.fixture(scope="session", autouse=True)
def write_tmp_settings(
    random_project_name,
    store_database_name,
    slurm_ssh_port,
    sge_ssh_port,
    db_port,
):
    """Collects the various sub-configs and writes them to a temporary file in a
    temporary directory."""
    tmp_dir: Path = Path(tempfile.mkdtemp())

    original_jf_remote_projects_folder = os.environ.get("JFREMOTE_PROJECTS_FOLDER")
    original_jf_remote_project = os.environ.get("JFREMOTE_PROJECT")
    original_config_file = os.environ.get("JFREMOTE_CONFIG_FILE")

    os.environ["JFREMOTE_PROJECTS_FOLDER"] = str(tmp_dir.resolve())
    workdir = tmp_dir / "jfr"
    workdir.mkdir(exist_ok=True)
    os.environ["JFREMOTE_PROJECT"] = random_project_name
    # Set config file to a random path so that we don't accidentally load the default
    os.environ["JFREMOTE_CONFIG_FILE"] = _get_random_name(length=10) + ".json"
    # This import must come after setting the env vars as jobflow loads the default
    # config on import
    from jobflow_remote.config import Project

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
                pre_run="source /home/jobflow/.venv/bin/activate",
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
                pre_run="source /home/jobflow/.venv/bin/activate",
                connect_kwargs={"allow_agent": False, "look_for_keys": False},
            ),
            "test_remote_limited_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                pre_run="source /home/jobflow/.venv/bin/activate",
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
                pre_run="source /home/jobflow/.venv/bin/activate",
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
                pre_run="source /home/jobflow/.venv/bin/activate",
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
                pre_run="source /home/jobflow/.venv/bin/activate",
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
    with open(tmp_dir / f"{random_project_name}.json", "w") as f:
        f.write(project_json)

    yield project

    shutil.rmtree(tmp_dir)
    # Reset environment variables if they were set elsewhere
    if original_jf_remote_projects_folder is not None:
        os.environ["JFREMOTE_PROJECTS_FOLDER"] = original_jf_remote_projects_folder
    if original_jf_remote_project is not None:
        os.environ["JFREMOTE_PROJECT"] = original_jf_remote_project
    if original_config_file is not None:
        os.environ["JFREMOTE_CONFIG_FILE"] = original_config_file


@pytest.fixture()
def job_controller(random_project_name):
    """Yields a jobcontroller instance for the test suite that also sets up the
    jobstore, resetting it after every test.
    """
    from jobflow_remote.jobs.jobcontroller import JobController

    jc = JobController.from_project_name(random_project_name)
    assert jc.reset(max_limit=0)
    return jc
