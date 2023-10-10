import os
import random
import shutil
import socket
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

import docker
import pytest


def _get_free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def _get_random_name(length=6):
    return "".join(random.choice("abcdef") for _ in range(length))


@pytest.fixture(scope="session")
def slurm_ssh_port():
    """The exposed local port for SSH connections to the Slurm container."""
    yield _get_free_port()


@pytest.fixture(scope="session")
def db_port():
    """The exposed local port for connections to the MongoDB stores."""
    yield _get_free_port()


@pytest.fixture(scope="session")
def docker_client():
    return docker.from_env()


def build_and_launch_container(
    docker_client: docker.client.DockerClient,
    dockerfile: Optional[os.PathLike] = None,
    image_name: Optional[str] = None,
    ports: Optional[Dict[str, int]] = None,
):
    """Builds and/or launches a container, returning the container object.

    Parameters:
        docker_client: The local docker client.
        dockerfile: An optional location of a dockerfile to build.
        image_name: Either the tag to attach to the built image, or an image
            name to pull from the web (may require authenticated docker client).
        ports: A port specification to use for the launched container.

    Yields:
        The launched container object, then stops the container after use.

    """

    try:
        if dockerfile is not None:
            print(f"Building {image_name}")
            _, logs = docker_client.images.build(
                path=str(Path(__file__).parent.parent.parent.resolve()),
                dockerfile=dockerfile,
                tag=image_name,
                rm=True,
                quiet=False,
            )

            for step in logs:
                if step.get("stream"):
                    print(step["stream"], end="")

        print(f"Launching container for {image_name}...")
        container = docker_client.containers.run(
            image_name, detach=True, remove=True, tty=True, ports=ports
        )
        print("Waiting for container to be ready", end="")
        while container.status != "running":
            print(".", end="")
            time.sleep(10)
            container.reload()
        print("")
        print(f"Container {container.id} launched.")
        print(f"{container.logs().decode()}")

        yield container
    finally:
        try:
            print(f"Stopping container {container.id}...")
            container.stop()
            print("Done!")
        except:
            pass


@pytest.fixture(scope="session", autouse=True)
def slurm_container(docker_client, slurm_ssh_port):
    """Build and launch a container running Slurm + SSH, exposed on a random available port."""
    ports = {"22/tcp": slurm_ssh_port}
    yield from build_and_launch_container(
        docker_client,
        "./tests/integration/dockerfiles/Dockerfile.slurm",
        "jobflow-slurm:latest",
        ports=ports,
    )


@pytest.fixture(scope="session", autouse=True)
def mongo_container(docker_client, db_port):
    """Build and launch a container running MongoDB, exposed on a random available port."""
    ports = {"27017/tcp": db_port}
    yield from build_and_launch_container(
        docker_client,
        dockerfile=None,
        image_name="mongo:7",
        ports=ports,
    )


@pytest.fixture(scope="session")
def random_project_name():
    return _get_random_name()


@pytest.fixture(scope="session")
def store_database_name():
    return _get_random_name()


@pytest.fixture(scope="session")
def fw_database_name():
    return _get_random_name()


@pytest.fixture(scope="session", autouse=True)
def write_tmp_settings(
    random_project_name,
    store_database_name,
    slurm_ssh_port,
    db_port,
):
    """Collects the various sub-configs and writes them to a temporary file in a temporary directory."""

    tmp_dir: Path = Path(tempfile.mkdtemp())

    os.environ["JFREMOTE_PROJECTS_FOLDER"] = str(tmp_dir.resolve())
    os.environ["JFREMOTE_PROJECT"] = random_project_name
    # Set the config file to a random path so that we don't accidentally load the default
    os.environ["JFREMOTE_CONFIG_FILE"] = _get_random_name(length=10) + ".json"
    # This import must come after setting the env vars as jobflow loads the default config
    # on import
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
            }
        },
        queue={
            "type": "MongoStore",
            "database": store_database_name,
            "host": "localhost",
            "port": db_port,
            "collection_name": "jobs",
        },
        log_level="debug",
        workers={
            "test_worker": dict(
                type="remote",
                host="localhost",
                port=slurm_ssh_port,
                scheduler_type="slurm",
                work_dir="/home/jobflow/jfr",
                user="jobflow",
                password="jobflow",
                resources={"partition": "debug", "ntasks": 1, "time": "00:01:00"},
            )
        },
        runner=dict(
            delay_checkout=1,
            delay_check_run_status=1,
            delay_advance_status=1,
            max_step_attempts=3,
            delta_retry=(1, 1, 1),
        ),
    )
    project_json = project.json(indent=2)
    with open(tmp_dir / f"{random_project_name}.json", "w") as f:
        f.write(project_json)

    yield
    shutil.rmtree(tmp_dir)


@pytest.fixture(scope="session", autouse=True)
def fw_lpad(
    store_database_name,
    db_port,
    mongo_container,
):
    """Set up a FireWorks LaunchPad with a fresh database in the db container."""
    from fireworks import LaunchPad

    lpad = LaunchPad(name=store_database_name, port=db_port)
    lpad.reset("", require_password=False)
    yield lpad


@pytest.fixture(scope="session")
def daemon_manager():
    from jobflow_remote.jobs.daemon import DaemonManager

    yield DaemonManager()


@pytest.fixture(scope="function")
def runner(daemon_manager):
    yield daemon_manager.start()
    daemon_manager.stop()
