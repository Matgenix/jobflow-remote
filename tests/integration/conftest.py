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
from monty.os import cd
from python_on_whales import DockerClient
from python_on_whales import docker as docker_pow

# Note that the workers are identified based on the scheduler_type, assuming that
# "shell" workers are executed locally and not in any container. If this changes
# the options will need to be modified. The fixtures will need to be updated accordingly.
WORKER_TYPES = [
    "shell",
    "slurm",
    "sge",
    "pbs",
]


def pytest_addoption(parser):
    parser.addoption(
        "--worker-types",
        "--wt",
        dest="worker_types",
        nargs="+",
        help="List of workers to be used for the integration tests. All available if not specified.",
        default=None,
        action="store",
        choices=WORKER_TYPES,
    )


@pytest.fixture(scope="session")
def workers_list(
    slurm_ssh_port, sge_ssh_port, pbs_ssh_port, tmp_proj_work_dirs, worker_types
):
    """
    Dictionary workers to be set up based on the types of workers selected.
    """
    tmp_proj_dir, workdir = tmp_proj_work_dirs

    prerun = (
        "source /home/jobflow/.venv/bin/activate; "
        "export COVERAGE_PROCESS_START=/home/jobflow/pyproject.toml; "
        "export COVERAGE_FILE=/home/jobflow/coverage/.coverage"
    )
    workers = {
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
                "max_wait": 5,
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
    }
    # remove the workers that do not belong to the selected type
    return {wn: w for wn, w in workers.items() if w["scheduler_type"] in worker_types}


@pytest.fixture(scope="session")
def worker_types(request):
    """
    List of worker types activated during the integration tests
    """
    wt = request.config.getoption("--worker-types")
    if wt is None:
        wt = list(WORKER_TYPES)
    return wt


@pytest.fixture(scope="session")
def integration_workers(workers_list):
    """
    List of worker names activated during the integration tests
    """
    return list(workers_list)


@pytest.fixture(autouse=True)
def check_worker_requirements(request, integration_workers):
    """
    Automatically check worker requirements and skip if needed.

    Will skip, depending on the selected worker types:
      * if there is a parametrization with a parameter named "worker" and
        the value is not among the selected worker. Only the corresponding
        parameter value will be skipped
      * if the test is marked with a list of worker names that are used inside
        the test (e.g. @pytest.mark.workers(["test_max_jobs_worker"])). If not
        all the workers are among the selected ones the test will be skipped.

    Tests with no "worker" parametrization or mark will be executed.
    """
    # Get the workers marker from the test
    workers_marker = request.node.get_closest_marker("workers")

    if workers_marker is not None:
        required_workers = workers_marker.args[0] if workers_marker.args else []

        # Check if any required worker is not in selected workers
        missing_workers = [w for w in required_workers if w not in integration_workers]
        if missing_workers:
            pytest.skip(
                f"Test requires workers {required_workers}, but {missing_workers} not in selected workers {integration_workers}"
            )

    # Handle parametrized workers
    if hasattr(request.node, "callspec") and "worker" in request.node.callspec.params:
        worker_param = request.node.callspec.params["worker"]
        if worker_param not in integration_workers:
            pytest.skip(
                f"Worker '{worker_param}' not in selected workers {integration_workers}"
            )


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
def bake_containers(worker_types):
    # targets here should be a list containing "slurm", "sge", "pbs"
    targets = [w for w in worker_types if w != "shell"]
    # don't bake anything if only "shell" is needed.
    if not targets:
        return
    hcl_path = Path(__file__).parent.resolve() / "dockerfiles/docker-bake.hcl"
    docker_pow.buildx.bake(
        targets=targets,
        files=hcl_path,
        set={"*.context": str(Path(__file__).parent.parent.parent.resolve())},
    )


@pytest.fixture(scope="session", autouse=True)
def compose_containers(
    slurm_ssh_port,
    sge_ssh_port,
    pbs_ssh_port,
    db_port,
    bake_containers,
    coverage_file,
    pytestconfig,
    worker_types,
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

"""
    if "slurm" in worker_types:
        compose_yaml += f"""
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

"""

    if "sge" in worker_types:
        compose_yaml += f"""
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

"""

    if "pbs" in worker_types:
        compose_yaml += f"""
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
            if pytestconfig.getoption("copy_files_from_containers"):
                print(" * Copying files back from the containers...")
                containers_files_dir = pytestconfig.rootpath / "containers_files"
                for c in containers:
                    if c.name in ("mongo_container",):
                        continue
                    container_dir = containers_files_dir / c.name
                    container_dir.mkdir(parents=True, exist_ok=True)
                    c.copy_from(
                        "/home/jobflow/jfr/",
                        container_dir,
                    )
            # After tests finish, copy coverage data from container(s) to local machine
            if coverage_file:
                from coverage import Coverage

                coverage_dir = Path(coverage_file).parent
                integration_cov_dir = coverage_dir / "coverage_integration_remote"
                integration_cov_dir.mkdir(exist_ok=True)
                integration_cov_dir = Path(
                    tempfile.mkdtemp(prefix="pytest_run_", dir=integration_cov_dir)
                )
                print(" * Copying coverage data back...")
                coverage_container_paths = []
                # This needs to be set as it may have been set elsewhere
                os.environ["COVERAGE_FILE"] = ".coverage"
                for c in containers:
                    if c.name in ("mongo_container",):
                        continue
                    coverage_container_dir = integration_cov_dir / c.name
                    coverage_container_dir.mkdir(exist_ok=True)
                    coverage_container_paths.append(coverage_container_dir)
                    flist = c.execute(
                        ["ls", "-a", "/home/jobflow/coverage/"]
                    ).splitlines()
                    for file in flist:
                        if file.startswith(".coverage"):
                            c.copy_from(
                                f"/home/jobflow/coverage/{file}",
                                coverage_container_dir / file,
                            )
                    with cd(coverage_container_dir):
                        # Combining coverage from the different jf execution runs for all the tests on this container
                        cov = Coverage()
                        cov.combine()
                        cov.save()
                with cd(integration_cov_dir):
                    for cov_container_path in coverage_container_paths:
                        cov_dir = cov_container_path.relative_to(integration_cov_dir)
                        cov_file = cov_dir / ".coverage"
                        if cov_file.exists():
                            shutil.copy(cov_file, f".coverage.{cov_dir}")

                    # Combining the coverage from each container
                    cov = Coverage()
                    cov.combine()
                    cov.save()

                    shutil.move(
                        ".coverage", coverage_dir / ".coverage-integration-remote"
                    )
        finally:
            if pytestconfig.getoption("keep_containers_alive"):
                print("\n * Keeping containers alive...")
                print(f"\n  - Docker compose yaml file: {f.name}")
                print("\n  - Docker containers:")
                containers = docker_client.compose.ps()
                for c in containers:
                    print(f"\n    - {c.name}")
                    inspect = docker_client.container.inspect(c.name)
                    ports = inspect.network_settings.ports or {}
                    ssh_bindings = ports.get("22/tcp", [])
                    seen_ports = set()
                    for binding in ssh_bindings:
                        host_ip = binding.get("HostIp") or "localhost"
                        host_port = binding.get("HostPort")

                        # Skip IPv6 all-addresses
                        if host_ip == "::":
                            continue

                        # Normalize IPv4 all-addresses to localhost
                        if host_ip == "0.0.0.0":  # noqa: S104
                            host_ip = "localhost"

                        # Deduplicate multiple bindings with same port
                        if host_port in seen_ports:
                            continue
                        seen_ports.add(host_port)
                        print(f"      ssh jobflow@{host_ip} -p {host_port}")
            else:
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
    pytestconfig,
    workers_list,
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
        workers=workers_list,
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

    return project


@pytest.fixture()
def clean_slurm_queue(write_tmp_settings, coverage_file, worker_types):
    """
    Clean the list of Jobs in the SLURM queue at the end of the test.
    """
    from jobflow_remote.remote.queue import QueueManager

    yield

    # Skip if slurm has been deselected. In principle, this fixture should never be used in that case.
    if "slurm" not in worker_types:
        return

    project = write_tmp_settings
    worker = project.workers["test_remote_slurm_worker"]
    queue_manager = QueueManager(worker.get_scheduler_io(), worker.get_host())
    # If tests are run with coverage, first try to wait until the slurm job finishes smoothly
    if coverage_file:
        for _ in range(30):
            time.sleep(1.0)
            if not queue_manager.get_jobs_list():
                break
    for qjob in queue_manager.get_jobs_list():
        queue_manager.cancel(qjob)
        time.sleep(0.1)
