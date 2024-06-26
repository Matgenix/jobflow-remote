import os
import random
import shutil
import tempfile
import warnings
from pathlib import Path

import pytest


def _get_random_name(length=6):
    return "".join(random.choice("abcdef") for _ in range(length))


@pytest.fixture(scope="session")
def store_database_name() -> str:
    return "jfremote_db_tests__"


@pytest.fixture(scope="session")
def mongoclient():
    """
    Generate a MongoClient for a local database.
    If a local DB is already available is that one (should be the one used in
    the CI or by developers with an accessible local DB). Otherwise, generate
    one with pymongo_inmemory, that should be installed.
    """
    import pymongo

    mc = pymongo.MongoClient(host="localhost", port=27017)
    # try connecting to the DB with a short delay, since the DB is local it
    # should not take long to reply
    try:
        with pymongo.timeout(1):
            mc.server_info()
        yield mc
    except Exception as e:
        warnings.warn(
            f"Could not connect to a local DB {getattr(e, 'message', str(e))}. Trying "
            "with pymongo_inmemory",
            stacklevel=2,
        )

        try:
            import pymongo_inmemory
        except ImportError as exc:
            raise pytest.skip(
                "No local DB and pymongo_inmemory. Either start a local mongodb or "
                "install pymongo_inmemory"
            ) from exc

        mc = pymongo_inmemory.MongoClient()
        assert mc.server_info()

        yield mc
        # stop the db started by pymongo_inmemory
        mc.close()


@pytest.fixture(scope="session")
def mongo_jobstore(store_database_name):
    from jobflow import JobStore
    from maggma.stores import MongoStore

    store = JobStore(MongoStore(store_database_name, "outputs"))
    store.connect()
    return store


@pytest.fixture(scope="session", autouse=True)
def write_tmp_settings(
    random_project_name,
    store_database_name,
    mongoclient,
):
    """Collects the various sub-configs and writes them to a temporary file in a
    temporary directory."""
    tmp_dir: Path = Path(tempfile.mkdtemp())

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
                "host": mongoclient.HOST,
                "port": mongoclient.PORT,
                "collection_name": "docs",
            },
            "additional_stores": {
                "big_data": {
                    "type": "GridFSStore",
                    "database": store_database_name,
                    "host": mongoclient.HOST,
                    "port": mongoclient.PORT,
                    "collection_name": "data",
                },
            },
        },
        queue={
            "store": {
                "type": "MongoStore",
                "database": store_database_name,
                "host": mongoclient.HOST,
                "port": mongoclient.PORT,
                "collection_name": "jobs",
            },
        },
        log_level="debug",
        workers={
            "test_local_worker": dict(
                type="local",
                scheduler_type="shell",
                work_dir=str(workdir),
                resources={},
            ),
            "test_local_worker_2": dict(
                type="local",
                scheduler_type="shell",
                work_dir=str(workdir),
                resources={},
            ),
        },
        exec_config={"test": {"export": {"TESTING_ENV_VAR": random_project_name}}},
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

    # In some cases it seems that the SETTINGS have already been imported
    # and thus not taking the new configurations into account.
    # Regenerate the JobflowRemoteSettings after setting paths and project
    import jobflow_remote
    from jobflow_remote.config.settings import JobflowRemoteSettings

    jobflow_remote.SETTINGS = JobflowRemoteSettings()

    yield
    shutil.rmtree(tmp_dir)


@pytest.fixture()
def job_controller(random_project_name):
    """Yields a jobcontroller instance for the test suite that also sets up the
    jobstore, resetting it after every test.
    """
    from jobflow_remote.jobs.jobcontroller import JobController

    jc = JobController.from_project_name(random_project_name)
    assert jc.reset()
    return jc


@pytest.fixture()
def one_job(random_project_name):
    """Add one flow with one job to the DB."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    return flow


@pytest.fixture()
def two_flows_four_jobs(random_project_name):
    """Add two flows with two jobs each to the DB"""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_first.name = "add1"
    add_second = add(add_first.output, 5)
    add_second.name = "add2"

    add_first.update_metadata({"test_meta": 1})

    flow = Flow([add_first, add_second])
    flow.name = "f1"
    submit_flow(flow, worker="test_local_worker")

    add_third = add(1, 5)
    add_third.name = "add3"
    add_fourth = add(add_third.output, 5)
    add_fourth.name = "add4"

    flow2 = Flow([add_third, add_fourth])
    flow2.name = "f2"
    submit_flow(flow2, worker="test_local_worker")

    return [flow, flow2]
