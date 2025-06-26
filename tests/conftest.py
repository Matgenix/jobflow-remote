import logging
import logging.config
import random
import time
import warnings
from functools import partial
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def tests_dir():
    return Path(__file__).resolve().parent.resolve()


@pytest.fixture(scope="session")
def test_data_dir(tests_dir):
    return tests_dir / "test_data"


@pytest.fixture(scope="session")
def log_to_stdout() -> None:
    import logging
    import sys

    # Set Logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)


@pytest.fixture(scope="session")
def clean_dir(debug_mode):
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield
    if debug_mode:
        print(f"Tests ran in {newpath}")
    else:
        os.chdir(old_cwd)
        shutil.rmtree(newpath)


@pytest.fixture()
def tmp_dir():
    """Same as clean_dir but is fresh for every test."""
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    new_path = tempfile.mkdtemp()
    os.chdir(new_path)
    yield
    os.chdir(old_cwd)
    shutil.rmtree(new_path)


@pytest.fixture(scope="session")
def debug_mode() -> bool:
    return False


def _get_random_name(length=6):
    return "".join(random.choice("abcdef") for _ in range(length))


@pytest.fixture(scope="session")
def random_project_name():
    return _get_random_name()


@pytest.fixture()
def daemon_manager(random_project_name, job_controller):
    from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus
    from jobflow_remote.utils.db import MissingDocumentError

    dm = DaemonManager.from_project_name(random_project_name)
    yield dm
    # make sure that the following actions on the daemon can be performed
    # by cleaning the document in the DB. Since the running_runner document
    # has been added at a later stage handle the cases where the document is
    # not present. It should be added for kill and shut_down to work.
    try:
        job_controller.clean_running_runner(break_lock=True)
    except MissingDocumentError:
        job_controller.auxiliary.insert_one({"running_runner": None})
    # kill processes and shut down daemon (otherwise will remain in the STOPPED state)
    dm.kill(raise_on_error=True)
    time.sleep(0.5)
    dm.shut_down(raise_on_error=True)
    for _ in range(10):
        time.sleep(1)
        try:
            if dm.check_status() == DaemonStatus.SHUT_DOWN:
                break
        except DaemonError:
            pass
    else:
        warnings.warn(
            "daemon manager did not shut down within the expected time", stacklevel=2
        )


@pytest.fixture()
def runner():
    from jobflow_remote.jobs.runner import Runner

    runner = Runner()
    yield runner
    runner.cleanup()


@pytest.fixture(autouse=True)
def reset_logging_config():
    """
    CLI tests run initialize_cli_logger that changes the log handlers and
    prevents the caplog fixture from working correctly. This removes
    the additional handler and restores previous handlers.

    Applied to all tests for safety.
    """
    from rich.logging import RichHandler

    # Store initial propagate state for all loggers
    initial_states = {
        name: logging.getLogger(name).propagate
        for name in logging.root.manager.loggerDict
    }

    yield

    # Remove any RichHandlers and restore propagate flags
    for name, was_propagating in initial_states.items():
        logger = logging.getLogger(name)
        logger.handlers = [h for h in logger.handlers if not isinstance(h, RichHandler)]
        logger.propagate = was_propagating
        logger.disabled = False  # Re-enable any disabled loggers


@pytest.fixture(scope="session")
def upgrade_test_dir(test_data_dir):
    """
    Path to the test data directory used for upgrade tests.

    Returns:
        Path: Path to the test data directory used for upgrade tests.
    """

    return test_data_dir / "upgrade"


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Hook to set the report of the test. Needed to check if a test failed.
    """
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        item.rep_call = report


@pytest.fixture(scope="session")
def shared_test_out_dir(tmp_path_factory):
    """
    Fixture to lazily create a shared temporary directory to store the dump
    of MongoDB for failed tests and move them to store as an artifact if
    running on github.
    """
    import os

    _tmp_dir = None

    def get_or_create():
        nonlocal _tmp_dir
        if _tmp_dir is None:
            _tmp_dir = Path(tmp_path_factory.mktemp("shared_test_dir"))
        return _tmp_dir

    yield get_or_create

    # If on github and the folder was created (i.e., some test failed), move
    # it so that it can be uploaded as an artifact.
    if os.getenv("GITHUB_WORKSPACE") and _tmp_dir:
        artifact_path = Path(os.getenv("GITHUB_WORKSPACE")) / "test_folder"
        os.rename(_tmp_dir, artifact_path)

        print(f"Database dump saved as artifact: {artifact_path}")


@pytest.fixture()
def job_controller(random_project_name, request, shared_test_out_dir, tests_dir):
    """Yields a jobcontroller instance for the test suite that also sets up the
    jobstore, resetting it after every test.
    """
    from monty.serialization import dumpfn

    from jobflow_remote.jobs.jobcontroller import JobController

    jc = JobController.from_project_name(random_project_name)
    assert jc.reset(max_limit=0)
    yield jc

    if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
        target_dir = shared_test_out_dir()

        # use the test directory tree + test name (including parameters) as a target folder
        test_filepath = request.node.path
        testpath_relative_to_tests_dir = test_filepath.relative_to(tests_dir)
        test_name = request.node.originalname

        test_dump_dir = target_dir / testpath_relative_to_tests_dir / test_name
        params = None
        if hasattr(request.node, "callspec"):
            params_id = request.node.callspec.id
            sanitized_params_id = "".join(c if c.isalnum() else "_" for c in params_id)
            test_dump_dir = test_dump_dir / sanitized_params_id
            params = request.node.callspec.params
        test_dump_dir.mkdir(parents=True, exist_ok=True)
        test_info = {"name": request.node.name, "params": params}
        dumpfn(test_info, test_dump_dir / "test_info.json", indent=2)

        # don't use the backup to create indented json files for easier access

        for coll in [jc.jobs, jc.flows, jc.auxiliary]:
            dump_file_path = test_dump_dir / f"{coll.name}.json"
            dumpfn(list(coll.find()), dump_file_path, indent=2)

        print(f"Data dumped to {test_dump_dir}")


@pytest.fixture()
def job_controller_drop(random_project_name, job_controller):
    """Yields a jobcontroller instance for the test suite that also sets up the
    jobstore. Drops the database at the end of the test.
    Useful for tests that may leave entries in the DB that are not cleaned with
    a reset.
    """
    try:
        yield job_controller
    finally:
        job_controller.db.client.drop_database(job_controller.db)


def wait_daemon_status(
    daemon_manager, target_status, acceptable_states=None, max_wait: int = 10
) -> bool:
    from jobflow_remote.jobs.daemon import DaemonError

    if not acceptable_states:
        acceptable_states = [target_status]

    state = None
    for _i in range(max_wait):
        time.sleep(1)
        # if the state cannot be determined keep waiting
        try:
            state = daemon_manager.check_status()
        except DaemonError:
            continue
        assert state in acceptable_states
        if state == target_status:
            return True
    raise RuntimeError(
        f"The daemon did not reach {target_status.value} within the expected time ({max_wait}). Last state: {state}"
    )


@pytest.fixture(scope="session")
def wait_daemon_started():
    from jobflow_remote.jobs.daemon import DaemonStatus

    return partial(
        wait_daemon_status,
        target_status=DaemonStatus.RUNNING,
        acceptable_states=[DaemonStatus.STARTING, DaemonStatus.RUNNING],
    )


@pytest.fixture(scope="session")
def wait_daemon_stopped():
    from jobflow_remote.jobs.daemon import DaemonStatus

    return partial(
        wait_daemon_status,
        target_status=DaemonStatus.STOPPED,
        acceptable_states=[
            DaemonStatus.STOPPING,
            DaemonStatus.STOPPED,
            DaemonStatus.RUNNING,
            DaemonStatus.PARTIALLY_RUNNING,
        ],
    )


@pytest.fixture(scope="session")
def wait_daemon_shutdown():
    from jobflow_remote.jobs.daemon import DaemonStatus

    acceptable_states = [
        DaemonStatus.STOPPING,
        DaemonStatus.STOPPED,
        DaemonStatus.RUNNING,
        DaemonStatus.PARTIALLY_RUNNING,
        DaemonStatus.SHUT_DOWN,
    ]

    return partial(
        wait_daemon_status,
        target_status=DaemonStatus.SHUT_DOWN,
        acceptable_states=acceptable_states,
    )
