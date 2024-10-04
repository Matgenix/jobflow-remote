import logging
import logging.config
import random
import time
import warnings

import pytest


@pytest.fixture(scope="session")
def test_dir():
    from pathlib import Path

    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / "test_data"
    return test_dir.resolve()


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
