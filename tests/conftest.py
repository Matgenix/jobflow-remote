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
def log_to_stdout():
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
    """Same as clean_dir but is fresh for every test"""
    import os
    import shutil
    import tempfile

    old_cwd = os.getcwd()
    newpath = tempfile.mkdtemp()
    os.chdir(newpath)
    yield
    os.chdir(old_cwd)
    shutil.rmtree(newpath)


@pytest.fixture(scope="session")
def debug_mode():
    return False


def _get_random_name(length=6):
    return "".join(random.choice("abcdef") for _ in range(length))


@pytest.fixture(scope="session")
def random_project_name():
    return _get_random_name()


@pytest.fixture(scope="function")
def daemon_manager(random_project_name):
    from jobflow_remote.jobs.daemon import DaemonError, DaemonManager, DaemonStatus

    dm = DaemonManager.from_project_name(random_project_name)
    yield dm
    # kill the processes and shut down the daemon (otherwise will remain in the STOPPED state)
    dm.kill(raise_on_error=True)
    time.sleep(0.5)
    dm.shut_down(raise_on_error=True)
    for i in range(10):
        time.sleep(1)
        try:
            if dm.check_status() == DaemonStatus.SHUT_DOWN:
                break
        except DaemonError:
            pass
    else:
        warnings.warn("daemon manager did not shut down within the expected time")


@pytest.fixture(scope="function")
def runner():
    from jobflow_remote.jobs.runner import Runner

    runner = Runner()
    yield runner
    runner.cleanup()
