from __future__ import annotations

import inspect
import logging
import logging.config
import os
import random
import shutil
import sys
import time
import warnings
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from rich.console import Console

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from pymongo.collection import Collection

    from jobflow_remote.jobs.jobcontroller import JobController


@pytest.fixture()
def patch_cli_consoles(monkeypatch):
    import jobflow_remote.cli

    err_console = Console(force_terminal=True, stderr=True, width=1000)
    out_console = Console(force_terminal=True, width=1000)
    # The out_console and err_console have to be patched everywhere they are imported
    # Doing this only for the cli
    for _mod_name, module in inspect.getmembers(jobflow_remote.cli, inspect.ismodule):
        if hasattr(module, "err_console"):
            monkeypatch.setattr(module, "err_console", err_console)
        if hasattr(module, "out_console"):
            monkeypatch.setattr(module, "out_console", out_console)


@pytest.fixture()
def run_check_cli(patch_cli_consoles):
    from jobflow_remote.testing.cli import run_check_cli

    return run_check_cli


@pytest.fixture(scope="session")
def test_dir():
    module_dir = Path(__file__).resolve().parent
    test_dir = module_dir / "test_data"
    return test_dir.resolve()


@pytest.fixture(scope="session")
def coverage_file(request):
    """Fixture to get the absolute path of the pytest-cov coverage file, ensuring it exists."""
    cov_plugin = request.config.pluginmanager.get_plugin("_cov")
    if cov_plugin:
        cov_controller = getattr(cov_plugin, "cov_controller", None)
        if cov_controller:
            data_file = (
                cov_controller.cov.config.data_file
            )  # Could be relative or absolute
            # Check if data_file is already absolute
            if not os.path.isabs(data_file):
                invocation_dir = (
                    request.config.invocation_dir
                )  # Pytest's invocation dir
                data_file = os.path.join(
                    invocation_dir, data_file
                )  # Convert to absolute path
            return data_file
    return None  # Return None if pytest-cov is inactive or file doesn't exist


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
    yield Path(new_path)
    os.chdir(old_cwd)
    shutil.rmtree(new_path)


@pytest.fixture(scope="session")
def tmp_proj_work_dirs(pytestconfig):
    import tempfile

    tmp_proj_dir: Path = Path(tempfile.mkdtemp())

    original_jf_remote_projects_folder = os.environ.get("JFREMOTE_PROJECTS_FOLDER")
    original_jf_remote_project = os.environ.get("JFREMOTE_PROJECT")
    original_config_file = os.environ.get("JFREMOTE_CONFIG_FILE")

    os.environ["JFREMOTE_PROJECTS_FOLDER"] = str(tmp_proj_dir.resolve())
    workdir = tmp_proj_dir / "jfr"
    workdir.mkdir(exist_ok=True)

    yield tmp_proj_dir, workdir

    # Reset environment variables if they were set elsewhere
    if original_jf_remote_projects_folder is not None:
        os.environ["JFREMOTE_PROJECTS_FOLDER"] = original_jf_remote_projects_folder
    if original_jf_remote_project is not None:
        os.environ["JFREMOTE_PROJECT"] = original_jf_remote_project
    if original_config_file is not None:
        os.environ["JFREMOTE_CONFIG_FILE"] = original_config_file

    if pytestconfig.getoption("keep_containers_alive"):
        print("\n * Containers are kept alive ... also keeping project configuration:")
        print(f"\n  - Directory for project configuration file: {tmp_proj_dir}")
    elif tmp_proj_dir.exists():
        shutil.rmtree(tmp_proj_dir)


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
def runner(patch_project):
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
def upgrade_test_dir(test_dir):
    """
    Path to the test data directory used for upgrade tests.

    Returns:
        Path: Path to the test data directory used for upgrade tests.
    """

    return test_dir / "upgrade"


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
    of MongoDB for failed tests and move them to store as an artifact is
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


# Snapshots of the state of the DB right after a full ``JobController.reset``.
# Used by the ``job_controller`` fixture to restore the DB without rebuilding all
# the indexes, which is by far the most expensive part of the reset.
# The key identifies the database and collections, the value is the snapshot.
_RESET_SNAPSHOTS: dict[tuple, dict[str, Any]] = {}

# Set this environment variable to a non-empty value to force a full reset in the
# ``job_controller`` fixture, i.e. to disable the snapshot based restore.
FULL_RESET_ENV_VAR = "JF_TEST_FULL_RESET"


def _tracked_collections(jc: JobController) -> dict[str, Collection]:
    """
    Collections whose content and indexes are restored by the snapshot.

    Parameters
    ----------
    jc
        The JobController whose collections should be returned.

    Returns
    -------
    dict
        The collections created or modified by ``JobController.reset``, by name.
    """
    colls = {
        "jobs": jc.jobs,
        "flows": jc.flows,
        "auxiliary": jc.auxiliary,
        "batches": jc.batches,
    }
    # also track the indexes of the collections where the outputs are stored, as
    # they are created by the reset as well. Only if the store exposes the collection.
    stores = {"default": jc.jobstore, **jc.optional_jobstores}
    for store_name, jobstore in stores.items():
        coll = getattr(jobstore.docs_store, "_collection", None)
        if coll is not None:
            colls[f"jobstore_{store_name}"] = coll
    return colls


def _snapshot_key(jc: JobController) -> tuple:
    """
    Key identifying the database and the collections handled by a JobController.

    Parameters
    ----------
    jc
        The JobController to identify.

    Returns
    -------
    tuple
        The key to be used in ``_RESET_SNAPSHOTS``.
    """
    return tuple(
        (name, coll.database.name, coll.name)
        for name, coll in _tracked_collections(jc).items()
    )


def _take_snapshot(jc: JobController) -> dict[str, Any]:
    """
    Store the indexes and the documents of the collections just after a reset.

    Parameters
    ----------
    jc
        A JobController that has just been reset.

    Returns
    -------
    dict
        The snapshot, with the index information of all the tracked collections
        and the documents of the collections of the JobController.
    """
    colls = _tracked_collections(jc)
    return {
        "indexes": {name: coll.index_information() for name, coll in colls.items()},
        "docs": {
            name: list(coll.find())
            for name, coll in colls.items()
            if not name.startswith("jobstore_")
        },
    }


def _prepare_db(jc: JobController, full_reset: bool) -> None:
    """
    Bring the DB of a JobController to the state obtained with a reset.

    A full ``JobController.reset`` is performed if requested, if it is the first
    time the database is seen or if the indexes do not match those obtained
    with the reset (e.g. because a test dropped the database or changed the indexes).
    Otherwise the documents are restored to those of the snapshot taken after a
    full reset, avoiding the expensive rebuild of the indexes.

    Notes
    -----
    The snapshot is generated from the outcome of ``JobController.reset``, so
    changes to the reset procedure are automatically taken into account. The
    side effects of the reset that are not related to the DB (e.g. the cleanup
    of the folders of batch workers) are not repeated.

    Parameters
    ----------
    jc
        The JobController to prepare.
    full_reset
        If True always perform a full reset.
    """
    key = _snapshot_key(jc)
    snapshot = _RESET_SNAPSHOTS.get(key)

    if not full_reset and snapshot is not None:
        colls = _tracked_collections(jc)
        try:
            indexes_ok = all(
                coll.index_information() == snapshot["indexes"][name]
                for name, coll in colls.items()
            )
        except Exception:
            indexes_ok = False
        if indexes_ok:
            for name, docs in snapshot["docs"].items():
                coll = colls[name]
                coll.delete_many({})
                if docs:
                    coll.insert_many(docs)
            return

    assert jc.reset(max_limit=0)
    _RESET_SNAPSHOTS[key] = _take_snapshot(jc)


def _job_controller_impl(
    random_project_name: str,
    request: pytest.FixtureRequest,
    shared_test_out_dir: Callable[[], Path],
    full_reset: bool,
) -> Generator[JobController, None, None]:
    """
    Generator implementing the ``job_controller`` and ``job_controller_reset``
    fixtures.

    Parameters
    ----------
    random_project_name
        The name of the project.
    request
        The pytest request of the fixture.
    shared_test_out_dir
        Function returning the folder where the DB is dumped if the test fails.
    full_reset
        If True always perform a full reset of the DB at the beginning.

    Yields
    ------
    JobController
        The JobController with the DB in the same state as after a reset.
    """
    from monty.serialization import dumpfn

    from jobflow_remote.jobs.jobcontroller import JobController

    jc = JobController.from_project_name(random_project_name)
    # Always close the connections at the end. Otherwise the open sockets
    # accumulate across the tests until garbage collected and, in long test
    # sessions, can exceed the limit of open files (e.g. 256 by default on macOS)
    try:
        _prepare_db(jc, full_reset=full_reset)
        yield jc

        if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
            target_dir = shared_test_out_dir()

            # use the test name (including parameters) as a target folder
            test_name = request.node.name
            sanitized_test_name = "".join(c if c.isalnum() else "_" for c in test_name)

            test_dump_dir = target_dir / sanitized_test_name
            test_dump_dir.mkdir(parents=True, exist_ok=True)

            # don't use the backup to create indented json files for easier access

            for coll in [jc.jobs, jc.flows, jc.auxiliary]:
                dump_file_path = test_dump_dir / f"{coll.name}.json"
                dumpfn(list(coll.find()), dump_file_path, indent=2)

            print(f"Data dumped to {test_dump_dir}")
    finally:
        jc.close()


@pytest.fixture()
def job_controller(random_project_name, request, shared_test_out_dir, patch_project):
    """Yields a jobcontroller instance for the test suite that also sets up the
    jobstore, bringing the DB to the state obtained after a reset before every test.

    The first time a database is used a full reset is performed. Afterwards the
    documents are restored from a snapshot taken after that reset, without
    rebuilding the indexes (see ``_prepare_db``). A full reset is performed
    again if the indexes do not match the snapshot. Use ``job_controller_reset``
    for the tests that need a full reset, or set the ``JF_TEST_FULL_RESET``
    environment variable to force it for all the tests.

    Notes:
        This job_controller fixture depends on patch_project to make sure that patch_project
        is called before initializing the JobController object. The patch_project fixture is
        not explicitly used within this fixture though.
    """
    yield from _job_controller_impl(
        random_project_name,
        request,
        shared_test_out_dir,
        full_reset=bool(os.getenv(FULL_RESET_ENV_VAR)),
    )


@pytest.fixture()
def job_controller_reset(
    random_project_name, request, shared_test_out_dir, patch_project
):
    """Same as ``job_controller``, but always performs a full ``JobController.reset``
    before the test. To be used by the tests that depend on the reset procedure
    itself (e.g. on the creation of the indexes or on the cleanup of the batch
    workers) and not only on the DB being empty.
    """
    yield from _job_controller_impl(
        random_project_name, request, shared_test_out_dir, full_reset=True
    )


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


def pytest_collection_modifyitems(config, items):
    from importlib.metadata import version

    from packaging.version import Version

    valid_markers = {"unit", "db", "integration"}

    jobflow_version = Version(version("jobflow"))

    for item in items:
        requires_jobflow = item.get_closest_marker("requires_jobflow")
        if requires_jobflow:
            min_version = requires_jobflow.args[0]
            if jobflow_version < Version(min_version):
                item.add_marker(
                    pytest.mark.skip(
                        reason=f"Requires jobflow>={min_version}, "
                        f"installed version is {jobflow_version}"
                    )
                )

        if item.nodeid.startswith(os.path.join("tests", "integration", "")):
            item.add_marker(pytest.mark.integration)
        elif item.nodeid.startswith(os.path.join("tests", "db", "")):
            item.add_marker(pytest.mark.db)
        elif item.nodeid.startswith(os.path.join("tests", "unit", "")):
            item.add_marker(pytest.mark.unit)
        elif len(valid_markers.intersection(item.keywords.keys())) != 1:
            raise RuntimeError(
                "Tests should be marked as either unit, db "
                "or integration, or be in one of the corresponding "
                "folders for unit, db or integration tests."
            )

    # Ensure each test has exactly one marker from the valid set
    for item in items:
        applied_markers = set(item.keywords.keys())
        # Check that the test has exactly one valid marker
        matching_markers = applied_markers.intersection(valid_markers)
        if len(matching_markers) != 1:
            raise AssertionError(
                f"Test {item.nodeid} should be marked with one of the test type markers "
                f"({', '.join(valid_markers)}).\n"
                f"Found: {matching_markers}"
            )


def pytest_addoption(parser):
    """Add a command-line option to enable the reporting of the coverage per flag."""
    parser.addoption(
        "--coverage-per-flag",
        action="store_true",
        default=False,
        dest="coverage_per_flag",
        help="Enable the reporting of the coverage per flag.",
    )
    parser.addoption(
        "--copy-files-from-containers",
        "--cffc",
        action="store_true",
        default=False,
        dest="copy_files_from_containers",
        help="Copy back the files from the containers.",
    )
    parser.addoption(
        "--keep-containers-alive",
        "--kca",
        action="store_true",
        default=False,
        dest="keep_containers_alive",
        help="Keep the containers alive for inspection.",
    )


@pytest.fixture(scope="session", autouse=True)
def raise_open_files_limit() -> None:
    """
    Raise the soft limit of open files for the test session.

    Each JobController opens several MongoClient, whose sockets and monitoring
    threads are released asynchronously after closing them. When the tests run
    quickly the open files can transiently exceed the low default soft limit of
    some systems (256 on macOS). When this happens connections fail in
    seemingly random ways (e.g. ``AutoReconnect`` due to ``localhost`` not being
    resolved).
    """
    try:
        import resource
    except ImportError:  # e.g. on Windows
        return

    target = 8192
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft >= target:
            return
        new_soft = target if hard == resource.RLIM_INFINITY else min(target, hard)
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
    except (ValueError, OSError):
        warnings.warn("Could not raise the limit of open files.", stacklevel=2)


def pytest_sessionstart(session):
    if session.config.getoption("coverage_per_flag"):
        import coverage

        # This part is not used in the GitHub testing workflows.
        # The goal is to be able to test the coverage per flag locally.
        # If the --coverage-per-flag option is set, the normal execution of pytest
        # is modified. Three pytest sessions (for each of the unit/db/integration markers
        # are executed (see the pytest.main calls below).
        # The normal execution is exited at the end of this pytest_sessionstart.
        # In this procedure, we want to keep the other options passed initially to pytest.
        # Hence, we get the initial arguments, remove the --coverage-per-flag argument and
        # modify the last -m argument if any (pytest only considers the last -m argument,
        # the others are ignored).
        modified_args = [
            arg for arg in sys.argv[1:] if arg not in ("--coverage-per-flag",)
        ]
        if "-m" not in modified_args:
            index_last_minus_m = None
        else:
            index_last_minus_m = next(
                i for i, v in reversed(list(enumerate(modified_args))) if v == "-m"
            )

        for marker in ("unit", "db", "integration"):
            if index_last_minus_m is None:
                this_marker_args = ["-m", marker, *modified_args]
            else:
                this_marker_args = list(modified_args)
                marker_expr = this_marker_args[index_last_minus_m + 1]
                this_marker_args[index_last_minus_m + 1] = (
                    f"({marker_expr}) and {marker}"
                )

            session.config.option.cov_config = "pyproject.toml"
            session.config.option.cov = "jobflow_remote"
            os.environ["COVERAGE_FILE"] = f".coverage-{marker}"
            this_marker_args.extend(
                ["--cov=jobflow_remote", "--cov-config=pyproject.toml"]
            )
            pytest.main(this_marker_args)

        # Here we combine the different coverage files together. This is done manually here based
        # on which coverage files needed to be combined for each flag (defined in the coverage-flags.yaml file).
        # In the GitHub testing workflow, the same coverage-flags.yaml file is used by the codecov action
        # to combine and upload coverages to codecov.
        import yaml

        module_dir = Path(__file__).resolve().parent
        with open(module_dir / "coverage-flags.yml") as f:
            flags = yaml.safe_load(f)

        os.environ.pop("COVERAGE_FILE", None)
        for flag, cov_files in flags.items():
            cov = coverage.Coverage()
            cov.combine(cov_files, keep=True)
            print(f"\n\nCoverage for {flag} tests:\n{' '*(20+len(flag))}\n")
            cov.report()
            cov.html_report(directory=f"htmlcov_{flag}")
            print(f"\nCoverage report generated in 'htmlcov_{flag}' directory")

        # Exit the original pytest run (prevent double execution)
        pytest.exit("Rerunning pytest separately for unit, db, and integration tests")


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


def update_project_data(
    update: dict, dict_mods: bool = True, project_file_path: Path = None
):
    from jobflow.utils.dict_mods import apply_mod
    from monty.serialization import dumpfn, loadfn

    from jobflow_remote.config.base import Project

    d = loadfn(project_file_path)

    if dict_mods:
        apply_mod(update, d)
    else:
        d = d | update

    # Validate the Project to get an error in case of invalid file generated,
    # otherwise if the values are wrong the error may be hidden by the
    # runner/daemon
    Project.model_validate(d)

    dumpfn(d, project_file_path)


@contextmanager
def update_project_data_ctx(
    update: dict, dict_mods: bool = True, project_file_path: Path = None
):
    from jobflow_remote.config.manager import ConfigManager

    cm = ConfigManager()
    current_project_data = cm.get_project_data()

    update_project_data(
        update=update, dict_mods=dict_mods, project_file_path=project_file_path
    )
    yield

    cm.dump_project(current_project_data)


@pytest.fixture()
def patch_project_ctx():
    from jobflow_remote.config.manager import ConfigManager

    cm = ConfigManager()
    current_project_data = cm.get_project_data()

    return partial(
        update_project_data_ctx, project_file_path=Path(current_project_data.filepath)
    )


@pytest.fixture()
def patch_project(monkeypatch, request):
    from jobflow_remote.config.manager import ConfigManager

    cm = ConfigManager()
    current_project_data = cm.get_project_data()

    updates = getattr(request, "param", None)

    if updates:
        # case where patch_project is parametrized in the test (with indirect=True)
        # this is useful for the job_controller initialization
        update_project_data(
            updates,
            dict_mods=True,
            project_file_path=Path(current_project_data.filepath),
        )
    # you can always reparametrize inside the test
    yield partial(
        update_project_data, project_file_path=Path(current_project_data.filepath)
    )

    cm.dump_project(current_project_data)


@pytest.fixture()
def remove_jfremote_modules():
    """
    At the end of the test remove the jobflow_remote modules from sys
    so that they are reloaded in the next test.

    Used in case a test modifies the objects in the tests and to prevent
    influencing the other tests.
    """
    import sys

    yield

    for module_name in list(sys.modules.keys()):
        if module_name.startswith("jobflow_remote"):
            try:
                if module_name in sys.modules:
                    del sys.modules[module_name]
            except Exception:
                pass


@pytest.fixture(autouse=True)
def disable_initial_load_plugins(monkeypatch):
    """
    Disable the execution of the initial load_plugins happening at import time
    to avoid that plugins installed in the environment change the CLI.
    """
    from jobflow_remote import SETTINGS

    monkeypatch.setattr(SETTINGS, "cli_load_plugins", False)


def reenable_log_propagation(f):
    def wrapper(*args, **kwargs):
        for logger_dict in args[0].get("loggers", {}).values():
            if not logger_dict.get("propagate", True):
                logger_dict["propagate"] = True
        # setting disable_existing_loggers to False does not seem strictly
        # necessary, but doing it anyway.
        args[0]["disable_existing_loggers"] = False

        return f(*args, **kwargs)

    return wrapper


@pytest.fixture(autouse=True)
def allow_log_propagation_during_dict_config(monkeypatch):
    """
    logging utils in jobflow_remote disable log propagation, preventing
    caplog to properly capture the logs during tests.
    This fixture intercepts the calls to dictConfig and sets propagate=True
    for all the loggers.
    """
    import logging.config

    monkeypatch.setattr(
        logging.config,
        "dictConfig",
        reenable_log_propagation(logging.config.dictConfig),
    )
