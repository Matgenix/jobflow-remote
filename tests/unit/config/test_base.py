import pytest
from pydantic_core import ValidationError


def test_scheduler_type_subclass():
    from qtoolkit.io.base import BaseSchedulerIO
    from qtoolkit.io.slurm import SlurmIO

    from jobflow_remote.config.base import LocalWorker
    from jobflow_remote.testing import MyShellIO

    with pytest.raises(ValidationError, match=r"Unknown scheduler type dodo"):
        LocalWorker.model_validate(
            {"work_dir": "/fakeworkdir", "scheduler_type": "dodo"}
        )

    worker = LocalWorker.model_validate(
        {"work_dir": "/fakeworkdir", "scheduler_type": "slurm"}
    )
    assert isinstance(worker.get_scheduler_io(), SlurmIO)

    with pytest.raises(
        ValidationError,
        match=r"The scheduler_type should either be a str or an as_dict of a subclass of BaseSchedulerIO",
    ):
        LocalWorker.model_validate(
            {
                "work_dir": "/fakeworkdir",
                "scheduler_type": {"dict": "not_an_msonable_as_dict"},
            }
        )

    worker = LocalWorker.model_validate(
        {"work_dir": "/fakeworkdir", "scheduler_type": MyShellIO().as_dict()}
    )

    sched_io = worker.get_scheduler_io()
    assert isinstance(sched_io, MyShellIO)
    assert isinstance(sched_io, BaseSchedulerIO)
    assert sched_io.USERNAME_MAXCHARS == MyShellIO.USERNAME_MAXCHARS


def test_remote_worker_returns_remote_host():
    """Test that RemoteWorker returns a RemoteHost."""
    from jobflow_remote.config.base import RemoteWorker
    from jobflow_remote.remote.host import RemoteHost

    worker = RemoteWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
        }
    )
    host = worker.get_host()
    assert isinstance(host, RemoteHost)
    assert host.host == "login.cluster.edu"


def test_separated_transfer_worker():
    """Test that SeparatedTransferWorker returns a SeparatedTransferHost."""
    from jobflow_remote.config.base import SeparatedTransferWorker
    from jobflow_remote.remote.host import SeparatedTransferHost

    worker = SeparatedTransferWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
            "transfer": {"host": "dtn.cluster.edu"},
        }
    )
    host = worker.get_host()
    assert isinstance(host, SeparatedTransferHost)
    assert host.command_host.host == "login.cluster.edu"
    assert host.transfer_host.host == "dtn.cluster.edu"


def test_separated_transfer_worker_inherits_credentials():
    """Test that transfer host inherits credentials from main host if not specified."""
    from jobflow_remote.config.base import SeparatedTransferWorker

    worker = SeparatedTransferWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "user": "testuser",
            "port": 2222,
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
            "transfer": {"host": "dtn.cluster.edu"},
        }
    )
    host = worker.get_host()
    # Transfer host should inherit user and port from main host
    assert host.transfer_host.user == "testuser"
    assert host.transfer_host.port == 2222


def test_separated_transfer_worker_cli_info():
    """Test that cli_info includes transfer host information."""
    from jobflow_remote.config.base import SeparatedTransferWorker

    worker = SeparatedTransferWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
            "transfer": {"host": "dtn.cluster.edu"},
        }
    )
    info = worker.cli_info
    assert info["host"] == "login.cluster.edu"
    assert info["transfer_host"] == "dtn.cluster.edu"


def test_project_unique_jobs_handle_dir():
    """Test that the Project rejects duplicate ``batch.jobs_handle_dir``
    only when the workers share a host."""
    from jobflow_remote.config.base import Project

    def _local_worker(work_dir: str, handle_dir: str) -> dict:
        return {
            "type": "local",
            "scheduler_type": "shell",
            "work_dir": work_dir,
            "batch": {
                "jobs_handle_dir": handle_dir,
                "work_dir": work_dir + "_batch",
            },
        }

    def _remote_worker(host: str, work_dir: str, handle_dir: str) -> dict:
        return {
            "type": "remote",
            "host": host,
            "scheduler_type": "slurm",
            "work_dir": work_dir,
            "batch": {
                "jobs_handle_dir": handle_dir,
                "work_dir": work_dir + "_batch",
            },
        }

    def _project_dict(workers: dict) -> dict:
        return {
            "name": "test_project",
            "queue": {"store": {}},
            "workers": workers,
        }

    # Distinct directories on the same host are accepted
    Project.model_validate(
        _project_dict(
            {
                "w1": _local_worker("/some/test/path/work1", "/some/test/path/h1"),
                "w2": _local_worker("/some/test/path/work2", "/some/test/path/h2"),
            }
        )
    )

    # Identical directories on the same host are rejected
    with pytest.raises(
        ValidationError,
        match=r"share the same `jobs_handle_dir`",
    ):
        Project.model_validate(
            _project_dict(
                {
                    "w1": _local_worker(
                        "/some/test/path/work1", "/some/test/path/same"
                    ),
                    "w2": _local_worker(
                        "/some/test/path/work2", "/some/test/path/same"
                    ),
                }
            )
        )

    # Trailing-slash variants on the same host are treated as equal by Path
    with pytest.raises(
        ValidationError,
        match=r"share the same `jobs_handle_dir`",
    ):
        Project.model_validate(
            _project_dict(
                {
                    "w1": _local_worker(
                        "/some/test/path/work1", "/some/test/path/same/"
                    ),
                    "w2": _local_worker(
                        "/some/test/path/work2", "/some/test/path/same"
                    ),
                }
            )
        )

    # The same path on different hosts is fine: the filesystems are independent
    Project.model_validate(
        _project_dict(
            {
                "w1": _remote_worker(
                    "host_a", "/some/test/path/work1", "/some/test/path/same"
                ),
                "w2": _remote_worker(
                    "host_b", "/some/test/path/work2", "/some/test/path/same"
                ),
            }
        )
    )

    # Different paths on different remote hosts are also accepted
    Project.model_validate(
        _project_dict(
            {
                "w1": _remote_worker(
                    "host_a", "/some/test/path/work1", "/some/test/path/h1"
                ),
                "w2": _remote_worker(
                    "host_b", "/some/test/path/work2", "/some/test/path/h2"
                ),
            }
        )
    )


def test_separated_transfer_worker_transfer_own_credentials():
    """Test that transfer host uses its own credentials when specified."""
    from jobflow_remote.config.base import SeparatedTransferWorker

    worker = SeparatedTransferWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "user": "mainuser",
            "password": "mainpassword",
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
            "transfer": {
                "host": "dtn.cluster.edu",
                "user": "transferuser",
                "password": "transferpassword",
            },
        }
    )
    host = worker.get_host()
    # Transfer host should use its own credentials
    assert host.transfer_host.user == "transferuser"
    # Connect kwargs should include the transfer password
    assert host.transfer_host.connect_kwargs.get("password") == "transferpassword"
    # Command host should use main credentials
    assert host.command_host.user == "mainuser"
    assert host.command_host.connect_kwargs.get("password") == "mainpassword"
