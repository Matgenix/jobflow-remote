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


def test_remote_worker_with_transfer():
    """Test that RemoteWorker with transfer option returns SeparatedTransferHost."""
    from jobflow_remote.config.base import RemoteWorker
    from jobflow_remote.remote.host import RemoteHost, SeparatedTransferHost

    # Without transfer option - returns RemoteHost
    worker = RemoteWorker.model_validate(
        {
            "host": "login.cluster.edu",
            "work_dir": "/scratch/work",
            "scheduler_type": "slurm",
        }
    )
    host = worker.get_host()
    assert isinstance(host, RemoteHost)

    # With transfer option - returns SeparatedTransferHost
    worker = RemoteWorker.model_validate(
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
