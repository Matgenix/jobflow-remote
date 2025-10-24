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
