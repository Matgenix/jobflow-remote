import os.path
from shutil import which
from typing import NoReturn

import pymongo
import pytest


def test_submit_flow(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker="test_local_worker")

    runner.run_all_jobs(max_seconds=10)

    assert len(job_controller.get_jobs({})) == 2
    job_1, job_2 = job_controller.get_jobs({})
    assert job_1["job"]["function_args"] == [1, 5]
    assert job_1["job"]["name"] == "add"

    output_1 = job_controller.jobstore.get_output(uuid=job_1["uuid"])
    assert output_1 == 6
    output_2 = job_controller.jobstore.get_output(uuid=job_2["uuid"])
    assert output_2 == 11
    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 2
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 1
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


def test_queries(job_controller, runner) -> None:
    """Test different options to query Jobs and Flows."""
    import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
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

    date_create = datetime.datetime.now()

    assert runner.run_one_job(max_seconds=10)

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1
    assert job_controller.count_jobs(states=JobState.READY) == 2
    assert job_controller.count_jobs(db_ids="1") == 1
    assert job_controller.count_jobs(job_ids=(add_first.uuid, 1)) == 1
    jobs_start_date = job_controller.get_jobs_info(start_date=date_create)
    assert len(jobs_start_date) == 1
    assert jobs_start_date[0].uuid == add_first.uuid

    jobs_end_date = job_controller.get_jobs_info(end_date=date_create)
    assert jobs_end_date[0].uuid == add_second.uuid

    assert job_controller.count_jobs(metadata={"test_meta": 1}) == 1

    assert job_controller.count_jobs(name="add") == 0
    assert job_controller.count_jobs(name="add1") == 1
    assert job_controller.count_jobs(name="add*") == 4

    assert job_controller.count_jobs(flow_ids=flow.uuid) == 2

    assert job_controller.count_jobs(workers="test_local_worker") == 4
    assert job_controller.count_jobs(workers="xxx") == 0

    with job_controller.lock_job(filter={"uuid": add_second.uuid}):
        assert job_controller.count_jobs(locked=True) == 1

    assert (
        job_controller.count_jobs(
            query={"uuid": {"$in": (add_first.uuid, add_second.uuid)}}
        )
        == 2
    )

    assert job_controller.count_flows(states=FlowState.READY) == 1
    assert job_controller.count_flows(states=FlowState.RUNNING) == 1
    assert job_controller.count_flows(job_ids=add_first.uuid) == 1
    assert job_controller.count_flows(db_ids="1") == 1
    assert job_controller.count_flows(flow_ids=flow.uuid) == 1
    assert job_controller.count_flows(start_date=date_create) == 1
    assert job_controller.count_flows(end_date=date_create) == 1
    assert job_controller.count_flows(name="f1") == 1
    assert job_controller.count_flows(name="f*") == 2
    assert (
        job_controller.count_flows(query={"uuid": {"$in": (flow.uuid, flow2.uuid)}})
        == 2
    )


def test_rerun_completed(job_controller, runner) -> None:
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add
    from jobflow_remote.utils.db import JobLockedError

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    j3 = add(j2.output, 2)
    flow = Flow([j1, j2, j3])

    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j1_path = Path(j1_info.run_dir)
    assert j1_info.state == JobState.COMPLETED
    assert j2_info.state == JobState.READY
    assert j3_info.state == JobState.WAITING

    # try rerunning the second job. Wrong state
    with pytest.raises(ValueError, match="The Job is in the READY state"):
        job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index)

    assert len(list(j1_path.iterdir())) > 0

    # since the first job is completed, the force option is required
    with pytest.raises(ValueError, match="Job in state COMPLETED cannot be rerun"):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    # two jobs are modified, as j2 is switched to WAITING
    # Use rerun_jobs instead of rerun_job to test that as well
    assert set(job_controller.rerun_jobs(job_ids=(j1.uuid, j1.index), force=True)) == {
        j1_info.db_id,
        j2_info.db_id,
    }

    assert not j1_path.exists()

    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )
    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.WAITING
    )
    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    # run all the jobs
    runner.run_all_jobs(max_seconds=20)
    assert job_controller.count_jobs(states=JobState.COMPLETED) == 3
    with pytest.raises(ValueError, match="Job in state COMPLETED cannot be rerun"):
        job_controller.rerun_job(job_id=j3.uuid, job_index=j3.index)

    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j3_path = Path(j3_info.run_dir)

    assert len(list(j3_path.iterdir())) > 0

    # The last job can be rerun, but still needs the "force" option.
    # Don't delete files
    assert set(
        job_controller.rerun_job(
            job_id=j3.uuid, job_index=j3.index, force=True, delete_files=False
        )
    ) == {j3_info.db_id}

    assert len(list(j3_path.iterdir())) > 0

    # The remaining tests are to verify that everything is correct with locked jobs
    # as well
    with job_controller.lock_job(filter={"uuid": j2.uuid}):
        with pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j2_info.db_id} is locked with lock_id",
        ):
            job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index)

        # try waiting, but fails in this case as well
        with pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j2_info.db_id} is locked with lock_id",
        ):
            job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index, wait=1)

    # The rerun fails even if a child is locked
    with (
        job_controller.lock_job(filter={"uuid": j3.uuid}),
        pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j3_info.db_id} is locked with lock_id",
        ),
    ):
        job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index, force=True)

    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.COMPLETED
    )

    assert (
        job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.RUNNING
    )

    # can rerun if breaking the lock
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_job(filter={"uuid": j2.uuid}),
    ):
        assert set(
            job_controller.rerun_job(
                job_id=j2.uuid, job_index=j2.index, force=True, break_lock=True
            )
        ) == {j2_info.db_id, j3_info.db_id}

    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.READY
    )


def test_rerun_failed(job_controller, runner) -> None:
    from pathlib import Path

    from jobflow import Flow, OnMissing

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, always_fails, ignore_input, self_replace

    j1 = always_fails()
    j2 = add(j1.output, 2)
    j3 = ignore_input(j1.output)
    j3.config.on_missing_references = OnMissing.NONE
    j4 = self_replace(j3.output)
    flow = Flow([j1, j2, j3, j4])

    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j4_info = job_controller.get_job_info(job_id=j4.uuid, job_index=j4.index)

    assert j1_info.state == JobState.FAILED
    assert j2_info.state == JobState.WAITING
    assert j3_info.state == JobState.READY
    assert j4_info.state == JobState.WAITING

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.FAILED

    # rerun without "force". Since the job is FAILED and the children are
    # WAITING or READY
    assert set(job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)) == {
        j1_info.db_id,
        j3_info.db_id,
    }

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    assert job_controller.count_jobs(states=JobState.READY) == 1

    # run the first job again and the job with OnMissing.None
    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    assert runner.run_one_job(max_seconds=10, job_id=[j3.uuid, j3.index])

    # cannot rerun the first as one of the children is COMPLETED
    with pytest.raises(
        ValueError,
        match="The child of Job.*has state COMPLETED which is not acceptable",
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j1_path = Path(j1_info.run_dir)
    j3_path = Path(j3_info.run_dir)

    assert len(list(j1_path.iterdir())) > 0
    assert len(list(j3_path.iterdir())) > 0

    # can be rerun with the "force" option
    assert set(
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)
    ) == {j1_info.db_id, j3_info.db_id, j4_info.db_id}

    assert job_controller.count_jobs(states=JobState.READY) == 1

    # check that also the folder of the complete child was removed
    assert not j1_path.exists()
    assert not j3_path.exists()

    # run again the jobs with j4. This generates a replace
    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    assert runner.run_one_job(max_seconds=10, job_id=[j3.uuid, j3.index])
    assert runner.run_one_job(max_seconds=10, job_id=[j4.uuid, j4.index])

    assert job_controller.count_jobs(job_ids=(j4.uuid, 2)) == 1

    # At this point it is impossible to rerun, even with the "force" option
    # because it will require rerunning j4, which is COMPLETED with a replacement
    # already existing in the DB.
    with pytest.raises(
        ValueError,
        match="The child of Job.*has state COMPLETED which is not acceptable",
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    with pytest.raises(
        ValueError, match="Job.*has a child job.*which is not the last index"
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)

    assert job_controller.get_job_info(db_id=j1_info.db_id).state == JobState.FAILED
    assert (
        job_controller.get_job_info(job_id=j4.uuid, job_index=2).state == JobState.READY
    )

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.FAILED


def test_rerun_remote_error(job_controller, monkeypatch, runner) -> None:
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")

    # patch the upload method of the runner to trigger a remote error
    def submit_error(self, lock) -> NoReturn:
        raise RuntimeError("FAKE ERROR")

    with monkeypatch.context() as m:
        m.setattr(Runner, "submit", submit_error)
        # patch this to 1 to avoid retrying multiple times
        m.setattr(runner.runner_options, "max_step_attempts", 1)
        with pytest.warns(match="FAKE ERROR"):
            assert runner.run_one_job(max_seconds=10, job_id=(j1.uuid, j1.index))

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    assert j1_info.state == JobState.REMOTE_ERROR
    assert (
        job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.RUNNING
    )

    assert len(list(Path(j1_info.run_dir).iterdir())) > 0

    # can rerun without "force"
    assert job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True) == [
        j1_info.db_id
    ]
    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )
    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    # check that files are being deleted also in case of remote_error
    assert not Path(j1_info.run_dir).exists()


def test_retry(job_controller, monkeypatch, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    # cannot retry a READY job
    with pytest.raises(ValueError, match="Job in state READY cannot be retried"):
        job_controller.retry_job(job_id=j.uuid, job_index=j.index)

    # patch the upload method of the runner to trigger a remote error
    def upload_error(self, lock) -> NoReturn:
        raise RuntimeError("FAKE ERROR")

    # Run to get the Job the REMOTE_ERROR state
    with monkeypatch.context() as m:
        m.setattr(Runner, "upload", upload_error)
        # patch this to 1 to avoid retrying multiple times
        m.setattr(runner.runner_options, "max_step_attempts", 1)
        with pytest.warns(match="FAKE ERROR"):
            assert runner.run_one_job(max_seconds=10, job_id=(j.uuid, j.index))

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.REMOTE_ERROR
    assert j_info.remote.retry_time_limit is not None

    assert job_controller.retry_job(job_id=j.uuid, job_index=j.index) == j_info.db_id

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.CHECKED_OUT
    assert j_info.remote.retry_time_limit is None

    # Run to fail only once
    with monkeypatch.context() as m:
        m.setattr(Runner, "upload", upload_error)
        # patch to make the runner fail only once
        m.setattr(runner.runner_options, "delta_retry", (30, 300, 1200))
        with pytest.warns(match="FAKE ERROR"):
            assert not runner.run_one_job(
                max_seconds=2, job_id=(j.uuid, j.index), raise_at_timeout=False
            )

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.CHECKED_OUT
    assert j_info.remote.retry_time_limit is not None


def test_pause_play(job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    assert job_controller.pause_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.PAUSED
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.PAUSED

    assert job_controller.play_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.READY
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.READY


def test_stop(job_controller, one_job) -> None:
    from jobflow_remote.jobs.state import FlowState, JobState

    j = one_job.jobs[0]
    assert job_controller.stop_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.USER_STOPPED
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.STOPPED


def test_unlock_jobs(job_controller, one_job) -> None:
    j = one_job.jobs[0]
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_job(filter={"uuid": j.uuid}),
    ):
        assert job_controller.unlock_jobs(job_ids=(j.uuid, 1)) == 1


def test_unlock_flows(job_controller, one_job) -> None:
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_flow(filter={"uuid": one_job.uuid}),
    ):
        assert job_controller.unlock_flows(flow_ids=one_job.uuid) == 1


def test_set_job_run_properties(job_controller, one_job) -> None:
    from qtoolkit import QResources
    from qtoolkit.core.data_objects import ProcessPlacement

    from jobflow_remote.config.base import ExecutionConfig

    # test setting worker
    with pytest.raises(
        ValueError, match="worker missing_worker is not present in the project"
    ):
        job_controller.set_job_run_properties(worker="missing_worker")

    assert job_controller.set_job_run_properties(worker="test_local_worker_2")
    assert (
        job_controller.get_job_info(job_id=one_job[0].uuid).worker
        == "test_local_worker_2"
    )

    # test setting exec config
    with pytest.raises(
        ValueError,
        match="exec_config missing_exec_config is not present in the project",
    ):
        job_controller.set_job_run_properties(exec_config="missing_exec_config")

    assert job_controller.set_job_run_properties(exec_config="test")
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == "test"

    ec1 = ExecutionConfig(modules=["some_module"])
    ec2 = ExecutionConfig(pre_run="command")
    ec3 = ExecutionConfig(modules=["some_module"], pre_run="command")
    assert job_controller.set_job_run_properties(exec_config=ec1)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec1

    assert job_controller.set_job_run_properties(exec_config=ec2, update=False)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec2

    assert job_controller.set_job_run_properties(
        exec_config={"modules": ["some_module"]}, update=True
    )
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec3

    # test setting resources
    qr = QResources(
        queue_name="test", process_placement=ProcessPlacement.NO_CONSTRAINTS
    )

    assert job_controller.set_job_run_properties(resources={"ntasks": 10})
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 10
    }
    assert job_controller.set_job_run_properties(resources={"nodes": 10})
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 10,
        "nodes": 10,
    }
    assert job_controller.set_job_run_properties(resources={"ntasks": 20}, update=False)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 20
    }

    assert job_controller.set_job_run_properties(resources=qr)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == qr

    assert job_controller.set_job_run_properties(priority=10)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).priority == 10
    assert job_controller.set_job_run_properties()
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).priority == 10


def test_set_job_doc_properties(job_controller, one_job) -> None:
    from jobflow_remote.jobs.state import JobState

    # error missing input
    with pytest.raises(
        ValueError, match="One and only one among job_id and db_id should be defined"
    ):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"}, acceptable_states=[JobState.COMPLETED]
        )

    # error wrong state
    with pytest.raises(
        ValueError, match="Job in state READY. The action cannot be performed"
    ):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"},
            job_id=one_job[0].uuid,
            acceptable_states=[JobState.COMPLETED],
        )

    job_controller.set_job_doc_properties(
        values={"job.metadata.x": "y"},
        job_id=one_job[0].uuid,
        acceptable_states=[JobState.READY],
    )

    assert job_controller.get_job_doc(job_id=one_job[0].uuid).job.metadata == {"x": "y"}


def test_reset(job_controller, two_flows_four_jobs):
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    assert job_controller.count_jobs() == 4

    assert not job_controller.reset(max_limit=1)
    assert job_controller.count_jobs() == 4
    assert job_controller.reset(max_limit=10, reset_output=True)

    assert job_controller.count_jobs() == 0

    for _ in range(4):
        f = Flow(add(1, 2))
        submit_flow(f, worker="test_local_worker")

    assert job_controller.count_jobs() == 4
    assert not job_controller.reset(max_limit=1, validation="1327-01-01")
    assert job_controller.count_jobs() == 4
    assert job_controller.reset(
        max_limit=1, validation=datetime.now().strftime("%Y-%m-%d")
    )
    assert job_controller.count_jobs() == 0


def test_delete_job(job_controller, two_flows_four_jobs, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState
    from jobflow_remote.testing import add

    runner.run_one_job(job_id=[two_flows_four_jobs[0][0].uuid, 1])
    flow_doc = job_controller.get_flow_info_by_flow_uuid(two_flows_four_jobs[0].uuid)
    assert flow_doc["state"] == FlowState.RUNNING.value

    assert len(job_controller.delete_job(job_id=two_flows_four_jobs[0][1].uuid)) == 1
    assert job_controller.count_jobs() == 3
    flow_doc = job_controller.get_flow_info_by_flow_uuid(two_flows_four_jobs[0].uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 1
    assert len(flow_doc["ids"]) == 1
    assert len(flow_doc["parents"]) == 1
    assert len(flow_doc["parents"][two_flows_four_jobs[0][0].uuid]["1"]) == 0

    assert job_controller.jobstore.get_output(two_flows_four_jobs[0][0].uuid) == 6

    # try deleting the last job of the Flow. should not succeed
    with pytest.raises(
        RuntimeError, match="It is not possible to delete the only Job of the Flow.*"
    ):
        job_controller.delete_job(job_id=two_flows_four_jobs[0][0].uuid)

    runner.run_one_job(job_id=[two_flows_four_jobs[1][0].uuid, 1])

    job3_info = job_controller.get_job_info(two_flows_four_jobs[1][0].uuid)

    assert os.path.isdir(job3_info.run_dir)

    assert (
        len(
            job_controller.delete_job(
                job_id=two_flows_four_jobs[1][0].uuid,
                delete_output=True,
                delete_files=True,
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(two_flows_four_jobs[1][0].uuid)

    assert not os.path.isdir(job3_info.run_dir)

    # add a big flow and try deleting it with small limit
    many_jobs = [add(1, 2)]
    for i in range(12):
        many_jobs.append(add(many_jobs[-1].output, i))
    large_flow = Flow(many_jobs)
    submit_flow(large_flow)
    job_ids = [(j.uuid, 1) for j in many_jobs[1:]]
    with pytest.raises(ValueError, match="Cannot perform deleting on 12 Jobs"):
        job_controller.delete_jobs(job_ids)

    # increase the limit and delete
    assert len(job_controller.delete_jobs(job_ids, max_limit=20)) == 12
    flow_doc = job_controller.get_flow_info_by_flow_uuid(large_flow.uuid)
    assert len(flow_doc["ids"]) == 1


def test_delete_job_with_replace(job_controller, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState
    from jobflow_remote.testing import add, self_replace

    # create a job with a replace to check that the job deletion is handled correctly,
    # although the final state may not be consistent
    j1 = self_replace(1)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")
    runner.run_all_jobs()

    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 2
    assert len(flow_doc["ids"]) == 3
    assert flow_doc["parents"][j2.uuid]["1"] == [j1.uuid]
    assert job_controller.count_jobs() == 3

    job1_info = job_controller.get_job_info(j1.uuid)
    assert len(job_controller.delete_job(job_id=j1.uuid, job_index=2)) == 1
    assert os.path.isdir(job1_info.run_dir)

    assert job_controller.count_jobs() == 2
    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 2
    assert len(flow_doc["ids"]) == 2
    assert len(flow_doc["parents"]) == 2
    assert flow_doc["parents"][j2.uuid]["1"] == [j1.uuid]

    # now delete also the job with index 1 and check again
    assert len(job_controller.delete_job(job_id=j1.uuid, job_index=1)) == 1
    assert job_controller.count_jobs() == 1
    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 1
    assert len(flow_doc["ids"]) == 1
    assert len(flow_doc["parents"]) == 1
    assert flow_doc["parents"][j2.uuid]["1"] == []


def test_add_flow(job_controller):
    from jobflow import Flow

    from jobflow_remote.testing import add

    j = add(1, 2)
    flow = Flow([j])

    job_controller.add_flow(flow, worker="test_local_worker")
    assert job_controller.count_jobs() == 1

    # the same flow cannot be inserted twice
    with pytest.raises(
        ValueError, match=".*A duplicate key error happened while inserting the flow.*"
    ):
        job_controller.add_flow(flow, worker="test_local_worker")
    assert job_controller.count_jobs() == 1


def test_indexes(job_controller, one_job):
    assert len(list(job_controller.jobs.list_indexes())) == 11
    job_controller.create_indexes(
        [[("xxx", pymongo.DESCENDING), ("yyy", pymongo.ASCENDING)]],
        unique=True,
        background=False,
    )
    assert len(list(job_controller.jobs.list_indexes())) == 12

    job_controller.build_indexes(background=False, drop=True)
    assert len(list(job_controller.jobs.list_indexes())) == 11


@pytest.mark.parametrize(
    "python",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                not which("mongodump"), reason="mongodump missing"
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "compress",
    [True, False],
)
def test_backup(job_controller_drop, python, compress):
    import tempfile
    from pathlib import Path

    from jobflow import Flow
    from maggma.stores.mongolike import MemoryStore

    from jobflow_remote.testing import add

    # modify the jobcontroller so that the collections are not the standard ones
    jobs_name = "test_jobs"
    flows_name = "test_flows"
    aux_name = "test_auxiliary"
    job_controller_drop.jobs_collection = jobs_name
    job_controller_drop.jobs = job_controller_drop.db[jobs_name]
    job_controller_drop.queue_store.collection_name = jobs_name
    job_controller_drop.queue_store._coll = job_controller_drop.jobs
    job_controller_drop.flows_collection = flows_name
    job_controller_drop.flows = job_controller_drop.db[flows_name]
    job_controller_drop.auxiliary_collection = aux_name
    job_controller_drop.auxiliary = job_controller_drop.db[aux_name]

    job_controller_drop.reset()

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    # do not use submit_flow, otherwise the original collections will be used.
    job_controller_drop.add_flow(flow, worker="test_local_worker")

    assert job_controller_drop.count_jobs() == 2

    db_name = job_controller_drop.queue_store.database
    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)

        job_controller_drop.backup_dump(
            dir_path=dir_name, compress=compress, python=python
        )

        files = [str(p.name) for p in (dir_path / db_name).glob("*")]

        ext = ".gz" if compress else ""
        assert "flows.bson" + ext in files
        assert "jf_auxiliary.bson" + ext in files
        assert "jobs.bson" + ext in files
        if python:
            assert len(files) == 3
        else:
            assert "flows.metadata.json" + ext in files
            assert "jf_auxiliary.metadata.json" + ext in files
            assert "jobs.metadata.json" + ext in files

            assert len(files) == 6

        with pytest.raises(
            RuntimeError,
            match=f"The collection named {jobs_name} for jobs contains 2 documents.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.jobs.delete_many({})
        with pytest.raises(
            RuntimeError,
            match=f"The collection named {flows_name} for flows contains 1 documents.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.flows.delete_many({})
        with pytest.raises(
            RuntimeError,
            match="next_id value is different from one in the auxiliary collection.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.auxiliary.delete_many({})

        job_controller_drop.backup_restore(dir_path=dir_path / db_name, python=python)

        assert job_controller_drop.count_jobs() == 2
        assert job_controller_drop.count_flows() == 1
        assert job_controller_drop.auxiliary.find({})[0]["next_id"] == 3

        # Other stores not supported for mongodump
        if not python:
            job_controller_drop.queue_store = MemoryStore()
            with pytest.raises(
                ValueError, match="Unsupported store type MemoryStore.*"
            ):
                job_controller_drop.backup_dump(
                    dir_path=dir_name, compress=compress, python=python
                )


def test_count_states(job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    # test both job and flow state count to avoid rerunning the job.
    submit_flow(flow, worker="test_local_worker")

    jobs_states = job_controller.count_jobs_states(list(JobState))
    assert jobs_states[JobState.COMPLETED] == 0
    assert jobs_states[JobState.READY] == 1
    assert jobs_states[JobState.RUNNING] == 0
    assert jobs_states[JobState.WAITING] == 1

    flows_states = job_controller.count_flows_states(list(FlowState))
    assert flows_states[FlowState.COMPLETED] == 0
    assert flows_states[FlowState.READY] == 1
    assert flows_states[FlowState.RUNNING] == 0

    runner = Runner()
    runner.run_one_job()

    jobs_states = job_controller.count_jobs_states(list(JobState))
    assert jobs_states[JobState.COMPLETED] == 1
    assert jobs_states[JobState.READY] == 1
    assert jobs_states[JobState.RUNNING] == 0

    flows_states = job_controller.count_flows_states(list(FlowState))
    assert flows_states[FlowState.COMPLETED] == 0
    assert flows_states[FlowState.READY] == 0
    assert flows_states[FlowState.RUNNING] == 1


def test_get_trends(job_controller, one_job):
    from datetime import datetime

    import dateutil

    from jobflow_remote.jobs.state import FlowState, JobState

    tzname = datetime.now(dateutil.tz.tzlocal()).tzname()

    now = datetime.now()
    utcnow = datetime.utcnow()
    job_trends = job_controller.get_trends(
        list(JobState), interval="days", interval_timezone=tzname
    )
    assert len(job_trends) == 7
    assert now.strftime("%Y-%m-%d") in job_trends
    assert job_trends[now.strftime("%Y-%m-%d")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y-%m-%d")][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="hours", num_intervals=5, interval_timezone=tzname
    )
    assert len(job_trends) == 5
    assert now.strftime("%Y-%m-%d %H") in job_trends
    assert job_trends[now.strftime("%Y-%m-%d %H")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y-%m-%d %H")][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="weeks", interval_timezone="UTC"
    )
    assert len(job_trends) == 4
    assert utcnow.strftime("%Y-%U") in job_trends
    assert job_trends[utcnow.strftime("%Y-%U")][JobState.READY] == 1
    assert job_trends[utcnow.strftime("%Y-%U")][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="years", num_intervals=2, interval_timezone=tzname
    )
    assert len(job_trends) == 2
    assert now.strftime("%Y") in job_trends
    assert job_trends[now.strftime("%Y")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y")][JobState.COMPLETED] == 0

    flow_trends = job_controller.get_trends(
        list(FlowState), interval="days", interval_timezone=tzname
    )
    assert len(flow_trends) == 7
    assert now.strftime("%Y-%m-%d") in flow_trends
    assert flow_trends[now.strftime("%Y-%m-%d")][FlowState.READY] == 1
    assert flow_trends[now.strftime("%Y-%m-%d")][FlowState.COMPLETED] == 0


def test_running_runner(job_controller, daemon_manager, wait_daemon_started):
    from jobflow_remote.utils.db import LockedDocumentError, MissingDocumentError

    assert job_controller.get_running_runner() is None
    daemon_manager.start(single=True)
    wait_daemon_started(daemon_manager)
    assert job_controller.get_running_runner() is not None

    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_auxiliary(filter={"running_runner": {"$exists": True}}),
    ):
        with pytest.raises(LockedDocumentError):
            job_controller.clean_running_runner()

        job_controller.clean_running_runner(break_lock=True)
        assert job_controller.get_running_runner() is None

    job_controller.auxiliary.delete_one({"running_runner": {"$exists": True}})
    with pytest.raises(
        MissingDocumentError, match="Runner document missing from auxiliary collection"
    ):
        job_controller.clean_running_runner()
    assert job_controller.get_running_runner() == "NO_DOCUMENT"
