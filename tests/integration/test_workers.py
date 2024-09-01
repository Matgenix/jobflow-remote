import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)

WORKERS = ["test_local_worker", "test_remote_slurm_worker", "test_remote_sge_worker"]


def test_project_init(random_project_name, write_tmp_settings) -> None:
    from jobflow_remote.config import ConfigManager

    cm = ConfigManager()
    assert len(cm.projects) == 1
    assert cm.projects[random_project_name]
    project = cm.get_project()
    assert len(project.workers) == len(write_tmp_settings.workers)


def test_paramiko_ssh_connection(job_controller, slurm_ssh_port) -> None:
    from paramiko import SSHClient
    from paramiko.client import WarningPolicy

    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy)
    client.connect(
        "localhost",
        port=slurm_ssh_port,
        username="jobflow",
        password="jobflow",
        look_for_keys=False,
        allow_agent=False,
    )


def test_project_check(job_controller, capsys) -> None:
    from jobflow_remote.testing.cli import run_check_cli

    expected = [
        "✓ Worker test_local_worker",
        "✓ Worker test_sanitize_local_worker",
        "✓ Worker test_remote_worker",
        "✓ Worker test_remote_limited_worker",
        "✓ Worker test_batch_remote_worker",
        "✓ Worker test_max_jobs_worker",
        "✓ Worker test_sanitize_remote_worker",
        "✓ Worker test_remote_slurm_worker",
        "✓ Worker test_remote_sge_worker",
        "✓ Jobstore",
        "✓ Queue store",
    ]
    # Here there will be "errors" reported as there likely be a mismatch of the
    # jobflow-remote version between the local environment and the one in the
    # container
    run_check_cli(["project", "check", "-e"], required_out=expected)


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_submit_flow(worker, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=10)

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


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_submit_flow_with_dependencies(worker, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, write_file

    add_parent_1 = add(1, 1)
    add_parent_2 = add(2, 2)
    add_children = add(add_parent_1.output, add_parent_2.output)
    write = write_file(add_children.output)

    flow = Flow([add_parent_1, add_parent_2, add_children, write])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=20)

    assert len(job_controller.get_jobs({})) == 4
    job_1, job_2, job_3, job_4 = job_controller.get_jobs({})
    assert job_1["job"]["function_args"] == [1, 1]

    output_1 = job_controller.jobstore.get_output(uuid=job_1["uuid"])
    assert output_1 == 2
    output_2 = job_controller.jobstore.get_output(uuid=job_2["uuid"])
    assert output_2 == 4

    output_3 = job_controller.jobstore.get_output(uuid=job_3["uuid"])
    assert output_3 == 6

    output_4 = job_controller.jobstore.get_output(uuid=job_4["uuid"])
    assert output_4 is None

    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 4
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 1
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_job_with_callable_kwarg(worker, job_controller) -> None:
    """Test whether a callable can be successfully provided as a keyword
    argument to a job.

    """
    import math

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import arithmetic

    job_1 = arithmetic(1, -2, op=math.copysign)
    job_2 = arithmetic([job_1.output], [1], op=math.dist)
    job_3 = arithmetic(job_2.output, 2, op=math.pow)

    flow = Flow([job_1, job_2, job_3])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run(ticks=12)

    assert job_controller.count_jobs({}) == 3
    assert len(job_controller.get_jobs({})) == 3
    assert job_controller.count_flows({}) == 1

    jobs = job_controller.get_jobs({})
    outputs = [job_controller.jobstore.get_output(uuid=job["uuid"]) for job in jobs]
    assert outputs == [-1, 2, 4]

    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 3
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 1
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_expected_failure(worker, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import always_fails

    job_1 = always_fails()
    job_2 = always_fails()

    flow = Flow([job_1, job_2])
    submit_flow(flow, worker=worker)

    assert job_controller.count_jobs({}) == 2
    assert len(job_controller.get_jobs({})) == 2
    assert job_controller.count_flows({}) == 1

    runner = Runner()
    runner.run(ticks=10)

    assert job_controller.count_jobs(states=JobState.FAILED) == 2
    assert job_controller.count_flows(states=FlowState.FAILED) == 1


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_exec_config(worker, job_controller, random_project_name) -> None:
    """Tests that an environment variable set in the exec config
    is available to the job.

    """
    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.testing import check_env_var

    job = check_env_var()
    submit_flow(job, worker=worker, exec_config="test")

    assert job_controller.count_jobs({}) == 1
    assert len(job_controller.get_jobs({})) == 1
    assert job_controller.count_flows({}) == 1

    runner = Runner()
    runner.run(ticks=5)

    job = job_controller.get_jobs({})[0]
    output = job_controller.jobstore.get_output(uuid=job["uuid"])
    assert output == random_project_name


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_additional_stores(worker, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add_big

    job = add_big(100, 100)
    flow = Flow(job)
    submit_flow(flow, worker=worker)

    assert job_controller.count_jobs({}) == 1
    assert job_controller.count_flows({}) == 1

    runner = Runner()
    runner.run(ticks=10)

    doc = job_controller.get_jobs({})[0]
    fs = job_controller.jobstore.additional_stores["big_data"]
    assert fs.count({"job_uuid": doc["job"]["uuid"]}) == 1
    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1
    assert job_controller.count_flows(states=FlowState.COMPLETED) == 1
    assert job_controller.jobstore.get_output(uuid=doc["job"]["uuid"])["result"] == 200
    blob_uuid = job_controller.jobstore.get_output(uuid=doc["job"]["uuid"])["data"][
        "blob_uuid"
    ]
    assert (
        next(iter(fs.query({"blob_uuid": blob_uuid})))["job_uuid"] == doc["job"]["uuid"]
    )


@pytest.mark.parametrize(
    "worker",
    WORKERS,
)
def test_undefined_additional_stores(worker, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.config import ConfigError
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_big_undefined_store, create_detour

    # If the Job is passed directly it fails during the submission
    job = add_big_undefined_store(100, 100)
    flow = Flow(job)
    with pytest.raises(ConfigError):
        submit_flow(flow, worker=worker)

    # If the job with missing additional sore is created dynamically
    # it will fail at runtime
    job = create_detour(detour_job=add_big_undefined_store(100, 100))
    submit_flow(job)

    assert job_controller.count_jobs({}) == 1
    assert job_controller.count_flows({}) == 1

    runner = Runner()
    runner.run(ticks=10)

    assert job_controller.count_jobs({}) == 2

    # The job should fail, as the additional store is not defined
    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1
    assert (
        job_controller.count_jobs(states=[JobState.COMPLETED, JobState.REMOTE_ERROR])
        == 2
    )


def test_submit_flow_with_scheduler_username(monkeypatch, job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.remote.queue import QueueManager
    from jobflow_remote.testing import add

    remote_worker_name = "test_remote_worker"

    job = add(1, 1)
    flow = Flow([job])
    submit_flow(flow, worker=remote_worker_name)

    # modify the runner so that uses a patched version of the worker
    # where the scheduler_username is set
    runner = Runner()
    patched_worker = runner.get_worker(remote_worker_name).model_copy()
    patched_worker.scheduler_username = "jobflow"

    def patched_get_worker(self, worker_name):
        if worker_name != remote_worker_name:
            return runner.workers[worker_name]
        return patched_worker

    # Patch the get_jobs_list function to ensure that it is called with
    # the correct parameters.
    patch_called = False
    user_arg = None
    orig_get_jobs_list = QueueManager.get_jobs_list

    def patched_get_jobs_list(self, jobs=None, user=None, timeout=None):
        nonlocal patch_called
        nonlocal user_arg
        patch_called = True
        user_arg = user
        return orig_get_jobs_list(self=self, jobs=jobs, user=user, timeout=timeout)

    with monkeypatch.context() as m:
        m.setattr(Runner, "get_worker", patched_get_worker)
        m.setattr(QueueManager, "get_jobs_list", patched_get_jobs_list)
        runner.run_all_jobs(max_seconds=30)

    assert patch_called, "The patched method was not called"
    assert (
        user_arg == "jobflow"
    ), f"The argument for user passed to QueueManager.get_jobs_list is '{user_arg}' instead of 'jobflow'"

    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 1
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 1
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


@pytest.mark.parametrize(
    "worker",
    ["test_remote_limited_worker"],
)
def test_priority(worker, job_controller) -> None:
    # test only on a limited worker to be sure that only one job will run at the time
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    # use 5 jobs to reduce the possibility of an accidental matching of the
    # order, but not too many, to avoid taking too much time.
    add1 = add(1, 1)
    flow1 = Flow([add1])
    submit_flow(flow1, worker=worker, priority=4)
    add2 = add(1, 1)
    flow2 = Flow([add2])
    submit_flow(flow2, worker=worker, priority=1)
    add3 = add(1, 1)
    flow3 = Flow([add3])
    submit_flow(flow3, worker=worker, priority=5)
    add4 = add(1, 1)
    flow4 = Flow([add4])
    submit_flow(flow4, worker=worker, priority=3)
    add5 = add(1, 1)
    flow5 = Flow([add5])
    submit_flow(flow5, worker=worker, priority=2)

    runner = Runner()
    runner.run_all_jobs(max_seconds=120)

    assert job_controller.count_jobs(states=[JobState.COMPLETED]) == 5
    # check that the jobs were executed according to the priority
    jobs_info = job_controller.get_jobs_info()
    jobs_info = sorted(jobs_info, key=lambda x: x.priority, reverse=True)
    for i in range(len(jobs_info) - 1):
        assert jobs_info[i].end_time < jobs_info[i + 1].start_time


@pytest.mark.parametrize(
    "worker",
    ["test_sanitize_local_worker", "test_sanitize_remote_worker"],
)
def test_sanitize(worker, job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    assert job_controller.project.workers[worker].get_host().sanitize is True

    flow = Flow([add(1, 2)])
    submit_flow(flow, worker=worker)

    runner = Runner()
    runner.run_one_job()

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1
