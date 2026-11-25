from __future__ import annotations

import builtins
import io
import os
from collections import OrderedDict
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from jobflow import Flow, JobStore, job
from maggma.stores import MemoryStore
from monty.serialization import loadfn
from qtoolkit.core.data_objects import (
    CancelResult,
    CancelStatus,
    QJob,
    QResources,
    QState,
    SubmissionStatus,
)
from typer.testing import CliRunner

from jobflow_remote.cli.jf import app as jf_app
from jobflow_remote.config.base import (
    BatchConfig,
    CloudWorker,
    ExecutionConfig,
    LocalWorker,
    Project,
    QueueConfig,
)
from jobflow_remote.config.helper import check_worker
from jobflow_remote.jobs.data import JobDoc, get_initial_job_doc_dict
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.run import run_cloud_job
from jobflow_remote.jobs.runner import Runner
from jobflow_remote.jobs.state import JobState
from jobflow_remote.remote.cloud_queue import CloudQueueManager, sanitize_cloud_job_name
from jobflow_remote.remote.data import get_remote_in_file, get_remote_store
from jobflow_remote.remote.storage import S3Storage, StorageError
from jobflow_remote.utils.examples import add
from jobflow_remote.utils.remote import UnsafeDeletionError, safe_remove_job_files


@job
def chdir_and_write(val: int) -> int:
    sub = Path("nested_subdir")
    sub.mkdir(exist_ok=True)
    os.chdir(sub)
    Path("marker.txt").write_text(f"val={val}")
    return val + 1


@job
def always_fails() -> None:
    raise RuntimeError("intentional failure")


def _set_fake_aws_env(monkeypatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _upload_job_in(storage: S3Storage, jf_job, remote_dir: str) -> str:
    store = JobStore(MemoryStore())
    remote_store = get_remote_store(store=store, work_dir=".", config_dict=None)
    in_buf = get_remote_in_file(jf_job.as_dict(), remote_store)
    storage.put(in_buf, f"{remote_dir}/jfremote_in.json")
    return storage.get_uri(remote_dir)


@pytest.fixture()
def s3_env(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    _set_fake_aws_env(monkeypatch)

    with moto.mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "jf-test-bucket"
        s3.create_bucket(Bucket=bucket)
        storage = S3Storage(bucket=bucket, prefix="proj", region_name="us-east-1")
        yield storage, s3, bucket


@pytest.fixture()
def aws_cloud_env(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    pytest.importorskip("qtoolkit.io.api")
    _set_fake_aws_env(monkeypatch)

    with moto.mock_aws(config={"batch": {"use_docker": False}}):
        s3 = boto3.client("s3", region_name="us-east-1")
        iam = boto3.client("iam", region_name="us-east-1")
        ec2 = boto3.client("ec2", region_name="us-east-1")
        batch = boto3.client("batch", region_name="us-east-1")

        bucket = "jf-cloud-bucket"
        s3.create_bucket(Bucket=bucket)

        role = iam.create_role(
            RoleName="batch-service-role",
            AssumeRolePolicyDocument='{"Version":"2012-10-17","Statement":[]}',
        )["Role"]["Arn"]
        iam.create_instance_profile(InstanceProfileName="ecs-ip")
        iam.add_role_to_instance_profile(
            InstanceProfileName="ecs-ip", RoleName="batch-service-role"
        )
        ip_arn = iam.get_instance_profile(InstanceProfileName="ecs-ip")[
            "InstanceProfile"
        ]["Arn"]

        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc, CidrBlock="10.0.1.0/24")["Subnet"][
            "SubnetId"
        ]
        sg = ec2.create_security_group(
            GroupName="batch-sg", Description="test", VpcId=vpc
        )["GroupId"]

        ce_arn = batch.create_compute_environment(
            computeEnvironmentName="test-ce",
            type="MANAGED",
            state="ENABLED",
            serviceRole=role,
            computeResources={
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 8,
                "desiredvCpus": 0,
                "instanceTypes": ["optimal"],
                "subnets": [subnet],
                "securityGroupIds": [sg],
                "instanceRole": ip_arn,
            },
        )["computeEnvironmentArn"]

        queue_name = "jf-queue"
        batch.create_job_queue(
            jobQueueName=queue_name,
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
        )

        jd_name = "jf-jd"
        batch.register_job_definition(
            jobDefinitionName=jd_name,
            type="container",
            containerProperties={
                "image": "python:3.12-slim",
                "vcpus": 2,
                "memory": 2048,
                "command": ["echo", "default"],
            },
        )

        worker = CloudWorker(
            bucket=bucket,
            storage_prefix="runs",
            region_name="us-east-1",
            job_queue=queue_name,
            job_definition=jd_name,
        )
        yield worker, batch, s3, bucket


class TestS3Storage:
    def test_put_get_exists_listdir_remove_and_rmtree(self, s3_env, tmp_path):
        storage, _s3, bucket = s3_env
        assert storage.test() is None
        assert storage.mkdir("/jf_work/ab/cd") is True
        assert storage.get_uri("/jf_work/ab/cd") == f"s3://{bucket}/proj/jf_work/ab/cd"
        assert storage.get_uri("proj/a.txt") == f"s3://{bucket}/proj/proj/a.txt"

        buf = io.BytesIO(b'{"hello": "world"}')
        storage.put(buf, "/jf_work/ab/cd/jfremote_in.json")
        assert storage.exists("/jf_work/ab/cd/jfremote_in.json")
        assert storage.exists("/jf_work/ab/cd")
        assert not storage.exists("/jf_work/ab/cd/missing.json")

        dl_file = tmp_path / "downloaded.json"
        storage.get("/jf_work/ab/cd/jfremote_in.json", dl_file)
        assert dl_file.read_text() == '{"hello": "world"}'

        # Missing object raises FileNotFoundError and leaves no empty file behind
        missing_dl = tmp_path / "missing.json"
        with pytest.raises(FileNotFoundError):
            storage.get("/jf_work/ab/cd/nope.json", missing_dl)
        assert not missing_dl.exists()

        # Directory upload (skipping broken symlink and skip_files) & download roundtrip
        src_dir = tmp_path / "src_dir"
        (src_dir / "sub").mkdir(parents=True)
        (src_dir / "a.txt").write_text("alpha")
        (src_dir / "skip.txt").write_text("skip-me")
        (src_dir / "sub" / "b.txt").write_text("beta")
        (src_dir / "broken_link").symlink_to(tmp_path / "does_not_exist")
        storage.upload_dir(src_dir, "/jf_work/ab/cd", skip_files={"skip.txt"})

        entries = set(storage.listdir("/jf_work/ab/cd"))
        assert {"jfremote_in.json", "a.txt", "sub"}.issubset(entries)
        assert "skip.txt" not in entries
        assert "broken_link" not in entries

        dst_dir = tmp_path / "dst_dir"
        storage.download_dir("/jf_work/ab/cd", dst_dir)
        assert (dst_dir / "a.txt").read_text() == "alpha"
        assert (dst_dir / "sub" / "b.txt").read_text() == "beta"

        assert safe_remove_job_files(storage, "/jf_work/ab/cd", raise_on_error=True)
        assert storage.listdir("/jf_work/ab/cd") == []

        # Refuses to delete folder without jfremote_in.json
        storage.put(io.BytesIO(b"x"), "/jf_work/other/file.txt")
        with pytest.raises(UnsafeDeletionError):
            safe_remove_job_files(storage, "/jf_work/other", raise_on_error=True)

        storage.remove("/jf_work/other/file.txt")
        assert not storage.exists("/jf_work/other/file.txt")

    def test_path_validation_prefix_guards_and_equality(self, s3_env):
        storage, _s3, _bucket = s3_env

        with pytest.raises(ValueError, match="non-empty"):
            S3Storage(bucket="")
        with pytest.raises(ValueError, match=r"\.\."):
            S3Storage(bucket="b1", prefix="a/../b")
        with pytest.raises(ValueError, match=r"\.\."):
            storage.get_uri("/jf_work/../secret")
        with pytest.raises(ValueError, match="does not match storage bucket"):
            storage.get_uri("s3://other-bucket/proj/a.txt")

        # Refuse deleting bucket root, storage prefix root, or ancestor prefix
        s_root = S3Storage(bucket="b1")
        with pytest.raises(ValueError, match="prefix root"):
            s_root.rmtree("/", raise_on_error=True)
        assert s_root.rmtree("/", raise_on_error=False) is False
        with pytest.raises(ValueError, match="prefix root"):
            storage.rmtree("/", raise_on_error=True)

        nested = S3Storage(bucket="jf-test-bucket", prefix="proj/sub")
        with pytest.raises(ValueError, match="outside storage prefix"):
            nested.rmtree("s3://jf-test-bucket/proj", raise_on_error=True)
        with pytest.raises(ValueError, match="outside storage prefix"):
            nested.get_uri("s3://jf-test-bucket/other/file.txt")

        s1 = S3Storage(bucket="b1", prefix="/p/", region_name="us-east-1")
        s2 = S3Storage(bucket="b1", prefix="p", region_name="us-east-1")
        s3 = S3Storage(bucket="b2", prefix="p", region_name="us-east-1")
        assert s1 == s2
        assert hash(s1) == hash(s2)
        assert s1 != s3
        assert s1.as_dict() == S3Storage.from_dict(s1.as_dict()).as_dict()

    def test_error_wrapping_and_missing_boto3(self, s3_env, monkeypatch, tmp_path):
        from boto3.exceptions import S3UploadFailedError
        from botocore.exceptions import ClientError

        storage, _s3, _bucket = s3_env
        client, _ = storage._get_client_and_errors()

        # Non-404 head_object error wrapped in StorageError
        monkeypatch.setattr(
            client,
            "head_object",
            MagicMock(
                side_effect=ClientError(
                    {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadObject"
                )
            ),
        )
        with pytest.raises(StorageError, match="Failed to check"):
            storage.exists("/jf_work/foo.json")

        # boto3 transfer error wrapped in StorageError
        monkeypatch.setattr(
            client,
            "upload_file",
            MagicMock(side_effect=S3UploadFailedError("upload boom")),
        )
        with pytest.raises(StorageError, match="upload boom"):
            storage.put("/dev/null", "/jf_work/fail.txt")

        # Missing local_dir in upload_dir
        with pytest.raises(FileNotFoundError):
            storage.upload_dir(tmp_path / "does_not_exist", "/jf_work/x")

        # test() returns error string on missing bucket
        missing_bucket = S3Storage(bucket="no-such-bucket-xyz", region_name="us-east-1")
        assert missing_bucket.test() is not None

        # Missing boto3 raises helpful ImportError
        fresh = S3Storage(bucket="b1", region_name="us-east-1")
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "boto3" or name.startswith("botocore"):
                raise ModuleNotFoundError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match="jobflow-remote\\[aws\\]"):
            fresh.put(io.BytesIO(b"x"), "k")


class TestCloudWorkerAndQueueManager:
    def test_cloud_worker_validation_and_project_schema(self):
        pytest.importorskip("qtoolkit.io.api")
        from qtoolkit.io.api import AWSBatchIO
        from qtoolkit.io.slurm import SlurmIO

        worker = CloudWorker(
            bucket="my-bucket",
            region_name="us-east-2",
            job_queue="q1",
            job_definition="jd1",
            endpoint_url="http://batch.local",
            storage_endpoint_url="http://minio.local",
        )
        assert worker.is_cloud is True
        assert worker.is_local is False
        assert worker.is_batch is False
        assert worker.cli_info["bucket"] == "my-bucket"
        assert worker.get_storage().endpoint_url == "http://minio.local"
        assert isinstance(worker.get_transfer_backend(), S3Storage)
        with pytest.raises(NotImplementedError):
            worker.get_host()

        # Serialized BaseAPISchedulerIO dict accepted; BaseSchedulerIO dict rejected
        dict_worker = CloudWorker(
            bucket="my-bucket",
            scheduler_type=AWSBatchIO(job_queue="q2", job_definition="jd2").as_dict(),
        )
        assert isinstance(dict_worker.get_scheduler_io(), AWSBatchIO)

        # Discriminated union through Project.model_validate
        proj = Project.model_validate(
            {
                "name": "p",
                "queue": {},
                "workers": {"cw": {"type": "cloud", "bucket": "my-bucket"}},
            }
        )
        assert isinstance(proj.workers["cw"], CloudWorker)

        with pytest.raises(ValueError, match="BaseAPISchedulerIO"):
            CloudWorker(bucket="my-bucket", scheduler_type=SlurmIO().as_dict())
        with pytest.raises(ValueError, match="non-empty"):
            CloudWorker(bucket="   ")
        with pytest.raises(ValueError, match="batch mode is not supported"):
            CloudWorker(
                bucket="my-bucket",
                batch=BatchConfig(
                    jobs_handle_dir=Path("/scratch/h"),
                    work_dir=Path("/scratch/w"),
                ),
            )
        with pytest.raises(ValueError, match="Unknown cloud API scheduler"):
            CloudWorker(bucket="my-bucket", scheduler_type="slurm")

    def test_sanitize_cloud_job_name(self):
        assert sanitize_cloud_job_name("relax Si (mp-149)") == "relax_Si__mp-149_"
        assert sanitize_cloud_job_name("___bad").startswith("jf_")
        assert sanitize_cloud_job_name("").startswith("jf_")
        assert len(sanitize_cloud_job_name("a" * 200)) == 128

    def test_cloud_queue_manager_with_moto(self, aws_cloud_env):
        from qtoolkit.core.data_objects import QCloudResources

        worker, batch, _s3, _bucket = aws_cloud_env
        qm = CloudQueueManager(scheduler_io=worker.get_scheduler_io())
        sub = qm.submit(
            commands=["jf -fe execution run-cloud s3://jf-cloud-bucket/runs/j1"],
            pre_run=["echo pre1", "echo pre2"],
            post_run="echo post",
            export={"OMP_NUM_THREADS": "2", "PATH": "$PATH:/opt/bin"},
            modules=["python/3.12"],
            options=QCloudResources(vcpus=2, memory=4096, time_limit=300),
            job_name="phonon fit (Si)",
        )
        assert sub.status == SubmissionStatus.SUCCESSFUL
        assert sub.job_id is not None

        raw = batch.describe_jobs(jobs=[sub.job_id])["jobs"][0]
        assert raw["jobName"] == "phonon_fit__Si_"
        cmd_str = raw["container"]["command"][-1]
        assert "echo pre1\necho pre2" in cmd_str
        assert "export OMP_NUM_THREADS=2" in cmd_str
        assert "export PATH=$PATH:/opt/bin" in cmd_str
        assert "module load python/3.12" in cmd_str
        assert "_jf_rc=$?" in cmd_str
        assert "echo post" in cmd_str
        assert "exit $_jf_rc" in cmd_str

        # Empty dict options convert to QCloudResources; explicit job_name preserved
        sub_empty = qm.submit(commands="echo hi", options={}, job_name="empty opts")
        assert sub_empty.status == SubmissionStatus.SUCCESSFUL
        assert (
            qm.prepare_options(
                QCloudResources(job_name="keep-me"), job_name="ignored"
            ).job_name
            == "keep-me"
        )
        with pytest.raises(TypeError, match="options must be"):
            qm.prepare_options(options=123)

        jobs = qm.get_jobs_list(jobs=[sub.job_id])
        assert [j.job_id for j in jobs] == [sub.job_id]
        with pytest.raises(TypeError, match="str/bytes"):
            qm.get_jobs_list(jobs=sub.job_id)

        cancel_res = qm.cancel(sub.job_id, reason="stop")
        assert cancel_res.status == CancelStatus.SUCCESSFUL

    def test_job_doc_accepts_qcloudresources_and_preserves_plain_dicts(self):
        pytest.importorskip("qtoolkit.io.api")
        from qtoolkit.core.data_objects import QCloudResources

        jf_job = add(1, 2)
        res = QCloudResources(vcpus=4, memory=8192, time_limit=600)
        doc_dict = get_initial_job_doc_dict(
            job=jf_job,
            parents=[],
            db_id="1",
            worker="aws_w",
            exec_config=ExecutionConfig(),
            resources=res,
            priority=0,
        )
        assert doc_dict["resources"]["@class"] == "QCloudResources"
        parsed = JobDoc.model_validate(doc_dict)
        assert isinstance(parsed.resources, QCloudResources)
        assert parsed.as_db_dict()["resources"]["vcpus"] == 4

        for plain in ({}, {"gpus": 1}, {"time_limit": 3600}, {"memory": 4096}):
            plain_doc = get_initial_job_doc_dict(
                job=jf_job,
                parents=[],
                db_id="2",
                worker="hpc_w",
                exec_config=ExecutionConfig(),
                resources=plain,
                priority=0,
            )
            validated = JobDoc.model_validate(plain_doc)
            assert type(validated.resources) is dict
            assert validated.resources == plain
            assert "@class" not in validated.as_db_dict()["resources"]


class TestRunCloudJobAndCli:
    def test_run_cloud_job_chdir_retry_cleanup_and_no_double_upload(
        self, s3_env, tmp_path, monkeypatch
    ):
        storage, _s3, _bucket = s3_env
        uri = _upload_job_in(storage, chdir_and_write(41), "/jf_work/test_job")
        # Plant stale files from an aborted prior attempt
        storage.put(io.BytesIO(b"stale"), "/jf_work/test_job/jfremote_out.json")
        storage.put(io.BytesIO(b"stale"), "/jf_work/test_job/remote_job_data.json")

        uploaded_keys: list[str] = []
        orig_upload_dir = S3Storage.upload_dir

        def spy_upload_dir(self, local_dir, remote_dir, skip_files=None):
            uploaded_keys.extend(sorted(skip_files or ()))
            return orig_upload_dir(self, local_dir, remote_dir, skip_files=skip_files)

        monkeypatch.setattr(S3Storage, "upload_dir", spy_upload_dir)

        run_cloud_job(
            storage_uri=uri,
            local_dir=tmp_path / "container_run",
            region_name="us-east-1",
        )

        # Critical files were passed via skip_files so upload_dir does not re-upload them
        assert "jfremote_out.json" in uploaded_keys
        assert "remote_job_data.json" in uploaded_keys
        assert storage.exists("/jf_work/test_job/nested_subdir/marker.txt")

        out_local = tmp_path / "out.json"
        storage.get("/jf_work/test_job/jfremote_out.json", out_local)
        assert loadfn(out_local)["error"] is None

        data_local = tmp_path / "remote_job_data.json"
        storage.get("/jf_work/test_job/remote_job_data.json", data_local)
        assert loadfn(data_local)[0].output == 42

    def test_run_cloud_job_failing_job_uploads_error(self, s3_env, tmp_path):
        storage, _s3, _bucket = s3_env
        uri = _upload_job_in(storage, always_fails(), "/jf_work/fail_job")
        run_cloud_job(
            storage_uri=uri,
            local_dir=tmp_path / "fail_run",
            region_name="us-east-1",
        )
        out_local = tmp_path / "fail_out.json"
        storage.get("/jf_work/fail_job/jfremote_out.json", out_local)
        assert loadfn(out_local)["error"] is not None

    def test_run_cloud_job_rejects_invalid_uris(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported cloud storage URI"):
            run_cloud_job("gs://bucket/path", local_dir=tmp_path)
        with pytest.raises(ValueError, match="non-root key prefix"):
            run_cloud_job("s3://bucket/.", local_dir=tmp_path)
        with pytest.raises(ValueError, match=r"\.\."):
            run_cloud_job("s3://bucket/a/../b", local_dir=tmp_path)

    def test_cli_execution_run_cloud(self, s3_env, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        storage, _s3, _bucket = s3_env
        uri = _upload_job_in(storage, add(10, 20), "/jf_work/cli_job")

        result = CliRunner().invoke(
            jf_app,
            [
                "execution",
                "run-cloud",
                uri,
                "--work-dir",
                str(tmp_path / "cli_run"),
                "--region",
                "us-east-1",
            ],
        )
        assert result.exit_code == 0, result.output
        assert storage.exists("/jf_work/cli_job/jfremote_out.json")


class TestRunnerAndControllerCloudIntegration:
    def test_runner_upload_submit_download_poll_and_cleanup(
        self, aws_cloud_env, tmp_path, monkeypatch
    ):
        from qtoolkit.core.data_objects import QCloudResources

        worker, batch, _s3, _bucket = aws_cloud_env
        worker.resources = QCloudResources(vcpus=2, memory=4096).as_dict()
        local_worker = LocalWorker(
            scheduler_type="shell", work_dir=tmp_path / "local_w"
        )

        project = Project(
            name="cloud_proj",
            workers={"cloud_w": worker, "local_w": local_worker},
            queue=QueueConfig(),
        )

        runner = object.__new__(Runner)
        runner.project_name = "cloud_proj"
        runner.project = project
        runner.workers = project.workers
        runner.limited_workers = {}
        runner.batch_workers = {}
        runner.hosts = {"local_w": local_worker.get_host()}
        runner.storages = {"cloud_w": worker.get_storage()}
        runner.queue_managers = {}
        runner.jobstore = JobStore(MemoryStore())
        runner.jobstore.connect()
        runner.optional_jobstores = {}
        runner._cached_jobstores = OrderedDict()
        runner.job_controller = MagicMock()
        runner.job_controller.get_flow_store.return_value = None

        jf_job = add(7, 8)
        Flow([jf_job])
        doc = {
            "db_id": "1",
            "uuid": jf_job.uuid,
            "index": 1,
            "worker": "cloud_w",
            "job": jf_job.as_dict(),
            "exec_config": None,
            "resources": None,
            "run_dir": f"/jf_work/old_run/{jf_job.uuid}_1",
            "state": JobState.CHECKED_OUT.value,
            "remote": {"process_id": None, "step_attempts": 0, "prerun_cleanup": True},
        }

        def make_lock(current_doc):
            lock = MagicMock()
            lock.locked_document = current_doc
            lock.update_on_release = None
            lock.__enter__.return_value = lock
            lock.__exit__.return_value = False
            return lock

        # 1. upload() cleans old_run and uploads jfremote_in.json
        storage = runner.get_storage("cloud_w")
        storage.put(io.BytesIO(b"stale"), f"{doc['run_dir']}/jfremote_in.json")
        storage.put(io.BytesIO(b"junk"), f"{doc['run_dir']}/old.txt")
        upload_lock = make_lock(doc)
        runner.upload(upload_lock)
        assert upload_lock.update_on_release["$set"]["state"] == JobState.UPLOADED.value
        assert not storage.exists(f"{doc['run_dir']}/old.txt")
        doc["run_dir"] = upload_lock.update_on_release["$set"]["run_dir"]
        assert storage.exists(f"{doc['run_dir']}/jfremote_in.json")

        # 2. submit() builds per-job run-cloud command and submits to AWS Batch
        doc["state"] = JobState.UPLOADED.value
        submit_lock = make_lock(doc)
        runner.submit(submit_lock)
        assert (
            submit_lock.update_on_release["$set"]["state"] == JobState.SUBMITTED.value
        )
        process_id = submit_lock.update_on_release["$set"]["remote.process_id"]
        assert process_id is not None

        raw = batch.describe_jobs(jobs=[process_id])["jobs"][0]
        cmd_str = raw["container"]["command"][-1]
        expected_uri = storage.get_uri(doc["run_dir"])
        assert expected_uri in cmd_str
        assert f"{worker.container_work_dir}/{jf_job.uuid}_1" in cmd_str
        assert "--region us-east-1" in cmd_str

        # Non-cloud worker rejects QCloudResources with clear TypeError
        bad_local_doc = dict(
            doc,
            worker="local_w",
            run_dir=str(tmp_path / "local_w" / "j1"),
            resources=QCloudResources(vcpus=2).as_dict(),
        )
        with pytest.raises(TypeError, match="does not support QCloudResources"):
            runner.submit(make_lock(bad_local_doc))

        # 3. Execute container command in-process via run_cloud_job
        run_cloud_job(
            storage_uri=expected_uri,
            local_dir=tmp_path / "exec_dir",
            region_name="us-east-1",
        )

        # 4. download() skips queue.out/queue.err on cloud workers
        fetched_paths: list[str] = []
        orig_get = storage.get

        def spy_get(src, dst):
            fetched_paths.append(str(src))
            return orig_get(src, dst)

        monkeypatch.setattr(storage, "get", spy_get)
        doc["state"] = JobState.RUN_FINISHED.value
        doc["remote"]["process_id"] = process_id
        runner.project.tmp_dir = str(tmp_path / "runner_tmp")
        download_lock = make_lock(doc)
        runner.download(download_lock)
        assert (
            download_lock.update_on_release["$set"]["state"]
            == JobState.DOWNLOADED.value
        )
        assert not any(p.endswith(("queue.out", "queue.err")) for p in fetched_paths)

        # 5. check_run_status() handles RUNNING -> SUBMITTED spot retry and passes user=None
        fake_qm = MagicMock()
        fake_qm.get_jobs_list.return_value = [
            QJob(job_id=process_id, state=QState.QUEUED)
        ]
        runner.queue_managers["cloud_w"] = fake_qm
        doc["state"] = JobState.RUNNING.value
        poll_lock = make_lock(doc)
        runner.job_controller.get_jobs.return_value = [doc]
        runner.job_controller.lock_job_for_update.return_value = poll_lock

        runner.check_run_status()
        assert poll_lock.update_on_release["$set"]["state"] == JobState.SUBMITTED.value
        assert (
            poll_lock.update_on_release["$set"]["remote.queue_state"]
            == QState.QUEUED.value
        )
        fake_qm.get_jobs_list.assert_called_once_with(jobs=[process_id], user=None)

        runner.cleanup()

    def test_job_controller_cancel_delete_and_resources_on_cloud(
        self, aws_cloud_env, monkeypatch
    ):
        from qtoolkit.core.data_objects import QCloudResources

        worker, _batch, _s3, _bucket = aws_cloud_env
        project = Project(
            name="cloud_proj",
            workers={"cloud_w": worker},
            queue=QueueConfig(),
        )
        jc = object.__new__(JobController)
        jc.project = project

        qm = CloudQueueManager(scheduler_io=worker.get_scheduler_io())
        sub = qm.submit(commands="echo ok", job_name="cancel-me")
        assert sub.job_id is not None

        jc._cancel_queue_process(
            {"worker": "cloud_w", "remote": {"process_id": sub.job_id}}
        )

        # Cancel failure raises RuntimeError
        bad_sched = MagicMock()
        bad_sched.cancel.return_value = CancelResult(
            job_id=sub.job_id, status=CancelStatus.FAILED, stderr="boom"
        )
        monkeypatch.setattr(CloudWorker, "get_scheduler_io", lambda _self: bad_sched)
        with pytest.raises(RuntimeError, match="Cancelling queue process"):
            jc._cancel_queue_process(
                {"worker": "cloud_w", "remote": {"process_id": sub.job_id}}
            )

        # safe_delete_files on CloudWorker uses S3Storage via SharedHosts.get_transfer
        storage = worker.get_storage()
        storage.put(io.BytesIO(b"{}"), "/jf_work/del_me/jfremote_in.json")
        deleted = jc.safe_delete_files(
            [{"run_dir": "/jf_work/del_me", "worker": "cloud_w"}]
        )
        assert len(deleted) == 1
        assert storage.listdir("/jf_work/del_me") == []

        # set_job_run_properties keeps use_pipeline=True when both exec_config dict and QCloudResources are set
        jc._many_jobs_action = MagicMock(return_value=["1"])
        res = QCloudResources(vcpus=2, memory=4096)
        jc.set_job_run_properties(
            exec_config={"pre_run": "echo hi"},
            resources=res,
            update=True,
            db_ids="1",
        )
        call_kwargs = jc._many_jobs_action.call_args.kwargs
        assert call_kwargs["values"]["resources"]["@class"] == "QCloudResources"
        assert call_kwargs["use_pipeline"] is True

    def test_check_worker_cloud(self, aws_cloud_env):
        from qtoolkit.core.data_objects import QCloudResources

        worker, _batch, _s3, _bucket = aws_cloud_env
        err, warn = check_worker(worker)
        assert err is None
        assert warn is None

        # Invalid storage_prefix caught
        err_prefix, _ = check_worker(
            worker.model_copy(update={"storage_prefix": "a/../b"})
        )
        assert err_prefix is not None

        # Non-cloud QResources caught
        err_res, _ = check_worker(
            worker.model_copy(update={"resources": QResources(processes=1).as_dict()})
        )
        assert err_res is not None

        # Invalid AWS Batch QCloudResources (time_limit < 60) caught by check_convert_resources
        err_limit, _ = check_worker(
            worker.model_copy(
                update={"resources": QCloudResources(time_limit=10).as_dict()}
            )
        )
        assert err_limit is not None

    def test_cli_job_and_project_cloud_branches(
        self, aws_cloud_env, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        worker, _batch, _s3, _bucket = aws_cloud_env
        storage = worker.get_storage()
        storage.put(io.BytesIO(b"hello-cloud"), "/jf_work/j1/out.txt")

        project = Project(
            name="cloud_proj",
            workers={"cloud_w": worker},
            queue=QueueConfig(),
        )
        fake_cm = MagicMock()
        fake_cm.get_worker.return_value = worker
        fake_cm.get_project.return_value = project

        fake_job_info = MagicMock()
        fake_job_info.worker = "cloud_w"
        fake_job_info.run_dir = "/jf_work/j1"
        fake_jc = MagicMock()
        fake_jc.get_job_info.return_value = fake_job_info

        monkeypatch.setattr(
            "jobflow_remote.cli.job.get_config_manager", lambda: fake_cm
        )
        monkeypatch.setattr(
            "jobflow_remote.cli.job.get_job_controller", lambda: fake_jc
        )
        monkeypatch.setattr(
            "jobflow_remote.cli.project.get_config_manager", lambda: fake_cm
        )

        cli_runner = CliRunner()
        res_qout = cli_runner.invoke(jf_app, ["job", "queue-out", "1"])
        assert res_qout.exit_code != 0
        assert "cloud logs" in res_qout.output.lower()

        res_todir = cli_runner.invoke(jf_app, ["job", "todir", "1"])
        assert res_todir.exit_code != 0
        assert "interactive shell" in res_todir.output.lower()

        res_ls = cli_runner.invoke(jf_app, ["job", "files", "ls", "1"])
        assert res_ls.exit_code == 0, res_ls.output
        assert "out.txt" in res_ls.output

        dl_dir = tmp_path / "dl"
        dl_dir.mkdir()
        res_get = cli_runner.invoke(
            jf_app, ["job", "files", "get", "1", "out.txt", "-p", str(dl_dir)]
        )
        assert res_get.exit_code == 0, res_get.output
        assert (dl_dir / "out.txt").read_text() == "hello-cloud"

        res_chk = cli_runner.invoke(jf_app, ["project", "check", "-w", "cloud_w"])
        assert res_chk.exit_code == 0, res_chk.output
