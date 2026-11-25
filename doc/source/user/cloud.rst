.. _cloud:

======================
Running on Cloud (AWS)
======================

A ``cloud`` worker runs jobflow-remote jobs on a cloud batch service (currently
AWS Batch) using cloud object storage (Amazon S3) instead of SSH and a shared
cluster filesystem.

How a cloud worker runs
=======================

Cloud workers follow the same execution model as remote SLURM workers, with S3
replacing the cluster scratch directory and SSH:

1. The local or shared ``Runner`` uploads ``jfremote_in.json`` to your S3
   bucket and submits a container job through the AWS Batch API.
2. Inside the container, ``jf -fe execution run-cloud`` downloads the job
   folder from S3, executes the job locally, and uploads ``jfremote_out.json``
   and the job store JSON files back to S3.
3. After the job finishes, the ``Runner`` downloads the results from S3 and
   inserts them into your permanent ``JobStore`` (for example, ``MongoStore``
   with ``GridFSStore`` or ``S3Store``).

Compute containers never need network access to your MongoDB database.

.. warning::

    Each job runs in its own container, in a temporary folder that is deleted
    when the job ends. A job therefore cannot read files from the run folder
    of an earlier job. Flows that pass ``prev_dir`` between jobs, such as the
    ``atomate2`` VASP relax and static chains, do not work on cloud workers.
    Flows that pass data only through job outputs, such as the force-field
    phonon workflow in :ref:`cloud_gpu_workflow`, work as on HPC.

Just like an HPC cluster where administrators configure SLURM queues once and
researchers reuse them across projects, cloud work splits cleanly into two
parts:

* **Part 1 (one-time setup per lab or account):** Provision your AWS Batch
  queues (CPU and optional GPU), S3 staging bucket, IAM roles, and
  ``jobflow-remote`` project file once.
* **Part 2 (day-to-day high-throughput campaigns):** Reuse those permanent
  queues across as many workflows and software stacks as you want. You only
  push a new container image when your scientific code changes, then submit
  flows in Python.

Part 1: One-time AWS setup (per lab or account)
===============================================

1.1 Install locally and choose your MongoDB
-------------------------------------------

On each machine where you submit flows (and on the host running the
``Runner``), install jobflow-remote with the ``aws`` extra:

.. code-block:: bash

    pip install "jobflow-remote[aws]"

You also need a reachable MongoDB instance for the queue store and
``JobStore``. For single-user testing, a local MongoDB on ``127.0.0.1:27017``
works. For a shared lab setup, use a managed MongoDB service (such as MongoDB
Atlas) and run ``jf runner`` on one small always-on VM or container so
individual researchers only submit flows and query results from their laptops.

.. tip::

    **Already have AWS Batch queues and S3 set up?** Skip Sections 1.2 and 1.3
    and jump straight to :ref:`cloud_configure_worker`. Make sure two IAM
    requirements are met:

    1. Your Batch container task role (``jobRoleArn``) allows
       ``s3:ListBucket`` and ``s3:GetBucketLocation`` on the bucket, plus
       ``s3:GetObject`` and ``s3:PutObject`` on its objects.
    2. Your local AWS credentials allow those S3 actions plus
       ``s3:DeleteObject``, and the AWS Batch actions ``batch:SubmitJob``,
       ``batch:DescribeJobs``, ``batch:ListJobs``, ``batch:TerminateJob``,
       ``batch:CancelJob``, and ``batch:TagResource``.

1.2 Deploy the AWS Batch stack once
-----------------------------------

Deploy our ready-made CloudFormation template
(:download:`aws_batch.yaml </_static/aws_batch.yaml>`) once from **AWS
CloudShell** (no local AWS CLI or Docker required). One stack creates a
dedicated public VPC without a NAT Gateway, a private S3 staging bucket, a
private ECR repository, CloudWatch log groups, scoped IAM roles and a Runner
user, a serverless Fargate CPU queue, an optional EC2 GPU queue (Spot or
On-Demand, scaling to ``0`` nodes when idle), and a reusable CodeBuild project
for building campaign images. When no jobs run, you pay only for the data
kept in S3, ECR, and CloudWatch Logs. While jobs run, AWS also bills the
public IPv4 address of each Fargate job and GPU node.

1. Sign in to the `AWS Management Console <https://console.aws.amazon.com/>`_.
2. In the **top-right region dropdown**, choose your region (for example,
   **US East (Ohio)** (``us-east-2``)).
3. Click the **CloudShell** terminal icon (``>_``) in the **top-right
   navigation bar** (or type ``CloudShell`` in the top search bar).

.. note::

    **EC2 GPU quota check:** New AWS accounts often start with ``0`` vCPUs for
    EC2 ``G``/``VT`` GPU instances (``g4dn``, ``g5``, ``g6``). Check your quota
    in CloudShell before enabling the GPU queue:

    .. code-block:: bash

        aws service-quotas get-service-quota --service-code ec2 --quota-code L-DB2E81BA \
          --query 'Quota.{Name:QuotaName,Value:Value}' --output table
        aws service-quotas get-service-quota --service-code ec2 --quota-code L-3819A6DF \
          --query 'Quota.{Name:QuotaName,Value:Value}' --output table

    The first quota (``L-DB2E81BA``) is for On-Demand GPU instances and the
    second (``L-3819A6DF``) is for Spot GPU instances. Check the one that
    matches ``GpuAllocationType``. That is ``L-3819A6DF`` for ``SPOT`` (the
    default) and ``L-DB2E81BA`` for ``EC2``. If it is ``0``, request an
    increase in **Service Quotas** (for example, ``4`` vCPUs for one
    ``g4dn.xlarge`` or ``g5.xlarge`` GPU node) before running GPU jobs, or
    deploy CPU-only first and enable ``EnableGpuQueue=true`` later on the
    same stack.

Run in **CloudShell** to deploy the stack (takes about 1 to 2 minutes) and
print your permanent queue and bucket outputs. Set ``EnableGpuQueue=true`` for
both CPU and EC2 Spot GPU queues (or omit ``--parameter-overrides`` for
CPU-only):

.. code-block:: bash

    aws iam create-service-linked-role --aws-service-name batch.amazonaws.com 2>/dev/null || true
    aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com 2>/dev/null || true
    aws iam create-service-linked-role --aws-service-name spot.amazonaws.com 2>/dev/null || true

    curl -fsSLO https://raw.githubusercontent.com/Matgenix/jobflow-remote/develop/doc/source/_static/aws_batch.yaml
    aws cloudformation deploy \
      --template-file aws_batch.yaml \
      --stack-name jfr-lab \
      --capabilities CAPABILITY_IAM \
      --parameter-overrides EnableGpuQueue=true GpuAllocationType=SPOT GpuMaxvCpus=16

    aws cloudformation describe-stacks --stack-name jfr-lab \
      --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' --output table

1.3 Create Runner credentials once
----------------------------------

In **CloudShell**, create an access key for the stack's scoped Runner IAM user:

.. code-block:: bash

    RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text)
    aws iam create-access-key --user-name "$RUNNER_USER"

On **your local machine** (or shared Runner host), save the printed
``AccessKeyId`` and ``SecretAccessKey`` in ``~/.aws/credentials``:

.. code-block:: bash

    mkdir -p ~/.aws

.. code-block:: ini

    [jfr-lab]
    aws_access_key_id = YOUR_ACCESS_KEY_ID
    aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

.. code-block:: bash

    chmod 600 ~/.aws/credentials

.. _cloud_configure_worker:

1.4 Configure your project file once
------------------------------------

On **your local machine**, create ``~/.jfremote/aws_ht.yaml`` (or place it in
``$JFREMOTE_PROJECTS_FOLDER``). Define both ``aws_cpu`` and ``aws_gpu``
workers pointing to the same S3 bucket and MongoDB/S3 ``JobStore`` (omit
``aws_gpu`` if you deployed CPU-only). ``profile_name`` is optional if you use
default AWS credentials (``AWS_PROFILE``, ``[default]``, AWS SSO, or an IAM
role):

.. code-block:: bash

    mkdir -p ~/.jfremote

.. code-block:: yaml

    name: aws_ht
    queue:
      store:
        type: MongoStore
        host: 127.0.0.1
        port: 27017
        database: jfr_aws_queue
        collection_name: jobs
    jobstore:
      docs_store:
        type: MongoStore
        host: 127.0.0.1
        port: 27017
        database: jfr_aws_outputs
        collection_name: outputs
      additional_stores:
        data:
          type: GridFSStore
          host: 127.0.0.1
          port: 27017
          database: jfr_aws_outputs
          collection_name: data
    workers:
      aws_cpu:
        type: cloud
        scheduler_type: aws_batch
        storage_type: s3
        bucket: <S3BucketName>
        storage_prefix: runs
        region_name: <Region>
        profile_name: jfr-lab
        job_queue: <CpuJobQueueArn>
        job_definition: <CpuJobDefinitionArn>
      aws_gpu:
        type: cloud
        scheduler_type: aws_batch
        storage_type: s3
        bucket: <S3BucketName>
        storage_prefix: runs
        region_name: <Region>
        profile_name: jfr-lab
        job_queue: <GpuJobQueueArn>
        job_definition: <GpuJobDefinitionArn>

.. note::

    For large binary outputs in ``additional_stores.data``, you can use either
    ``GridFSStore`` (MongoDB) or ``S3Store`` (from ``maggma.stores.aws``, for
    permanent S3 storage), just like on an HPC cluster. The ``bucket`` field on
    ``CloudWorker`` is only the staging area for job execution files.

Verify MongoDB, S3, and AWS Batch connectivity, then initialize the queue
database:

.. code-block:: bash

    jf -p aws_ht project check --errors
    jf -p aws_ht admin reset

.. dropdown:: Quick 2-minute sanity check (no Docker needed)

    Before building a scientific container image, you can test end-to-end
    connectivity on the default ``public.ecr.aws/docker/library/python:3.12-slim``
    image by temporarily adding
    ``pre_run: pip install -q --no-cache-dir "jobflow-remote[aws]"`` under
    ``aws_cpu`` and submitting two tiny jobs:

    .. code-block:: python

        from jobflow import Flow
        from qtoolkit.core.data_objects import QCloudResources

        from jobflow_remote import submit_flow
        from jobflow_remote.utils.examples import add

        job1 = add(1, 2)
        job2 = add(job1.output, 3)
        submit_flow(
            Flow([job1, job2]),
            worker="aws_cpu",
            project="aws_ht",
            resources=QCloudResources(vcpus=1, memory=2048, time_limit=300),
        )

    Run ``jf -p aws_ht runner run`` in one terminal, and check
    ``jf -p aws_ht job list`` and ``jf -p aws_ht job output 2`` (should print
    ``6``) in another. Remove the temporary ``pre_run`` line once you switch
    to your production container image below.

Part 2: Day-to-day high-throughput campaigns
============================================

Once Part 1 is in place, you never touch VPCs, S3 buckets, IAM roles, or Batch
compute environments again. Running a new campaign is just two steps: build a
tagged image for your scientific stack (only when dependencies change), and
submit flows in Python.

.. _cloud_build_image:

2.1 Build a tagged container image for your campaign
----------------------------------------------------

Every Python function executed by a ``Job`` must be importable inside its
container image. Use versioned tags (such as ``mace-gpu-v1`` or ``mace-gpu-v2``)
inside your single shared ECR repository so multiple campaigns coexist cleanly.

Below is an example CUDA-enabled ``Dockerfile`` for GPU ``atomate2`` +
MACE-OMAT-0 + ``pheasy`` phonon campaigns (with CUDA PyTorch, ``mace-torch``,
and ``pheasy`` installed from git as documented in ``atomate2``):

.. tab-set::

    .. tab-item:: AWS CodeBuild (no local Docker)

        Run in **CloudShell** (in the same AWS region selected in Part 1). Set
        ``IMAGE_TAG`` to any version tag for your campaign:

        .. code-block:: bash

            export IMAGE_TAG="mace-gpu-v1"
            BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
            CB_PROJ=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='CodeBuildProjectName'].OutputValue" --output text)

            cat > /tmp/Dockerfile <<'EOF'
            FROM public.ecr.aws/docker/library/python:3.12-slim

            ENV PIP_NO_CACHE_DIR=1 \
                PYTHONDONTWRITEBYTECODE=1 \
                PYTHONUNBUFFERED=1

            RUN apt-get update && apt-get install -y --no-install-recommends git gcc g++ && rm -rf /var/lib/apt/lists/*
            RUN pip install --upgrade pip && \
                pip install --index-url https://download.pytorch.org/whl/cu124 torch && \
                pip install "numpy<=2.2" "mace-torch>=0.3.16" "jobflow-remote[aws]" \
                  "atomate2[pheasy,ase]" "f90nml>=1.3.1" "h5py>=3.6.0" "numba>=0.58.1" \
                  "scikit-learn>=1.0.2" "spglib>=1.16.3" && \
                pip install --no-deps "pheasy @ git+https://gitlab.com/hpsahasrabuddhe/pheasy.git@9f24162a4ed0f0ab8911d382fd2617944aade55d"

            WORKDIR /tmp/jf_run
            EOF

            (cd /tmp && zip -q context.zip Dockerfile)
            aws s3 cp /tmp/context.zip "s3://${BUCKET}/build/context.zip"

            BUILD_ID=$(aws codebuild start-build --project-name "$CB_PROJ" \
              --environment-variables-override "name=IMAGE_TAG,value=${IMAGE_TAG},type=PLAINTEXT" \
              --query 'build.id' --output text)
            while [ "$(aws codebuild batch-get-builds --ids "$BUILD_ID" \
              --query 'builds[0].buildStatus' --output text)" = "IN_PROGRESS" ]; do
              sleep 10
            done
            aws codebuild batch-get-builds --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text

        Confirm the printed build status is ``SUCCEEDED`` (you can also view
        build logs by searching for **CodeBuild** in the top AWS Console search
        bar and opening **Build projects**).

    .. tab-item:: Local Docker

        Run on **your local machine** inside an empty folder containing your
        ``Dockerfile``. This requires Docker and local AWS CLI credentials with
        ECR push permissions (not the scoped ``[jfr-lab]`` Runner profile).
        Replace ``<Region>`` and ``<EcrRepositoryUri>`` from Part 1:

        .. code-block:: bash

            export REGION="<Region>"
            export ECR_REPO_URI="<EcrRepositoryUri>"
            export IMAGE_TAG="mace-gpu-v1"
            export ECR_REGISTRY="${ECR_REPO_URI%%/*}"

            aws ecr get-login-password --region "$REGION" | \
              docker login --username AWS --password-stdin "$ECR_REGISTRY"
            docker buildx build --platform linux/amd64 -t "${ECR_REPO_URI}:${IMAGE_TAG}" --push .

Point the stack's CPU and GPU job definitions at the new image in
**CloudShell** (or pass a lighter CPU image for ``ContainerImage``), update
both ``job_definition`` fields in ``~/.jfremote/aws_ht.yaml``, and restart the
``Runner`` (``jf -p aws_ht runner restart`` if running as a daemon):

.. code-block:: bash

    [ -f aws_batch.yaml ] || curl -fsSLO https://raw.githubusercontent.com/Matgenix/jobflow-remote/develop/doc/source/_static/aws_batch.yaml
    ECR_REPO_URI=$(aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)
    IMAGE_TAG="mace-gpu-v1"  # the tag you built in Section 2.1

    aws cloudformation deploy \
      --template-file aws_batch.yaml \
      --stack-name jfr-lab \
      --capabilities CAPABILITY_IAM \
      --parameter-overrides \
        EnableGpuQueue=true \
        GpuAllocationType=SPOT \
        GpuMaxvCpus=16 \
        ContainerImage="${ECR_REPO_URI}:${IMAGE_TAG}" \
        GpuContainerImage="${ECR_REPO_URI}:${IMAGE_TAG}"

    aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='CpuJobDefinitionArn' || OutputKey=='GpuJobDefinitionArn'].[OutputKey,OutputValue]" --output table

.. _cloud_gpu_workflow:

2.2 Run GPU high-throughput workflows with hybrid CPU/GPU routing
-----------------------------------------------------------------

On **your local machine** (with ``atomate2[pheasy]`` installed to construct
flows), submit your flows.

The example below sends the flow to ``aws_gpu`` (with ``gpus=1`` and
``max_retries=2`` so EC2 Spot interruptions retry automatically) and moves
the ``structure_to_primitive`` step, which needs no GPU, to ``aws_cpu``. Add
one ``set_run_config`` call per job name to move other steps the same way.
Keep ``run_phonon_displacements`` on ``aws_gpu``, because the force jobs it
creates run on the same worker as this step.

.. code-block:: python

    from atomate2.forcefields.flows.pheasy import PhononMaker
    from atomate2.forcefields.jobs import ForceFieldRelaxMaker, ForceFieldStaticMaker
    from pymatgen.core import Lattice, Structure
    from qtoolkit.core.data_objects import QCloudResources

    from jobflow_remote import set_run_config, submit_flow

    a = 5.4687
    si_prim = Structure(
        Lattice([[0, a / 2, a / 2], [a / 2, 0, a / 2], [a / 2, a / 2, 0]]),
        ["Si", "Si"],
        [[0, 0, 0], [0.25, 0.25, 0.25]],
    )

    mace_kwargs = {"model": "medium-omat-0", "device": "cuda"}
    maker = PhononMaker(
        min_length=8.0,
        symprec=1e-3,
        use_symmetrized_structure="primitive",
        bulk_relax_maker=ForceFieldRelaxMaker(
            force_field_name="MACE-MP-0",
            calculator_kwargs=mace_kwargs,
            relax_kwargs={"fmax": 1e-4},
        ),
        static_energy_maker=ForceFieldStaticMaker(
            force_field_name="MACE-MP-0",
            calculator_kwargs=mace_kwargs,
        ),
        phonon_displacement_maker=ForceFieldStaticMaker(
            force_field_name="MACE-MP-0",
            calculator_kwargs=mace_kwargs,
        ),
        create_thermal_displacements=False,
    )

    flow = maker.make(si_prim)

    # Run this step on the CPU queue (or on a LocalWorker).
    set_run_config(
        flow,
        name_filter="structure_to_primitive",
        worker="aws_cpu",
        resources=QCloudResources(vcpus=1, memory=2048, time_limit=300),
    )

    submit_flow(
        flow,
        worker="aws_gpu",
        project="aws_ht",
        resources=QCloudResources(
            vcpus=4,
            memory=14000,
            gpus=1,
            max_retries=2,
            time_limit=1800,
        ),
    )

Start the ``Runner`` (foreground with ``jf -p aws_ht runner run``, or as a
daemon with ``jf -p aws_ht runner start``). AWS Batch automatically scales EC2
GPU nodes up to ``GpuMaxvCpus`` to run queued jobs in parallel, caches your
container image on warm EC2 nodes between consecutive jobs, and scales back to
``0`` nodes when the queue empties.

2.3 Reusing the same queues across multiple campaigns
-----------------------------------------------------

To run several campaigns with different software stacks on the same
permanent queues without redeploying CloudFormation:

1. Push each stack to your shared ECR repository under its own tag (for
   example, ``mace-gpu-v1``, ``mace-gpu-v2``).
2. Register one AWS Batch job definition per software stack (or define one
   ``CloudWorker`` per stack in ``aws_ht.yaml`` pointing to the same
   ``job_queue`` with a different ``job_definition``). Copy the stack's job
   definition and change only the image, so the new one keeps the stack's
   job role, execution role, public IP, and log settings. Without the
   stack's job role, the container cannot read or write the S3 bucket.
3. You can also override the job definition on a single submission with
   ``QCloudResources(..., scheduler_kwargs={"jobDefinition": "<arn-or-name>"})``.

Inspecting files, logs, and canceling jobs
==========================================

Standard job file commands work on cloud workers from your local machine:

.. code-block:: bash

    # List or download files from a job's S3 run folder
    jf -p aws_ht job files ls 1
    jf -p aws_ht job files get 1 jfremote_out.json

To cancel an active (queued or running) AWS Batch job, run:

.. code-block:: bash

    jf -p aws_ht job stop <job_id>

``jf job queue-out`` and ``jf job todir`` are not supported for cloud workers
because containers have no interactive SSH shell and stream stdout/stderr to
CloudWatch Logs instead of ``queue.out`` and ``queue.err``. To read container
logs, search for **CloudWatch** in the top AWS Console search bar, open
**Logs -> Log groups**, and select ``/aws/batch/jfr-lab``, or run in
**CloudShell** (in the same AWS region selected in Part 1):

.. code-block:: bash

    aws logs tail /aws/batch/jfr-lab --since 30m

.. _cloud_cleanup:

Tearing down all AWS resources
==============================

When you want to remove the entire lab stack, open **CloudShell** in the same
AWS region selected in Part 1, empty the S3 bucket, delete the Runner IAM
access key, and delete the CloudFormation stack. CloudFormation deletes all
queues, compute environments, job definitions, CodeBuild project, ECR
repository, S3 bucket, log groups, IAM roles, IAM user, and VPC in the right
order:

.. code-block:: bash

    BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text) && \
    RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text) && \
    aws s3 rm "s3://${BUCKET}" --recursive && \
    for k in $(aws iam list-access-keys --user-name "$RUNNER_USER" \
      --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
      aws iam delete-access-key --user-name "$RUNNER_USER" --access-key-id "$k"
    done && \
    aws cloudformation delete-stack --stack-name jfr-lab && \
    aws cloudformation wait stack-delete-complete --stack-name jfr-lab

Finally, remove the ``[jfr-lab]`` section from ``~/.aws/credentials`` on your
local machine.
