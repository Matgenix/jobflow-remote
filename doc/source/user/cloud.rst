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

1. The local ``Runner`` uploads ``jfremote_in.json`` to your S3 bucket and
   submits a container job through the AWS Batch API.
2. Inside the container, ``jf -fe execution run-cloud`` downloads the job
   folder from S3, executes the job locally, and uploads ``jfremote_out.json``
   and the job store JSON files back to S3.
3. After the job finishes, the ``Runner`` downloads the results from S3 and
   inserts them into your permanent ``JobStore`` (for example, ``MongoStore``
   with ``GridFSStore`` or ``S3Store``).

Compute containers never need network access to your MongoDB database.

Step 1: Install the AWS extra locally
=====================================

On your local machine (where you submit flows and run the ``Runner``), install
jobflow-remote with the ``aws`` extra:

.. code-block:: bash

    pip install "jobflow-remote[aws]"

You also need a reachable MongoDB instance for the queue store and
``JobStore``. Either run MongoDB locally on ``127.0.0.1:27017`` or point the
project file in Step 3 to a hosted MongoDB service (such as MongoDB Atlas).

.. tip::

    **Already have AWS set up?** If you already have an S3 bucket, an AWS Batch
    job queue, a registered Batch job definition (using an image with
    ``jobflow-remote[aws]`` or installing it via ``pre_run``), and local AWS
    credentials, skip Step 2 and jump straight to :ref:`cloud_configure_worker`.
    Make sure two IAM requirements are met:

    1. Your Batch container task role (``jobRoleArn``) allows
       ``s3:ListBucket`` and ``s3:GetBucketLocation`` on the bucket, plus
       ``s3:GetObject`` and ``s3:PutObject`` on its objects.
    2. Your local AWS credentials allow those S3 actions plus
       ``s3:DeleteObject``, and the AWS Batch actions ``batch:SubmitJob``,
       ``batch:DescribeJobs``, ``batch:ListJobs``, ``batch:TerminateJob``,
       ``batch:CancelJob``, and ``batch:TagResource``.

Step 2: Deploy AWS resources in one command
===========================================

The easiest way to provision AWS Batch and S3 without installing the AWS CLI or
Docker locally is to deploy our ready-made CloudFormation template
(:download:`aws_batch_fargate.yaml </_static/aws_batch_fargate.yaml>`) from
**AWS CloudShell**.

One stack creates everything needed: a dedicated free public VPC, a private S3
staging bucket, a private ECR repository, CloudWatch log groups, scoped IAM
roles and a Runner IAM user, a managed Fargate compute environment, a job
queue, a job definition, and a CodeBuild project for custom container images.

1. Sign in to the `AWS Management Console <https://console.aws.amazon.com/>`_.
2. In the **top-right region dropdown**, choose your region (for example,
   **US East (Ohio)** (``us-east-2``)). If your account is on an AWS Free Plan,
   pick the region enabled for your account.
3. Click the **CloudShell** terminal icon (``>_``) in the **top-right
   navigation bar** (or type ``CloudShell`` in the top search bar) to open a
   browser shell pre-authenticated to your account.

Run in **CloudShell** to deploy the stack (takes about 1 to 2 minutes) and
print your stack outputs:

.. code-block:: bash

    aws iam create-service-linked-role --aws-service-name batch.amazonaws.com 2>/dev/null || true
    aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com 2>/dev/null || true

    curl -fsSLO https://raw.githubusercontent.com/Matgenix/jobflow-remote/develop/doc/source/_static/aws_batch_fargate.yaml
    aws cloudformation deploy \
      --template-file aws_batch_fargate.yaml \
      --stack-name jfr-demo \
      --capabilities CAPABILITY_IAM

    aws cloudformation describe-stacks --stack-name jfr-demo \
      --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' --output table

Create a local access key for the stack's scoped Runner IAM user in
**CloudShell**:

.. code-block:: bash

    RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-demo \
      --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text)
    aws iam create-access-key --user-name "$RUNNER_USER"

Now switch to **your local machine**. Create ``~/.aws`` if it does not exist,
save the printed ``AccessKeyId`` and ``SecretAccessKey`` in
``~/.aws/credentials``, and restrict file permissions:

.. code-block:: bash

    mkdir -p ~/.aws

.. code-block:: ini

    [jfr-demo]
    aws_access_key_id = YOUR_ACCESS_KEY_ID
    aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

.. code-block:: bash

    chmod 600 ~/.aws/credentials

.. _cloud_configure_worker:

Step 3: Configure the worker and run a quickstart Flow
======================================================

Run on **your local machine**. Create ``~/.jfremote`` if it does not exist,
then save the project file at ``~/.jfremote/aws_demo.yaml`` (or inside the
folder pointed to by ``$JFREMOTE_PROJECTS_FOLDER``):

.. code-block:: bash

    mkdir -p ~/.jfremote

Copy ``S3BucketName``, ``Region``, ``JobQueueArn``, and ``JobDefinitionArn``
from the Step 2 outputs table into ``aws_demo.yaml`` (and point ``queue`` and
``jobstore`` to your MongoDB instance). ``profile_name`` is optional if you use
default AWS credentials (``AWS_PROFILE``, ``[default]``, AWS SSO, or an IAM
role):

.. code-block:: yaml

    name: aws_demo
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
      aws_batch:
        type: cloud
        scheduler_type: aws_batch
        storage_type: s3
        bucket: <S3BucketName>
        storage_prefix: runs
        region_name: <Region>
        profile_name: jfr-demo
        job_queue: <JobQueueArn>
        job_definition: <JobDefinitionArn>
        pre_run: pip install -q --no-cache-dir "jobflow-remote[aws]"

.. note::

    The default stack uses the public ``python:3.12-slim`` image and installs
    ``jobflow-remote[aws]`` via ``pre_run`` so you can test end-to-end with
    zero Docker setup. For production or heavy packages (such as ``atomate2``
    or PyTorch), build a custom image in :ref:`cloud_build_image` and remove
    ``pre_run``.

    For large binary outputs in ``additional_stores.data``, you can use either
    ``GridFSStore`` (MongoDB) or ``S3Store`` (from ``maggma.stores.aws``, for
    permanent S3 storage), just like on an HPC cluster. The ``bucket`` field on
    ``CloudWorker`` is only the staging area for job execution files.

On your local machine, verify MongoDB, S3, and AWS Batch connectivity, then
initialize the queue database:

.. code-block:: bash

    jf -p aws_demo project check --errors
    jf -p aws_demo admin reset

Submit a two-job test workflow on **your local machine**:

.. code-block:: python

    from jobflow import Flow
    from qtoolkit.core.data_objects import QCloudResources

    from jobflow_remote import submit_flow
    from jobflow_remote.utils.examples import add

    job1 = add(1, 2)
    job2 = add(job1.output, 3)
    flow = Flow([job1, job2])

    print(
        submit_flow(
            flow,
            worker="aws_batch",
            project="aws_demo",
            resources=QCloudResources(vcpus=1, memory=2048, time_limit=300),
        )
    )

Start the ``Runner`` locally (in the foreground for testing, or as a daemon
with ``jf -p aws_demo runner start``):

.. code-block:: bash

    jf -p aws_demo runner run

In another local terminal, watch jobs move through ``SUBMITTED``, ``RUNNING``,
and ``COMPLETED``:

.. code-block:: bash

    jf -p aws_demo job list
    jf -p aws_demo job output 2

.. _cloud_build_image:

Step 4: Build a custom container image (optional, for production)
=================================================================

For real workflows (such as ``atomate2``, ML force fields, or compiled codes),
pre-install your Python packages inside the stack's private ECR image so jobs
start without running ``pip install`` every time.

Every Python function executed by a ``Job`` must be importable inside the
container image.

.. tab-set::

    .. tab-item:: AWS CodeBuild (no local Docker)

        Run in **CloudShell** (in the same AWS region selected in Step 2). The
        CloudFormation stack already created the ECR repository, IAM role, and
        CodeBuild project with an inline buildspec. Edit the ``pip install``
        line below to add your packages, zip the ``Dockerfile`` to S3, and run
        one build:

        .. code-block:: bash

            BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-demo \
              --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
            CB_PROJ=$(aws cloudformation describe-stacks --stack-name jfr-demo \
              --query "Stacks[0].Outputs[?OutputKey=='CodeBuildProjectName'].OutputValue" --output text)

            cat > /tmp/Dockerfile <<'EOF'
            FROM public.ecr.aws/docker/library/python:3.12-slim

            ENV PIP_NO_CACHE_DIR=1 \
                PYTHONDONTWRITEBYTECODE=1 \
                PYTHONUNBUFFERED=1

            RUN pip install --upgrade pip && pip install "jobflow-remote[aws]"

            WORKDIR /tmp/jf_run
            EOF

            (cd /tmp && zip -q context.zip Dockerfile)
            aws s3 cp /tmp/context.zip "s3://${BUCKET}/build/context.zip"

            BUILD_ID=$(aws codebuild start-build --project-name "$CB_PROJ" --query 'build.id' --output text)
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
        ``Dockerfile`` (based on ``public.ecr.aws/docker/library/python:3.12-slim``).
        This option requires Docker and local AWS CLI credentials with ECR push
        permissions (not the scoped ``[jfr-demo]`` Runner profile). Replace
        ``<Region>`` and ``<EcrImageUri>`` from Step 2:

        .. code-block:: bash

            export REGION="<Region>"
            export IMAGE_URI="<EcrImageUri>"
            export ECR_REGISTRY="${IMAGE_URI%%/*}"

            aws ecr get-login-password --region "$REGION" | \
              docker login --username AWS --password-stdin "$ECR_REGISTRY"
            docker buildx build --platform linux/amd64 -t "$IMAGE_URI" --push .

After pushing the image (with either tab above), update the stack's job
definition in **CloudShell** (in the same AWS region selected in Step 2):

.. code-block:: bash

    [ -f aws_batch_fargate.yaml ] || curl -fsSLO https://raw.githubusercontent.com/Matgenix/jobflow-remote/develop/doc/source/_static/aws_batch_fargate.yaml
    IMAGE_URI=$(aws cloudformation describe-stacks --stack-name jfr-demo \
      --query "Stacks[0].Outputs[?OutputKey=='EcrImageUri'].OutputValue" --output text)

    aws cloudformation deploy \
      --template-file aws_batch_fargate.yaml \
      --stack-name jfr-demo \
      --capabilities CAPABILITY_IAM \
      --parameter-overrides ContainerImage="$IMAGE_URI"

    aws cloudformation describe-stacks --stack-name jfr-demo \
      --query "Stacks[0].Outputs[?OutputKey=='JobDefinitionArn'].OutputValue" --output text

On your local machine, update ``job_definition`` in ``~/.jfremote/aws_demo.yaml``
with the new ``JobDefinitionArn``, remove the ``pre_run`` line, and restart the
``Runner`` (``jf -p aws_demo runner restart`` if running as a daemon) so new
jobs use the updated revision.

.. dropdown:: Example: atomate2 harmonic phonon workflow (diamond Si)

    Install ``atomate2[pheasy]`` locally so you can construct the workflow. In
    your custom container ``Dockerfile``, install ``git`` (via ``apt-get``)
    along with CPU PyTorch, ``mace-torch``, ``atomate2[pheasy,ase]``, and
    ``pheasy`` (installed from git as documented in ``atomate2``, not from
    PyPI). Then submit with ``2 vCPU`` and ``8192 MiB``:

    .. code-block:: python

        from atomate2.forcefields.flows.pheasy import PhononMaker
        from atomate2.forcefields.jobs import ForceFieldRelaxMaker, ForceFieldStaticMaker
        from pymatgen.core import Lattice, Structure
        from qtoolkit.core.data_objects import QCloudResources

        from jobflow_remote import submit_flow

        a = 5.4687
        si_prim = Structure(
            Lattice([[0, a / 2, a / 2], [a / 2, 0, a / 2], [a / 2, a / 2, 0]]),
            ["Si", "Si"],
            [[0, 0, 0], [0.25, 0.25, 0.25]],
        )

        mace_kwargs = {"model": "medium-omat-0", "device": "cpu"}
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

        submit_flow(
            maker.make(si_prim),
            worker="aws_batch",
            project="aws_demo",
            resources=QCloudResources(vcpus=2, memory=8192, time_limit=900),
        )

.. note::

    Serverless Fargate pulls the container image on each new job (about 30 to
    60 s cold start for large images). For multi-step workflows with
    lightweight Python steps, define a ``LocalWorker`` alongside
    ``CloudWorker`` and route small steps locally with
    ``set_run_config(flow, name_filter=..., worker="local", resources={})``.
    You can also use an EC2-backed AWS Batch compute environment to cache
    images on warm instances.

Step 5: Inspect files, view logs, and cancel jobs
=================================================

Standard job file commands work on cloud workers from your local machine:

.. code-block:: bash

    # List or download files from a job's S3 run folder
    jf -p aws_demo job files ls 1
    jf -p aws_demo job files get 1 jfremote_out.json

To cancel an active (queued or running) AWS Batch job, run:

.. code-block:: bash

    jf -p aws_demo job stop <job_id>

``jf job queue-out`` and ``jf job todir`` are not supported for cloud workers
because containers have no interactive SSH shell and stream stdout/stderr to
CloudWatch Logs instead of ``queue.out`` and ``queue.err``. To read container
logs, search for **CloudWatch** in the top AWS Console search bar, open
**Logs -> Log groups**, and select ``/aws/batch/jfr-demo``, or run in
**CloudShell** (in the same AWS region selected in Step 2):

.. code-block:: bash

    aws logs tail /aws/batch/jfr-demo --since 30m

.. _cloud_cleanup:

Step 6: Tear down all AWS resources
===================================

When you are done testing, open **CloudShell** in the same AWS region selected
in Step 2, empty the S3 bucket, delete the Runner IAM access key, and delete
the CloudFormation stack. CloudFormation automatically deletes the job queue,
compute environment, job definition, CodeBuild project, ECR repository, S3
bucket, log groups, IAM roles, IAM user, and VPC in the right order:

.. code-block:: bash

    BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-demo \
      --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text) && \
    RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-demo \
      --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text) && \
    aws s3 rm "s3://${BUCKET}" --recursive && \
    for k in $(aws iam list-access-keys --user-name "$RUNNER_USER" \
      --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
      aws iam delete-access-key --user-name "$RUNNER_USER" --access-key-id "$k"
    done && \
    aws cloudformation delete-stack --stack-name jfr-demo && \
    aws cloudformation wait stack-delete-complete --stack-name jfr-demo

Finally, remove the ``[jfr-demo]`` section from ``~/.aws/credentials`` on your
local machine.
