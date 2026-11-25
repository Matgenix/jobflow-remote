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
project file in Step 4 to a hosted MongoDB service (such as MongoDB Atlas).

.. tip::

    **Already have AWS set up?** If you already have an S3 bucket, an ECR
    repository, an AWS Batch job queue, IAM roles, a CloudWatch log group, and
    local AWS credentials, skip Step 2 and jump to :ref:`cloud_build_image`. If
    you also already have a registered Batch job definition using an image with
    ``jobflow-remote[aws]``, jump straight to :ref:`cloud_configure_worker`.
    Make sure three requirements are met:

    1. Your container image includes ``jobflow-remote[aws]``.
    2. Your Batch container task role (``jobRoleArn``) allows
       ``s3:ListBucket`` and ``s3:GetBucketLocation`` on the bucket, plus
       ``s3:GetObject`` and ``s3:PutObject`` on its objects.
    3. Your local AWS credentials allow those S3 actions plus
       ``s3:DeleteObject``, and the AWS Batch actions ``batch:SubmitJob``,
       ``batch:DescribeJobs``, ``batch:ListJobs``, ``batch:TerminateJob``,
       ``batch:CancelJob``, and ``batch:TagResource``.

Step 2: Set up AWS resources
============================

If you are new to AWS, the easiest way to create the required resources without
installing the AWS CLI locally is **AWS CloudShell** inside the AWS Console.

1. Sign in to the `AWS Management Console <https://console.aws.amazon.com/>`_.
2. In the **top-right region dropdown**, choose the region you want to use
   (for example, **US East (Ohio)** (``us-east-2``)). If your account is on an
   AWS Free Plan, pick the region enabled for your account.
3. Click the **CloudShell** terminal icon (``>_``) in the **top-right
   navigation bar** (or type ``CloudShell`` in the top search bar) to open a
   browser shell pre-authenticated to your account.

Run the commands below in **CloudShell** in order unless noted otherwise.

2.1 Create the S3 bucket, ECR repository, and log group
-------------------------------------------------------

In CloudShell, set your resource names and create a private S3 bucket, an ECR
container repository, and a CloudWatch log group. If you chose a different
region above, change ``REGION="us-east-2"`` to match:

.. code-block:: bash

    export REGION="us-east-2"
    export ACCT=$(aws sts get-caller-identity --query Account --output text)
    export BUCKET="jfr-demo-${ACCT}"
    export ECR_REPO="jfr-demo"
    export CE_NAME="jfr-demo-ce"
    export QUEUE_NAME="jfr-demo-q"
    export JD_NAME="jfr-demo-jd"
    export LOG_GROUP="/aws/batch/jfr-demo"

    if [ "$REGION" = "us-east-1" ]; then
      aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
    else
      aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
        --create-bucket-configuration LocationConstraint="$REGION"
    fi

    aws s3api put-public-access-block --bucket "$BUCKET" --region "$REGION" \
      --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

    aws ecr create-repository --repository-name "$ECR_REPO" --region "$REGION"
    aws logs create-log-group --log-group-name "$LOG_GROUP" --region "$REGION"
    aws logs put-retention-policy --log-group-name "$LOG_GROUP" --retention-in-days 7 --region "$REGION"

    echo "BUCKET=$BUCKET REGION=$REGION"

Note the printed ``BUCKET`` value (``jfr-demo-<your-account-id>``) for Step 4.

2.2 Create the IAM roles for Batch containers
---------------------------------------------

AWS Batch Fargate jobs use two IAM roles:

* **Execution role** (``jfr-demo-exec-role``): pulls your container image from
  ECR and writes stdout/stderr to CloudWatch Logs.
* **Task role** (``jfr-demo-task-role``): gives the container S3 read and write
  access to ``$BUCKET`` without storing AWS keys in environment variables.

In CloudShell, create both roles and ensure the Batch and ECS service-linked
roles exist:

.. code-block:: bash

    aws iam create-service-linked-role --aws-service-name batch.amazonaws.com 2>/dev/null || true
    aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com 2>/dev/null || true

    cat > /tmp/ecs_trust.json <<'EOF'
    {
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }
    EOF

    aws iam create-role --role-name jfr-demo-exec-role --assume-role-policy-document file:///tmp/ecs_trust.json
    aws iam attach-role-policy --role-name jfr-demo-exec-role \
      --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

    aws iam create-role --role-name jfr-demo-task-role --assume-role-policy-document file:///tmp/ecs_trust.json
    cat > /tmp/task_s3_policy.json <<EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
          "Resource": "arn:aws:s3:::${BUCKET}"
        },
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:PutObject"],
          "Resource": "arn:aws:s3:::${BUCKET}/*"
        }
      ]
    }
    EOF
    aws iam put-role-policy --role-name jfr-demo-task-role \
      --policy-name jfr-demo-s3-access --policy-document file:///tmp/task_s3_policy.json
    sleep 10

2.3 Create the Fargate compute environment and job queue
--------------------------------------------------------

In CloudShell, look up your default VPC subnets and default security group,
create a managed Fargate compute environment (``maxvCpus=8``), and attach an
enabled job queue. You can view both in the AWS Console by searching for
**Batch** in the top search bar and opening **Compute environments** and
**Job queues**:

.. code-block:: bash

    VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" \
      --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
    SUBNETS=$(aws ec2 describe-subnets --region "$REGION" \
      --filters Name=vpc-id,Values="$VPC_ID" --query 'Subnets[].SubnetId' --output text | tr '\t' ',')
    SG_ID=$(aws ec2 describe-security-groups --region "$REGION" \
      --filters Name=vpc-id,Values="$VPC_ID" Name=group-name,Values=default \
      --query 'SecurityGroups[0].GroupId' --output text)

    aws batch create-compute-environment --region "$REGION" \
      --compute-environment-name "$CE_NAME" \
      --type MANAGED --state ENABLED \
      --compute-resources "type=FARGATE,maxvCpus=8,subnets=[${SUBNETS}],securityGroupIds=[${SG_ID}]"

    # Wait until the compute environment finishes creating (about 15 seconds)
    while true; do
      STATUS=$(aws batch describe-compute-environments --region "$REGION" \
        --compute-environments "$CE_NAME" --query 'computeEnvironments[0].status' --output text)
      echo "Compute environment status: $STATUS"
      [ "$STATUS" = "CREATING" ] || [ "$STATUS" = "UPDATING" ] || break
      sleep 3
    done

    aws batch create-job-queue --region "$REGION" \
      --job-queue-name "$QUEUE_NAME" \
      --state ENABLED --priority 1 \
      --compute-environment-order "order=1,computeEnvironment=${CE_NAME}"

Confirm that the printed compute environment status is ``VALID`` before
continuing.

2.4 Create local credentials for the Runner
-------------------------------------------

In CloudShell, create a scoped IAM user for your local machine so the
``Runner`` can upload to ``$BUCKET`` and submit or poll jobs on AWS Batch. (You
can also manage IAM users in the AWS Console by searching for **IAM** in the
top search bar and opening **Users**.)

.. code-block:: bash

    cat > /tmp/runner_policy.json <<EOF
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "s3:ListBucket", "s3:GetBucketLocation",
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject"
          ],
          "Resource": ["arn:aws:s3:::${BUCKET}", "arn:aws:s3:::${BUCKET}/*"]
        },
        {
          "Effect": "Allow",
          "Action": [
            "batch:SubmitJob", "batch:DescribeJobs", "batch:ListJobs",
            "batch:TerminateJob", "batch:CancelJob", "batch:TagResource"
          ],
          "Resource": "*",
          "Condition": {"StringEquals": {"aws:RequestedRegion": "${REGION}"}}
        }
      ]
    }
    EOF

    aws iam create-user --user-name jfr-demo-user
    POL_ARN=$(aws iam create-policy --policy-name jfr-demo-runner-policy \
      --policy-document file:///tmp/runner_policy.json --query 'Policy.Arn' --output text)
    aws iam attach-user-policy --user-name jfr-demo-user --policy-arn "$POL_ARN"
    aws iam create-access-key --user-name jfr-demo-user

Now switch to **your local machine**. Create ``~/.aws`` if it does not exist,
then save the printed ``AccessKeyId`` and ``SecretAccessKey`` in
``~/.aws/credentials`` under a named profile:

.. code-block:: bash

    mkdir -p ~/.aws

.. code-block:: ini

    [jfr-demo]
    aws_access_key_id = YOUR_ACCESS_KEY_ID
    aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

Restrict permissions on the credentials file on your local machine:

.. code-block:: bash

    chmod 600 ~/.aws/credentials

.. _cloud_build_image:

Step 3: Build the worker image and register the job definition
==============================================================

Your worker container needs ``jobflow-remote[aws]`` and any scientific packages
your workflows import (for example, ``atomate2``):

.. code-block:: dockerfile

    FROM python:3.12-slim

    ENV PIP_NO_CACHE_DIR=1 \
        PYTHONDONTWRITEBYTECODE=1 \
        PYTHONUNBUFFERED=1

    RUN pip install --upgrade pip && pip install "jobflow-remote[aws]"

    WORKDIR /tmp/jf_run

.. tip::

    Every Python function executed by a ``Job`` must be importable inside the
    container image. Add packages such as ``atomate2`` or ML force fields to
    the ``pip install`` line above.

Build the image for ``linux/amd64`` (the default Fargate architecture) and push
it to your ECR repository using either AWS CodeBuild (recommended if you do not
have Docker or ECR credentials locally) or local Docker:

.. tab-set::

    .. tab-item:: AWS CodeBuild (no local Docker)

        Run in **CloudShell**. This writes ``/tmp/Dockerfile`` and
        ``/tmp/buildspec.yml``, uploads them to S3, and builds and pushes the
        image inside AWS. You can watch build logs in the AWS Console by
        searching for **CodeBuild** in the top search bar and opening
        **Build projects**.

        If you skipped Step 2 or opened a new CloudShell session, export
        ``REGION``, ``ACCT``, ``BUCKET``, and ``ECR_REPO`` (as in Step 2.1)
        before running this block:

        .. code-block:: bash

            cat > /tmp/Dockerfile <<'EOF'
            FROM python:3.12-slim

            ENV PIP_NO_CACHE_DIR=1 \
                PYTHONDONTWRITEBYTECODE=1 \
                PYTHONUNBUFFERED=1

            RUN pip install --upgrade pip && pip install "jobflow-remote[aws]"

            WORKDIR /tmp/jf_run
            EOF

            cat > /tmp/buildspec.yml <<'EOF'
            version: 0.2
            phases:
              pre_build:
                commands:
                  - aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY
              build:
                commands:
                  - docker build --platform linux/amd64 -t $IMAGE_URI .
              post_build:
                commands:
                  - docker push $IMAGE_URI
            EOF

            (cd /tmp && zip -q context.zip Dockerfile buildspec.yml)
            aws s3 cp /tmp/context.zip "s3://${BUCKET}/build/context.zip" --region "$REGION"

            cat > /tmp/cb_trust.json <<'EOF'
            {
              "Version": "2012-10-17",
              "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "codebuild.amazonaws.com"},
                "Action": "sts:AssumeRole"
              }]
            }
            EOF
            aws iam create-role --role-name jfr-demo-cb-role --assume-role-policy-document file:///tmp/cb_trust.json
            cat > /tmp/cb_policy.json <<EOF
            {
              "Version": "2012-10-17",
              "Statement": [
                {"Effect": "Allow", "Action": ["ecr:GetAuthorizationToken"], "Resource": "*"},
                {
                  "Effect": "Allow",
                  "Action": [
                    "ecr:BatchCheckLayerAvailability", "ecr:CompleteLayerUpload",
                    "ecr:InitiateLayerUpload", "ecr:PutImage", "ecr:UploadLayerPart"
                  ],
                  "Resource": "arn:aws:ecr:${REGION}:${ACCT}:repository/${ECR_REPO}"
                },
                {
                  "Effect": "Allow",
                  "Action": ["s3:GetBucketLocation", "s3:GetBucketAcl"],
                  "Resource": "arn:aws:s3:::${BUCKET}"
                },
                {
                  "Effect": "Allow",
                  "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                  "Resource": "arn:aws:s3:::${BUCKET}/build/*"
                },
                {
                  "Effect": "Allow",
                  "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                  "Resource": [
                    "arn:aws:logs:${REGION}:${ACCT}:log-group:/aws/codebuild/jfr-demo-build",
                    "arn:aws:logs:${REGION}:${ACCT}:log-group:/aws/codebuild/jfr-demo-build:*"
                  ]
                }
              ]
            }
            EOF
            aws iam put-role-policy --role-name jfr-demo-cb-role \
              --policy-name jfr-demo-cb-access --policy-document file:///tmp/cb_policy.json
            sleep 10

            export IMAGE_URI="${ACCT}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:latest"
            aws codebuild create-project --region "$REGION" \
              --name jfr-demo-build \
              --source "type=S3,location=${BUCKET}/build/context.zip" \
              --artifacts type=NO_ARTIFACTS \
              --service-role "arn:aws:iam::${ACCT}:role/jfr-demo-cb-role" \
              --environment "type=LINUX_CONTAINER,image=aws/codebuild/standard:7.0,computeType=BUILD_GENERAL1_MEDIUM,privilegedMode=true,environmentVariables=[{name=ECR_REGISTRY,value=${ACCT}.dkr.ecr.${REGION}.amazonaws.com},{name=IMAGE_URI,value=${IMAGE_URI}}]"

            BUILD_ID=$(aws codebuild start-build --region "$REGION" \
              --project-name jfr-demo-build --query 'build.id' --output text)
            while [ "$(aws codebuild batch-get-builds --region "$REGION" \
              --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text)" = "IN_PROGRESS" ]; do
              sleep 10
            done
            aws codebuild batch-get-builds --region "$REGION" \
              --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text

        Confirm the final build status is ``SUCCEEDED``.

    .. tab-item:: Local Docker

        Run on **your local machine** inside an empty folder containing only
        the ``Dockerfile`` above. This option requires Docker and local AWS CLI
        credentials with ECR push permissions (the scoped ``[jfr-demo]`` Runner
        profile from Step 2.4 intentionally does not grant ECR push access).
        Change ``REGION`` and ``ECR_REPO`` if you used different values:

        .. code-block:: bash

            export REGION="us-east-2"
            export ECR_REPO="jfr-demo"
            export ACCT=$(aws sts get-caller-identity --query Account --output text)
            export IMAGE_URI="${ACCT}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:latest"

            aws ecr get-login-password --region "$REGION" | \
              docker login --username AWS --password-stdin "${ACCT}.dkr.ecr.${REGION}.amazonaws.com"
            docker buildx build --platform linux/amd64 -t "$IMAGE_URI" --push .

Run in **CloudShell** to register the Fargate job definition. Adjust ``VCPU``
and ``MEMORY`` defaults as needed. Fargate enforces valid CPU and memory pairs,
such as 1 vCPU with 2048 MiB or 2 vCPU with 8192 MiB. If you skipped Step 2 or
opened a new CloudShell session, export ``REGION``, ``ACCT``, ``ECR_REPO``,
``JD_NAME``, and an existing ``LOG_GROUP``, and replace the two IAM role ARNs
below with your own (for an EC2-backed Batch queue, omit the Fargate-specific
fields):

.. code-block:: bash

    export IMAGE_URI="${ACCT}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO}:latest"
    cat > /tmp/jd.json <<EOF
    {
      "jobDefinitionName": "${JD_NAME}",
      "type": "container",
      "platformCapabilities": ["FARGATE"],
      "containerProperties": {
        "image": "${IMAGE_URI}",
        "executionRoleArn": "arn:aws:iam::${ACCT}:role/jfr-demo-exec-role",
        "jobRoleArn": "arn:aws:iam::${ACCT}:role/jfr-demo-task-role",
        "fargatePlatformConfiguration": {"platformVersion": "LATEST"},
        "networkConfiguration": {"assignPublicIp": "ENABLED"},
        "resourceRequirements": [
          {"type": "VCPU", "value": "1"},
          {"type": "MEMORY", "value": "2048"}
        ],
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "${LOG_GROUP}",
            "awslogs-region": "${REGION}",
            "awslogs-stream-prefix": "jfr"
          }
        },
        "command": ["sh", "-c", "echo default"]
      }
    }
    EOF

    aws batch register-job-definition --region "$REGION" --cli-input-json file:///tmp/jd.json

.. _cloud_configure_worker:

Step 4: Configure the CloudWorker project
=========================================

Run on **your local machine**. Create ``~/.jfremote`` if it does not exist,
then save the project file at ``~/.jfremote/aws_demo.yaml`` (or inside the
folder pointed to by ``$JFREMOTE_PROJECTS_FOLDER``):

.. code-block:: bash

    mkdir -p ~/.jfremote

Replace ``<your-bucket>`` with the ``BUCKET`` value from Step 2.1
(``jfr-demo-<your-account-id>``), update ``region_name``, ``job_queue``, and
``job_definition`` if you used different names or existing AWS Batch resources,
and point ``queue`` and ``jobstore`` to your MongoDB instance. ``profile_name``
is optional and can be omitted if you use default AWS credentials
(``AWS_PROFILE``, ``[default]``, AWS SSO, or an IAM role):

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
        bucket: <your-bucket>
        storage_prefix: runs
        region_name: us-east-2
        profile_name: jfr-demo
        job_queue: jfr-demo-q
        job_definition: jfr-demo-jd

.. note::

    For large binary outputs in ``additional_stores.data``, you can use either
    ``GridFSStore`` (MongoDB) or ``S3Store`` (from ``maggma.stores.aws``, for
    permanent S3 storage), just like on an HPC cluster. The ``bucket`` field on
    ``CloudWorker`` is only the staging area for job execution files.

On your local machine, verify MongoDB, S3, and AWS Batch connectivity, then
initialize the queue database:

.. code-block:: bash

    jf -p aws_demo project check --errors
    jf -p aws_demo admin reset

Step 5: Submit a Flow and run the Runner
========================================

On **your local machine**, start with a quick two-job workflow to confirm
end-to-end execution:

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

Run the ``Runner`` locally (in the foreground for testing, or as a daemon with
``jf -p aws_demo runner start``):

.. code-block:: bash

    jf -p aws_demo runner run

In another local terminal, watch job states progress through ``SUBMITTED``,
``RUNNING``, and ``COMPLETED``:

.. code-block:: bash

    jf -p aws_demo job list
    jf -p aws_demo job output 2

.. dropdown:: Example: atomate2 harmonic phonon workflow (diamond Si)

    To run a multi-job materials workflow such as ``atomate2``'s ``pheasy``
    harmonic phonon workflow with MACE-OMAT-0-medium, install ``atomate2``
    locally to construct the flow, and include CPU PyTorch, ``mace-torch``,
    ``atomate2[pheasy,ase]``, and ``pheasy`` (installed from git as documented
    in ``atomate2``, not from PyPI) in your container image with ``2 vCPU`` and
    ``8192 MiB``, then submit:

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

Step 6: Inspect files, view logs, and cancel jobs
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
CloudShell (replace ``us-east-2`` if you used another region):

.. code-block:: bash

    aws logs tail /aws/batch/jfr-demo --region us-east-2 --since 30m

.. _cloud_cleanup:

Step 7: Tear down AWS resources
===============================

When you are done testing, delete every temporary resource in reverse order
from **CloudShell** so nothing continues to incur storage or log charges. The
block below re-exports the Step 2.1 variables first so it works even in a fresh
CloudShell session (change ``REGION`` if you used a different region):

.. code-block:: bash

    export REGION="us-east-2"
    export ACCT=$(aws sts get-caller-identity --query Account --output text)
    export BUCKET="jfr-demo-${ACCT}"
    export ECR_REPO="jfr-demo"
    export CE_NAME="jfr-demo-ce"
    export QUEUE_NAME="jfr-demo-q"
    export JD_NAME="jfr-demo-jd"
    export LOG_GROUP="/aws/batch/jfr-demo"

    # 1. Disable and delete the job queue
    aws batch update-job-queue --region "$REGION" --job-queue "$QUEUE_NAME" --state DISABLED
    while [ "$(aws batch describe-job-queues --region "$REGION" \
      --job-queues "$QUEUE_NAME" --query 'jobQueues[0].status' --output text)" = "UPDATING" ]; do
      sleep 3
    done
    aws batch delete-job-queue --region "$REGION" --job-queue "$QUEUE_NAME"
    while [ "$(aws batch describe-job-queues --region "$REGION" \
      --job-queues "$QUEUE_NAME" --query 'jobQueues[0].status' --output text 2>/dev/null)" = "DELETING" ]; do
      sleep 3
    done

    # 2. Disable and delete the compute environment
    aws batch update-compute-environment --region "$REGION" --compute-environment "$CE_NAME" --state DISABLED
    while [ "$(aws batch describe-compute-environments --region "$REGION" \
      --compute-environments "$CE_NAME" --query 'computeEnvironments[0].status' --output text)" = "UPDATING" ]; do
      sleep 3
    done
    aws batch delete-compute-environment --region "$REGION" --compute-environment "$CE_NAME"

    # 3. Deregister job definition revisions
    for arn in $(aws batch describe-job-definitions --region "$REGION" \
      --job-definition-name "$JD_NAME" --status ACTIVE --query 'jobDefinitions[].jobDefinitionArn' --output text); do
      aws batch deregister-job-definition --region "$REGION" --job-definition "$arn"
    done

    # 4. Delete CodeBuild project (if created), ECR repository, S3 bucket, and log groups
    aws codebuild delete-project --region "$REGION" --name jfr-demo-build 2>/dev/null || true
    aws ecr delete-repository --region "$REGION" --repository-name "$ECR_REPO" --force
    aws s3 rb "s3://${BUCKET}" --force --region "$REGION"
    aws logs delete-log-group --region "$REGION" --log-group-name "$LOG_GROUP"
    aws logs delete-log-group --region "$REGION" --log-group-name /aws/codebuild/jfr-demo-build 2>/dev/null || true

    # 5. Delete IAM roles and runner IAM user
    aws iam detach-role-policy --role-name jfr-demo-exec-role \
      --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy 2>/dev/null || true
    aws iam delete-role --role-name jfr-demo-exec-role 2>/dev/null || true

    aws iam delete-role-policy --role-name jfr-demo-task-role --policy-name jfr-demo-s3-access 2>/dev/null || true
    aws iam delete-role --role-name jfr-demo-task-role 2>/dev/null || true

    aws iam delete-role-policy --role-name jfr-demo-cb-role --policy-name jfr-demo-cb-access 2>/dev/null || true
    aws iam delete-role --role-name jfr-demo-cb-role 2>/dev/null || true

    POL_ARN="arn:aws:iam::${ACCT}:policy/jfr-demo-runner-policy"
    for k in $(aws iam list-access-keys --user-name jfr-demo-user --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
      aws iam delete-access-key --user-name jfr-demo-user --access-key-id "$k"
    done
    aws iam detach-user-policy --user-name jfr-demo-user --policy-arn "$POL_ARN" 2>/dev/null || true
    aws iam delete-policy --policy-arn "$POL_ARN" 2>/dev/null || true
    aws iam delete-user --user-name jfr-demo-user 2>/dev/null || true

Finally, remove the ``[jfr-demo]`` section from ``~/.aws/credentials`` on your
local machine.
