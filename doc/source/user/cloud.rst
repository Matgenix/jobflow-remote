.. _cloud:

======================
Running on Cloud (AWS)
======================

A ``cloud`` worker runs jobflow-remote jobs on AWS Batch. Job files move
through an Amazon S3 bucket. No SSH connection and no shared filesystem are
needed.

How a cloud worker runs a job
=============================

1. The ``Runner`` uploads ``jfremote_in.json`` to the S3 bucket and submits an
   AWS Batch job.
2. The AWS Batch job starts a container that runs
   ``jf -fe execution run-cloud``. This command downloads the job folder from
   S3, runs the job, and uploads ``jfremote_out.json`` and the job store files
   back to S3.
3. When AWS Batch reports the job as finished, the ``Runner`` downloads the
   results from S3 and stores them in your ``JobStore``.

The container never connects to your MongoDB database.

.. warning::

    * Each job runs in its own container, in a temporary folder that is
      deleted when the job ends. A job cannot read files from the run folder
      of an earlier job. Flows that pass ``prev_dir`` between jobs, such as
      the ``atomate2`` VASP relax and static chains, do not work on cloud
      workers. Flows that pass data only through job outputs, such as the
      force-field phonon workflow in :ref:`cloud_submit`, work as on HPC.
    * Each jobflow job runs as one AWS Batch job. A GPU job gets a whole GPU.
      Several jobs cannot share one GPU yet.

The setup has two parts. Part 1 is done once per lab or AWS account. Part 2
is repeated for each campaign.

How to use this guide
=====================

This guide uses two terminals. Every command block says which one to use.

* **Your computer** is the machine where you submit flows and run the
  ``Runner``. It needs Python and access to your MongoDB database.
* **AWS CloudShell** is a terminal inside the AWS Console in your web
  browser. Its AWS CLI is already signed in as your AWS Console user. This
  user needs permission to create IAM roles and CloudFormation stacks (for
  example the ``AdministratorAccess`` policy).

To open CloudShell:

1. Go to https://console.aws.amazon.com/ and sign in.
2. In the region menu at the top right of the page, choose your region (for
   example **US East (Ohio)**, ``us-east-2``). Use the same region for the
   whole guide.
3. Click **CloudShell** in the toolbar at the bottom left of the page. You
   can also type ``CloudShell`` in the search box at the top of the page.
   A terminal opens at the bottom of the page.

Blocks that start with ``cat > FILE <<'EOF'`` create the file ``FILE``. Copy
the whole block with the copy button at its top right, paste it into the
terminal, and press Enter. CloudShell may ask you to confirm a paste of
several lines. Click **Paste**.

Part 1: One-time setup
======================

Step 1. Install jobflow-remote
------------------------------

On your computer, create a Python environment and install jobflow-remote with
the ``aws`` extra. Use conda or uv:

.. tab-set::
    :sync-group: env

    .. tab-item:: conda
        :sync: conda

        .. code-block:: bash

            conda create -n jfr python=3.12 -y
            conda activate jfr
            pip install "jobflow-remote[aws]"

    .. tab-item:: uv
        :sync: uv

        .. code-block:: bash

            uv venv ~/.venvs/jfr --python 3.12
            source ~/.venvs/jfr/bin/activate
            uv pip install "jobflow-remote[aws]"

Every command on your computer from here on runs in this environment. When
you open a new terminal on your computer, activate it first:

.. tab-set::
    :sync-group: env

    .. tab-item:: conda
        :sync: conda

        .. code-block:: bash

            conda activate jfr

    .. tab-item:: uv
        :sync: uv

        .. code-block:: bash

            source ~/.venvs/jfr/bin/activate

If you gave the environment another name or path, use that instead of ``jfr``
in these commands.

You also need a MongoDB database for the queue and the ``JobStore``, as for
any jobflow-remote project. A local MongoDB on ``127.0.0.1:27017`` is enough
for testing.

Step 2. Check your EC2 quotas
-----------------------------

AWS accounts on the Free plan can start only Free Tier instance types, so
jobs stay ``RUNNABLE``. If your account is on the Free plan, upgrade it in the
AWS Console at https://console.aws.amazon.com/billing/home#/freetier/upgrade.
The remaining
credits are kept. If the upgrade asks for a spend limit, leave the optional
cost controls unchecked. Close to the limit, they block new instances or pause
running ones, which stops AWS Batch jobs.

In CloudShell, show the vCPU quotas that the queues use:

.. code-block:: bash

    for code in L-34B43A08 L-1216C47A L-3819A6DF L-DB2E81BA; do
      aws service-quotas get-service-quota --service-code ec2 --quota-code "$code" \
        --query 'Quota.{Name:QuotaName,Value:Value}' --output table
    done

The quotas count vCPUs:

* ``L-34B43A08`` is for Spot standard instances, which the CPU queue uses by
  default (``CpuAllocationType=SPOT``).
* ``L-1216C47A`` is for On-Demand standard instances
  (``CpuAllocationType=EC2``).
* ``L-3819A6DF`` is for Spot GPU instances, which the GPU queue uses by
  default (``GpuAllocationType=SPOT``). It must be at least ``4`` for one
  ``g4dn.xlarge``, ``g5.xlarge``, or ``g6.xlarge`` instance.
* ``L-DB2E81BA`` is for On-Demand GPU instances (``GpuAllocationType=EC2``).

Step 3 sets the maximum vCPUs of each queue to its quota. A job with
``vcpus=1`` uses 1 vCPU of the quota, so a quota of ``64`` runs up to 64 such
jobs at once.

New accounts often start with small quotas and with GPU quotas of ``0``. In
CloudShell, ask for 64 vCPUs of Spot standard instances and 4 vCPUs of Spot
GPU instances:

.. code-block:: bash

    aws service-quotas request-service-quota-increase --service-code ec2 --quota-code L-34B43A08 --desired-value 64
    aws service-quotas request-service-quota-increase --service-code ec2 --quota-code L-3819A6DF --desired-value 4

Check the requests later in CloudShell:

.. code-block:: bash

    aws service-quotas list-requested-service-quota-change-history --service-code ec2 --query 'RequestedQuotas[].[QuotaCode,DesiredValue,Status]' --output table

Skip a request if the quota is already at least that value. If a request
for the same quota is still open, the command for that quota fails with
``ResourceAlreadyExistsException``. AWS answers within a few days. Wait for
``APPROVED`` before you add the GPU queue in Step 3.

Step 3. Deploy the AWS stack
----------------------------

Skip Steps 3 and 4 if you already have your own AWS Batch queues and S3
bucket. The container role (``jobRoleArn`` of the job definition) then needs
``s3:ListBucket`` and ``s3:GetBucketLocation`` on the bucket, and
``s3:GetObject`` and ``s3:PutObject`` on its objects. The credentials of the
``Runner`` need the same S3 actions plus ``s3:DeleteObject``, and the AWS
Batch actions ``batch:SubmitJob``, ``batch:DescribeJobs``, ``batch:ListJobs``,
and ``batch:TerminateJob``.

The CloudFormation template below creates one stack with:

* a VPC without a NAT Gateway
* a private S3 bucket for job files, which deletes them after
  ``RunFilesRetentionDays`` days
* a private ECR repository for your container images
* CloudWatch log groups
* IAM roles for the containers and an IAM user for the ``Runner``
* a CPU queue and an optional GPU queue on EC2 instances, both scaling to
  ``0`` instances when idle
* a CodeBuild project that builds container images

When no jobs run, you pay only for the data kept in S3, ECR, and CloudWatch
Logs. While jobs run, you also pay for the EC2 instances and for the public
IPv4 address of each. The instances carry the tag ``Project=jfr-lab``. To see
their cost in Cost Explorer, activate this tag under **Billing and Cost
Management**, **Cost allocation tags**, after the first job ran. The tag
appears there up to 24 hours after its first use, and the activation takes up
to 24 hours more. To include earlier costs, choose **Backfill tags** on the
same page.

1. In CloudShell, create the template file ``aws_batch.yaml``:

   .. dropdown:: Template (click to show, then use the copy button)

       .. literalinclude:: /_static/aws_batch.yaml
          :language: yaml
          :prepend: cat > aws_batch.yaml <<'EOF'
          :append: EOF

2. In CloudShell, deploy the stack:

   .. code-block:: bash

       aws iam create-service-linked-role --aws-service-name batch.amazonaws.com 2>/dev/null || true
       aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com 2>/dev/null || true
       aws iam create-service-linked-role --aws-service-name spot.amazonaws.com 2>/dev/null || true

       CPU_QUOTA=$(aws service-quotas get-service-quota --service-code ec2 \
         --quota-code L-34B43A08 --query 'Quota.Value' --output text)
       aws cloudformation deploy \
         --template-file aws_batch.yaml \
         --stack-name jfr-lab \
         --capabilities CAPABILITY_IAM \
         --parameter-overrides CpuMaxvCpus="${CPU_QUOTA%.*}"

   The deployment takes a few minutes. The CPU queue then uses up to your
   Spot standard quota of vCPUs. ``echo $CPU_QUOTA`` must show at least ``2``,
   the vCPUs of a ``c7i.large``. If it shows less, wait until the request
   from Step 2 is ``APPROVED``. Run this block again after every approved
   increase, so that the queue can use it. For On-Demand instances, use the
   quota code ``L-1216C47A`` and add ``CpuAllocationType=EC2`` to
   ``--parameter-overrides``.

3. In CloudShell, add the GPU queue once your Spot GPU quota is at least
   ``4``. Parameters you do not pass keep their current values.

   .. code-block:: bash

       GPU_QUOTA=$(aws service-quotas get-service-quota --service-code ec2 \
         --quota-code L-3819A6DF --query 'Quota.Value' --output text)
       aws cloudformation deploy \
         --template-file aws_batch.yaml \
         --stack-name jfr-lab \
         --capabilities CAPABILITY_IAM \
         --parameter-overrides EnableGpuQueue=true GpuMaxvCpus="${GPU_QUOTA%.*}"

   For On-Demand GPU instances, use the quota code ``L-DB2E81BA`` and add
   ``GpuAllocationType=EC2``. Then run Step 5 again, so that the project file
   gets the ``aws_gpu`` worker. If the ``Runner`` is running, restart it on
   your computer with ``jf -p aws_ht runner restart``, so that it reads the
   new worker.

Update the stack only when no jobs are running. A change of the instance
settings replaces the instances of a queue.

The main parameters of the template are:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Default
     - Meaning
   * - ``CpuAllocationType``
     - ``SPOT``
     - ``SPOT`` or ``EC2`` (On-Demand) instances for the CPU queue
   * - ``CpuInstanceTypes``
     - ``c7i,c6i,c7a,m7i``
     - EC2 instance families for the CPU queue
   * - ``CpuMaxvCpus``
     - ``16``
     - Maximum vCPUs used at once by the CPU queue
   * - ``EnableGpuQueue``
     - ``false``
     - Create the GPU queue
   * - ``GpuAllocationType``
     - ``SPOT``
     - ``SPOT`` or ``EC2`` (On-Demand) instances for the GPU queue
   * - ``GpuInstanceTypes``
     - ``g4dn,g5,g6``
     - EC2 instance families for the GPU queue
   * - ``GpuMaxvCpus``
     - ``4``
     - Maximum vCPUs used at once by the GPU queue
   * - ``RunFilesRetentionDays``
     - ``30``
     - Days after upload when the bucket deletes the job files

Step 4. Create the Runner credentials
-------------------------------------

1. In CloudShell, create an access key for the ``Runner`` IAM user of the
   stack:

   .. code-block:: bash

       RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text)
       aws iam create-access-key --user-name "$RUNNER_USER" \
         --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
       aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='Region'].OutputValue" --output text

   It prints the access key ID and the secret access key, then the region of
   the stack. The secret key is shown only once.

2. On your computer, save the key and the region in the profile ``jfr-lab``.
   The commands ask for the three values from the previous step. The secret
   key is not shown while you type or paste it. The profile also makes the
   ``Runner`` retry AWS requests that fail for a short time. If the profile
   exists already, it is replaced.

   .. code-block:: bash

       {
       printf "AccessKeyId: "; read -r AK
       printf "SecretAccessKey: "; read -rs SK; echo
       printf "Region: "; read -r REGION
       AK="$AK" SK="$SK" REGION="$REGION" python - <<'EOF'
       import os
       from pathlib import Path


       def set_section(path, header, lines):
           """Write a section of an AWS file and drop an older one with the same header."""
           old = path.read_text().splitlines() if path.exists() else []
           kept, skip = [], False
           for line in old:
               if line.lstrip().startswith("["):
                   skip = line.strip() == header
               if not skip:
                   kept.append(line)
           text = "\n".join(kept).strip()
           section = "\n".join([header, *lines])
           path.write_text(f"{text}\n\n{section}\n" if text else f"{section}\n")
           path.chmod(0o600)


       aws = Path.home() / ".aws"
       aws.mkdir(exist_ok=True)
       set_section(
           aws / "credentials",
           "[jfr-lab]",
           [
               f"aws_access_key_id = {os.environ['AK']}",
               f"aws_secret_access_key = {os.environ['SK']}",
           ],
       )
       set_section(
           aws / "config",
           "[profile jfr-lab]",
           [f"region = {os.environ['REGION']}", "retry_mode = adaptive", "max_attempts = 10"],
       )
       print("Saved the profile jfr-lab in ~/.aws/credentials and ~/.aws/config")
       EOF
       }

.. _cloud_configure_worker:

Step 5. Create the project file
-------------------------------

1. On your computer, run the block below. It reads the outputs of the stack
   with the ``Runner`` profile from Step 4. It then writes
   ``~/.jfremote/aws_ht.yaml`` with one worker per queue you created.

   .. code-block:: bash

       MONGO_HOST=127.0.0.1
       MONGO_PORT=27017
       env -u AWS_DEFAULT_REGION AWS_PROFILE=jfr-lab MONGO_HOST="$MONGO_HOST" MONGO_PORT="$MONGO_PORT" python - <<'EOF'
       import os
       from pathlib import Path

       import boto3

       stack = boto3.client("cloudformation").describe_stacks(StackName="jfr-lab")
       out = {o["OutputKey"]: o["OutputValue"] for o in stack["Stacks"][0]["Outputs"]}

       host = os.environ["MONGO_HOST"]
       port = os.environ["MONGO_PORT"]
       text = f"""name: aws_ht
       queue:
         store:
           type: MongoStore
           host: {host}
           port: {port}
           database: jfr_aws_queue
           collection_name: jobs
       jobstore:
         docs_store:
           type: MongoStore
           host: {host}
           port: {port}
           database: jfr_aws_outputs
           collection_name: outputs
         additional_stores:
           data:
             type: GridFSStore
             host: {host}
             port: {port}
             database: jfr_aws_outputs
             collection_name: data
       workers:
       """
       region = out["Region"]
       bucket = out["S3BucketName"]
       for worker, queue in [("aws_cpu", "Cpu"), ("aws_gpu", "Gpu")]:
           if queue + "JobQueueArn" not in out:
               continue
           job_queue = out[queue + "JobQueueArn"]
           # Without the revision number, AWS Batch uses the latest revision.
           job_definition = out[queue + "JobDefinitionArn"].rsplit(":", 1)[0]
           text += f"""  {worker}:
           type: cloud
           scheduler_type: aws_batch
           scheduler_kwargs:
             job_queue: {job_queue}
             job_definition: {job_definition}
             region_name: {region}
             profile_name: jfr-lab
           storage_type: s3
           storage_kwargs:
             bucket: {bucket}
             prefix: runs
             region_name: {region}
             profile_name: jfr-lab
       """

       path = Path.home() / ".jfremote" / "aws_ht.yaml"
       path.parent.mkdir(exist_ok=True)
       path.write_text(text)
       print(f"Created {path}")
       EOF

2. On your computer, look at the file and check that MongoDB is reachable:

   .. code-block:: bash

       cat ~/.jfremote/aws_ht.yaml
       python -c "import pymongo; print(pymongo.MongoClient('mongodb://$MONGO_HOST:$MONGO_PORT', serverSelectionTimeoutMS=3000).admin.command('ping'))"

   The second command prints ``{'ok': 1.0}``. If it fails, start MongoDB. If
   your MongoDB runs on another host or port, change ``MONGO_HOST`` and
   ``MONGO_PORT`` in the first two lines of item 1 and run both items again.

If you skipped Steps 3 and 4, write the file yourself in the same format,
with the values of your own queues, job definitions, and bucket.

The workers are ``aws_cpu`` (CPU queue) and ``aws_gpu`` (GPU queue). ``scheduler_kwargs`` are passed to
the AWS Batch scheduler (``qtoolkit.io.api.AWSBatchIO``). ``storage_kwargs``
are passed to the S3 storage (``jobflow_remote.remote.storage.S3Storage``).
``profile_name`` is the profile from Step 4.

The results go to your ``JobStore``, as on HPC. The bucket of the stack
deletes the job files ``RunFilesRetentionDays`` days (default ``30``) after
they were uploaded. ``jf job files`` and ``jf job retry`` need these files, so
use them within that time. ``jf flow delete --files`` deletes the files of a
flow earlier.

Step 6. Check the setup
-----------------------

1. On your computer, run the two commands below. The first checks that
   jobflow-remote can reach MongoDB, S3, and AWS Batch. The second creates the
   queue database.

   .. code-block:: bash

       jf -p aws_ht project check --errors
       jf -p aws_ht admin reset

2. On your computer, create a test script. It runs two small jobs on the
   CPU queue. Its ``pre_run`` installs jobflow-remote in the default
   Python image of the stack when the job starts, so you do not need to build
   an image for this test.

   .. code-block:: bash

       cat > test_aws.py <<'EOF'
       from jobflow import Flow
       from qtoolkit.core.data_objects import QCloudResources

       from jobflow_remote import submit_flow
       from jobflow_remote.config.base import ExecutionConfig
       from jobflow_remote.utils.examples import add

       job1 = add(1, 2)
       job2 = add(job1.output, 3)
       submit_flow(
           Flow([job1, job2]),
           worker="aws_cpu",
           project="aws_ht",
           exec_config=ExecutionConfig(
               pre_run='pip install -q --no-cache-dir "jobflow-remote[aws]"'
           ),
           resources=QCloudResources(vcpus=1, memory=1800, time_limit=600),
       )
       EOF

3. On your computer, submit the flow and start the ``Runner``:

   .. code-block:: bash

       python test_aws.py
       jf -p aws_ht runner run

4. On your computer, open a second terminal. Activate the environment and
   check the jobs:

   .. tab-set::
       :sync-group: env

       .. tab-item:: conda
           :sync: conda

           .. code-block:: bash

               conda activate jfr
               jf -p aws_ht job list
               jf -p aws_ht job output 2

       .. tab-item:: uv
           :sync: uv

           .. code-block:: bash

               source ~/.venvs/jfr/bin/activate
               jf -p aws_ht job list
               jf -p aws_ht job output 2

   The first job waits 5 to 10 minutes while AWS starts an instance and the job
   installs jobflow-remote. After about 15 minutes both jobs are
   ``COMPLETED`` and the second command prints ``6``. Stop the ``Runner`` with ``Ctrl+C`` in the first terminal.

Part 2: Running a campaign
==========================

.. _cloud_build_image:

Step 1. Build a container image
-------------------------------

Every Python function that a job runs must be importable in the container
image. Build one image per software stack. The CodeBuild project of the stack
builds the image and pushes it to the ECR repository of the stack, so you do
not need Docker.

The same ``Dockerfile`` builds a CPU image and a GPU image. Only the PyTorch
wheels differ. The CPU queue uses the CPU image, which is several GB smaller
than the GPU image. Give every build a new tag, for example ``mace-cpu-v2``
after a change. The instances keep using their cached copy of a tag they have
pulled before.

1. In CloudShell, create the ``Dockerfile``. This example installs PyTorch,
   MACE, ``atomate2``, and ``pheasy`` for force-field phonon calculations. It
   also downloads the MACE-OMAT-0 model during the build, so jobs do not
   download it.

   .. code-block:: bash

       cat > Dockerfile <<'EOF'
       FROM public.ecr.aws/docker/library/python:3.12-slim

       ARG TORCH_INDEX=https://download.pytorch.org/whl/cpu

       ENV PIP_NO_CACHE_DIR=1 \
           PYTHONDONTWRITEBYTECODE=1 \
           PYTHONUNBUFFERED=1

       RUN apt-get update && apt-get install -y --no-install-recommends git gcc g++ && rm -rf /var/lib/apt/lists/*
       RUN pip install --upgrade pip && \
           pip install --index-url "$TORCH_INDEX" torch && \
           pip install "numpy<=2.2" "mace-torch>=0.3.16" "jobflow-remote[aws]" \
             "atomate2[pheasy,ase]" "f90nml>=1.3.1" "h5py>=3.6.0" "numba>=0.58.1" \
             "scikit-learn>=1.0.2" "spglib>=1.16.3" && \
           pip install --no-deps "pheasy @ git+https://gitlab.com/hpsahasrabuddhe/pheasy.git@9f24162a4ed0f0ab8911d382fd2617944aade55d"
       RUN python -c "from mace.calculators import mace_mp; mace_mp(model='medium-omat-0', device='cpu')"

       WORKDIR /tmp/jf_run
       EOF

2. In CloudShell, build the image and push it to ECR. Build the CPU image
   first. Build the GPU image only if you use the GPU queue.

   .. tab-set::

       .. tab-item:: CPU image

           .. code-block:: bash

               IMAGE_TAG="mace-cpu-v1"
               TORCH_INDEX="https://download.pytorch.org/whl/cpu"
               BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
                 --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
               CB_PROJ=$(aws cloudformation describe-stacks --stack-name jfr-lab \
                 --query "Stacks[0].Outputs[?OutputKey=='CodeBuildProjectName'].OutputValue" --output text)

               zip -q context.zip Dockerfile
               aws s3 cp context.zip "s3://${BUCKET}/build/context.zip"

               BUILD_ID=$(aws codebuild start-build --project-name "$CB_PROJ" \
                 --environment-variables-override \
                   "name=IMAGE_TAG,value=${IMAGE_TAG},type=PLAINTEXT" \
                   "name=TORCH_INDEX,value=${TORCH_INDEX},type=PLAINTEXT" \
                 --query 'build.id' --output text)
               STATUS=IN_PROGRESS
               while [ "$STATUS" = IN_PROGRESS ]; do sleep 20; STATUS=$(aws codebuild batch-get-builds --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text); echo "$STATUS"; done

       .. tab-item:: GPU image

           .. code-block:: bash

               IMAGE_TAG="mace-gpu-v1"
               TORCH_INDEX="https://download.pytorch.org/whl/cu124"
               BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
                 --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
               CB_PROJ=$(aws cloudformation describe-stacks --stack-name jfr-lab \
                 --query "Stacks[0].Outputs[?OutputKey=='CodeBuildProjectName'].OutputValue" --output text)

               zip -q context.zip Dockerfile
               aws s3 cp context.zip "s3://${BUCKET}/build/context.zip"

               BUILD_ID=$(aws codebuild start-build --project-name "$CB_PROJ" \
                 --environment-variables-override \
                   "name=IMAGE_TAG,value=${IMAGE_TAG},type=PLAINTEXT" \
                   "name=TORCH_INDEX,value=${TORCH_INDEX},type=PLAINTEXT" \
                 --query 'build.id' --output text)
               STATUS=IN_PROGRESS
               while [ "$STATUS" = IN_PROGRESS ]; do sleep 20; STATUS=$(aws codebuild batch-get-builds --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text); echo "$STATUS"; done

   The loop prints the build status every 20 seconds. The build takes 10 to
   20 minutes. The last line is ``SUCCEEDED`` when the image is in ECR. If it
   is ``FAILED``, type ``CodeBuild`` in the search box of the AWS Console
   and open the build to read its log.

Step 2. Point the job definitions at the image
----------------------------------------------

In CloudShell, in the folder with ``aws_batch.yaml`` from Part 1, Step 3,
update the stack so that the job definitions use the new image. Parameters
you do not pass keep their current values.

.. tab-set::

    .. tab-item:: CPU image

        .. code-block:: bash

            ECR_REPO_URI=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)
            aws cloudformation deploy \
              --template-file aws_batch.yaml \
              --stack-name jfr-lab \
              --capabilities CAPABILITY_IAM \
              --parameter-overrides ContainerImage="${ECR_REPO_URI}:mace-cpu-v1"

    .. tab-item:: CPU and GPU images

        .. code-block:: bash

            ECR_REPO_URI=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)
            aws cloudformation deploy \
              --template-file aws_batch.yaml \
              --stack-name jfr-lab \
              --capabilities CAPABILITY_IAM \
              --parameter-overrides \
                ContainerImage="${ECR_REPO_URI}:mace-cpu-v1" \
                GpuContainerImage="${ECR_REPO_URI}:mace-gpu-v1"

Change the tags if you built other ones in Step 1.

The update creates a new revision of each job definition whose image changed.
The workers in ``aws_ht.yaml`` use the latest revision, so you do not need to
change the file.

To use several images on the same queues, register one job definition per
image. Copy a job definition of the stack and change only the image. The copy
then keeps the job role, the log settings, and the retry rule. Without the
job role of the stack,
the container cannot read or write the S3 bucket. Then point one worker per
image at its job definition. You can also override the job definition for
single jobs with
``QCloudResources(..., scheduler_kwargs={"jobDefinition": "<arn>"})``. The
``scheduler_kwargs`` of ``QCloudResources`` are fields of the AWS Batch
``SubmitJob`` request. They are different from the ``scheduler_kwargs`` of
the worker in ``aws_ht.yaml``.

.. _cloud_resources:

Step 3. Choose the resources of each job
----------------------------------------

Each job gets its own ``QCloudResources``. The main fields are ``vcpus``,
``memory`` (MiB), ``gpus``, ``time_limit`` (seconds, at least ``60``), and
``max_retries``.

The job definitions of the stack rerun a job up to two times when AWS
stops its instance, for example when a Spot instance is reclaimed. Other
failures, such as a timeout or an error in the job, are not rerun. Setting
``max_retries`` replaces this rule for that job. AWS Batch then reruns every
failure except a timeout, up to ``max_retries`` times.

The two queues place jobs differently:

.. list-table::
   :header-rows: 1

   * - Worker
     - Where a job runs
     - Typical use
   * - ``aws_cpu`` (CPU)
     - An EC2 instance that can be shared with other jobs. AWS Batch runs
       several jobs on one instance when its free vCPUs and memory allow.
       The instance keeps the image cached between jobs.
     - Many small CPU jobs, for example ``pheasy`` fits with 1 vCPU each
   * - ``aws_gpu`` (GPU)
     - An EC2 GPU instance. A job with ``gpus=1`` gets one whole GPU.
     - MLIP force calculations

Jobs cannot use all of the instance memory. The operating system and the ECS
agent use part of it. Ask for a little less than the full memory per vCPU so
that jobs fill the instance. ``c7i``, ``c6i``, and ``c7a`` instances have
2 GiB per vCPU, so the CPU job definition of the stack uses ``memory=1800``
with ``vcpus=1``.

Use ``submit_flow(..., resources=...)`` to set the resources of all jobs in a
flow. Use ``set_run_config`` to give jobs with a given name another worker or
other resources. The example in :ref:`cloud_submit` uses both.

.. _cloud_submit:

Step 4. Submit the flows
------------------------

The example below runs the ``atomate2`` force-field phonon workflow with
MACE-OMAT-0 and ``pheasy``. ``force_field_name="MACE-MP-0"`` selects the MACE
calculator, and the ``model`` key selects MACE-OMAT-0. The task documents
still record ``MLFF.MACE_MP_0`` as the force field name. ``default_dtype`` is
``float64`` so that the small force differences between displaced supercells
stay accurate. It is slower than ``float32``, most of all on GPUs. With
``socket=True``, one job computes the forces of all displaced supercells of a
structure and loads the model once. The ``pheasy`` fit
(``generate_frequencies_eigenvectors``) and the short setup steps run on the
CPU queue with 1 vCPU each. The force calculations run on the GPU queue. If your GPU quota is ``0``, use the
**Without GPU queue** tab in item 2. The force calculations then also run on
the CPU queue. For a campaign, call ``submit_flow`` once per structure. The
CPU queue then runs the fits of many structures at the same time.

``OMP_NUM_THREADS`` in the resources sets the number of threads of each job to
its vCPUs. Without it, PyTorch in each job starts one thread per core of the
instance, and the jobs that share an instance slow each other down.

1. On your computer, install ``atomate2`` in the environment from Part 1,
   Step 1. You need it to build the flows.

   .. tab-set::
       :sync-group: env

       .. tab-item:: conda
           :sync: conda

           .. code-block:: bash

               conda activate jfr
               pip install "atomate2[pheasy]"

       .. tab-item:: uv
           :sync: uv

           .. code-block:: bash

               source ~/.venvs/jfr/bin/activate
               uv pip install "atomate2[pheasy]"

2. On your computer, create the submission script:

   .. tab-set::
       :sync-group: gpu

       .. tab-item:: With GPU queue
           :sync: gpu

           .. code-block:: bash

               cat > submit_phonons.py <<'EOF'
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

               mace_kwargs = {"model": "medium-omat-0", "device": "cuda", "default_dtype": "float64"}
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
                   socket=True,
               )
               flow = maker.make(si_prim)

               cpu_resources = QCloudResources(
                   vcpus=1, memory=1800, time_limit=1800, environment={"OMP_NUM_THREADS": "1"}
               )
               for name in (
                   "structure_to_primitive",
                   "get_supercell_size",
                   "generate_phonon_displacements",
                   "generate_frequencies_eigenvectors",
               ):
                   set_run_config(
                       flow, name_filter=name, worker="aws_cpu", resources=cpu_resources
                   )

               submit_flow(
                   flow,
                   worker="aws_gpu",
                   project="aws_ht",
                   resources=QCloudResources(
                       vcpus=4,
                       memory=14000,
                       gpus=1,
                       time_limit=1800,
                       environment={"OMP_NUM_THREADS": "4"},
                   ),
               )
               EOF

           Keep ``run_phonon_displacements`` on ``aws_gpu``, because the force
           job it creates runs on the same worker as this step.

       .. tab-item:: Without GPU queue
           :sync: cpu

           .. code-block:: bash

               cat > submit_phonons.py <<'EOF'
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

               mace_kwargs = {"model": "medium-omat-0", "device": "cpu", "default_dtype": "float64"}
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
                   socket=True,
               )
               flow = maker.make(si_prim)

               cpu_resources = QCloudResources(
                   vcpus=1, memory=1800, time_limit=1800, environment={"OMP_NUM_THREADS": "1"}
               )
               for name in (
                   "structure_to_primitive",
                   "get_supercell_size",
                   "generate_phonon_displacements",
                   "generate_frequencies_eigenvectors",
               ):
                   set_run_config(
                       flow, name_filter=name, worker="aws_cpu", resources=cpu_resources
                   )

               submit_flow(
                   flow,
                   worker="aws_cpu",
                   project="aws_ht",
                   resources=QCloudResources(
                       vcpus=2, memory=3600, time_limit=3600, environment={"OMP_NUM_THREADS": "2"}
                   ),
               )
               EOF

           All jobs run on ``aws_cpu``. The relax, the static, and the
           force job of the displaced supercells get 2 vCPUs each.

3. On your computer, submit the flow and start the ``Runner`` as a daemon:

   .. code-block:: bash

       python submit_phonons.py
       jf -p aws_ht runner start

AWS Batch starts EC2 instances when jobs are queued and terminates them a few
minutes after the queues are empty.

Inspecting and stopping jobs
============================

Run the ``jf`` commands of this section in a terminal on your computer. Run
the ``aws`` commands in CloudShell. In a new terminal on your computer,
activate the environment first:

.. tab-set::
    :sync-group: env

    .. tab-item:: conda
        :sync: conda

        .. code-block:: bash

            conda activate jfr

    .. tab-item:: uv
        :sync: uv

        .. code-block:: bash

            source ~/.venvs/jfr/bin/activate

List the flows and the jobs:

.. code-block:: bash

    jf -p aws_ht flow list
    jf -p aws_ht job list

List or download the files of a job from S3. The second command saves the
file in the current folder.

.. code-block:: bash

    jf -p aws_ht job files ls 1
    jf -p aws_ht job files get 1 jfremote_out.json

A job stays ``SUBMITTED`` until its container starts. The first job on a
new instance waits a few minutes while the instance starts and pulls the
image. In CloudShell, see the AWS Batch state of the jobs in the CPU queue.
For the GPU queue, replace ``CpuJobQueueArn`` with ``GpuJobQueueArn``.

.. code-block:: bash

    QUEUE=$(aws cloudformation describe-stacks --stack-name jfr-lab --query "Stacks[0].Outputs[?OutputKey=='CpuJobQueueArn'].OutputValue" --output text)
    for s in RUNNABLE STARTING RUNNING SUCCEEDED FAILED; do aws batch list-jobs --job-queue "$QUEUE" --job-status "$s" --query 'jobSummaryList[].[jobName,status,statusReason]' --output text; done

``RUNNABLE`` means the job waits for an instance. If a job stays
``RUNNABLE`` for more than 15 minutes, read in CloudShell why AWS could
not start an instance:

.. code-block:: bash

    aws autoscaling describe-scaling-activities --max-items 5 --query 'Activities[].[StartTime,AutoScalingGroupName,StatusCode,StatusMessage]' --output table

The list covers every Auto Scaling group in the region, including the ones
that AWS Batch creates for the queues. A message about the vCPU limit means
that the quota is too low. A message that the instance type is not eligible
for Free Tier means that the account is on the Free plan. Both are explained
in Part 1, Step 2.

A job goes to ``REMOTE_ERROR`` when an action of the ``Runner`` on AWS fails
several times in a row, for example while AWS Batch or S3 does not answer.
Run the failed action again with the job's DB id:

.. code-block:: bash

    jf -p aws_ht job retry <db_id>

Stop a queued or running job:

.. code-block:: bash

    jf -p aws_ht job stop <job_id>

Delete a flow and its jobs. Use the flow id from ``jf -p aws_ht flow list``.
The command also cancels the AWS Batch jobs of the flow that are still queued
or running.

.. code-block:: bash

    jf -p aws_ht flow delete -fid <flow_id>

The containers write stdout and stderr to CloudWatch Logs, not to
``queue.out`` and ``queue.err``. For this reason ``jf job queue-out`` does not
work for cloud workers. ``jf job todir`` does not work either, because there
is no shell access to the container. In CloudShell, read the logs of the last
30 minutes:

.. code-block:: bash

    aws logs tail /aws/batch/jfr-lab --since 30m

.. _cloud_cleanup:

Deleting the AWS stack
======================

1. On your computer, stop the jobs that are still queued or running, then
   stop the ``Runner``. A job that still runs would write new files to the
   bucket after it was emptied, and the stack could not be deleted.

   .. code-block:: bash

       jf -p aws_ht job stop -s SUBMITTED -s RUNNING
       jf -p aws_ht runner stop
       jf -p aws_ht runner status

   Run the last command again until it shows that the ``Runner`` is shut down.

2. In CloudShell, empty the S3 bucket, delete the access keys of the
   ``Runner`` user, and delete the stack. CloudFormation then deletes all
   other resources of the stack.

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

3. On your computer, remove the profile ``jfr-lab`` from ``~/.aws/credentials``
   and ``~/.aws/config``:

   .. code-block:: bash

       python - <<'EOF'
       from pathlib import Path

       for name, header in [("credentials", "[jfr-lab]"), ("config", "[profile jfr-lab]")]:
           path = Path.home() / ".aws" / name
           if not path.exists():
               continue
           kept, skip = [], False
           for line in path.read_text().splitlines():
               if line.lstrip().startswith("["):
                   skip = line.strip() == header
               if not skip:
                   kept.append(line)
           path.write_text("\n".join(kept).strip() + "\n")
       EOF
