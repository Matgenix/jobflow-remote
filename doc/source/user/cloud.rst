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

Activate this environment (``conda activate jfr`` or
``source ~/.venvs/jfr/bin/activate``) in every terminal on your computer that
you use in this guide.

You also need a MongoDB database for the queue and the ``JobStore``, as for
any jobflow-remote project. A local MongoDB on ``127.0.0.1:27017`` is enough
for testing.

Step 2. Check your EC2 quotas
-----------------------------

Skip this step if you only use the Fargate CPU queue.

In CloudShell, run:

.. code-block:: bash

    for code in L-1216C47A L-3819A6DF L-DB2E81BA; do
      aws service-quotas get-service-quota --service-code ec2 --quota-code "$code" \
        --query 'Quota.{Name:QuotaName,Value:Value}' --output table
    done

The quotas count vCPUs:

* ``L-1216C47A`` is for On-Demand standard instances, which the EC2 CPU queue
  uses. New accounts often start at ``5``. Raise it to the value of
  ``Ec2CpuMaxvCpus`` (default ``64``) to run that many 1-vCPU jobs at once.
* ``L-3819A6DF`` is for Spot GPU instances, which the GPU queue uses by
  default (``GpuAllocationType=SPOT``). It must be at least ``4`` for one
  ``g4dn.xlarge`` or ``g5.xlarge`` instance.
* ``L-DB2E81BA`` is for On-Demand GPU instances (``GpuAllocationType=EC2``).

New accounts often start the GPU quotas at ``0``. To raise a quota, type
``Service Quotas`` in the search box at the top of the AWS Console, open
**Amazon Elastic Compute Cloud (Amazon EC2)**, search for the quota code, and
click **Request increase at account level**. Wait for approval before you
enable the GPU queue.

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
* a private S3 bucket for job files
* a private ECR repository for your container images
* CloudWatch log groups
* IAM roles for the containers and an IAM user for the ``Runner``
* a Fargate CPU queue
* an optional EC2 CPU queue and an optional EC2 GPU queue, both scaling to
  ``0`` instances when idle
* a CodeBuild project that builds container images

When no jobs run, you pay only for the data kept in S3, ECR, and CloudWatch
Logs. While jobs run, you also pay for the Fargate tasks and EC2 instances,
and for the public IPv4 address of each.

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

       aws cloudformation deploy \
         --template-file aws_batch.yaml \
         --stack-name jfr-lab \
         --capabilities CAPABILITY_IAM \
         --parameter-overrides EnableEc2CpuQueue=true EnableGpuQueue=true

   Remove ``EnableGpuQueue=true`` if your Spot GPU quota is still ``0``.
   Remove ``EnableEc2CpuQueue=true`` if you do not need the EC2 CPU queue.
   Leave out the whole ``--parameter-overrides`` line to deploy only the
   Fargate CPU queue. You can enable a queue later by running the same command
   with that parameter. The deployment takes a few minutes.

The main parameters of the template are:

.. list-table::
   :header-rows: 1

   * - Parameter
     - Default
     - Meaning
   * - ``CpuMaxvCpus``
     - ``8``
     - Maximum vCPUs used at once by the Fargate CPU queue
   * - ``EnableEc2CpuQueue``
     - ``false``
     - Create the EC2 CPU queue
   * - ``Ec2CpuInstanceTypes``
     - ``c7i``
     - EC2 instance families for the EC2 CPU queue
   * - ``Ec2CpuMaxvCpus``
     - ``64``
     - Maximum vCPUs used at once by the EC2 CPU queue
   * - ``EnableGpuQueue``
     - ``false``
     - Create the EC2 GPU queue
   * - ``GpuAllocationType``
     - ``SPOT``
     - ``SPOT`` or ``EC2`` (On-Demand) instances for the GPU queue
   * - ``GpuInstanceTypes``
     - ``g4dn,g5,g6``
     - EC2 instance families for the GPU queue
   * - ``GpuMaxvCpus``
     - ``16``
     - Maximum vCPUs used at once by the GPU queue

Step 4. Create the Runner credentials
-------------------------------------

1. In CloudShell, create an access key for the ``Runner`` IAM user of the
   stack:

   .. code-block:: bash

       RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text)
       aws iam create-access-key --user-name "$RUNNER_USER" \
         --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text

   It prints the access key ID and the secret access key. The secret key is
   shown only once.

2. On your computer, save the key in the profile ``jfr-lab`` of
   ``~/.aws/credentials``. The commands ask for the two values from the
   previous step. The secret key is not shown while you type or paste it.

   .. code-block:: bash

       mkdir -p ~/.aws
       printf "AccessKeyId: "; read -r AK
       printf "SecretAccessKey: "; read -rs SK; echo
       printf "\n[jfr-lab]\naws_access_key_id = %s\naws_secret_access_key = %s\n" "$AK" "$SK" >> ~/.aws/credentials
       chmod 600 ~/.aws/credentials

.. _cloud_configure_worker:

Step 5. Create the project file
-------------------------------

1. In CloudShell, print the command that creates the project file. It fills
   in the bucket, queues, and job definitions of your stack, and adds one
   worker per queue you created.

   .. code-block:: bash

       OUTPUTS=$(aws cloudformation describe-stacks --stack-name jfr-lab --query 'Stacks[0].Outputs' --output json)
       out() { echo "$OUTPUTS" | jq -r --arg k "$1" '.[] | select(.OutputKey == $k) | .OutputValue'; }
       worker() {
         [ -n "$(out "$2JobQueueArn")" ] || return 0
         jd=$(out "$2JobDefinitionArn")
         printf '  %s:\n    type: cloud\n    scheduler_type: aws_batch\n' "$1"
         printf '    scheduler_kwargs:\n      job_queue: %s\n      job_definition: %s\n' "$(out "$2JobQueueArn")" "${jd%:*}"
         printf '      region_name: %s\n      profile_name: jfr-lab\n' "$(out Region)"
         printf '    storage_type: s3\n    storage_kwargs:\n      bucket: %s\n' "$(out S3BucketName)"
         printf '      prefix: runs\n      region_name: %s\n      profile_name: jfr-lab\n' "$(out Region)"
       }
       echo "mkdir -p ~/.jfremote && cat > ~/.jfremote/aws_ht.yaml <<'EOF'"
       cat <<'TEMPLATE'
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
       TEMPLATE
       worker aws_cpu Cpu
       worker aws_cpu_ec2 Ec2Cpu
       worker aws_gpu Gpu
       echo "EOF"

2. Select all printed lines, from ``mkdir -p ~/.jfremote`` to the last
   ``EOF``, and copy them. On your computer, paste them into the terminal and
   press Enter. This creates ``~/.jfremote/aws_ht.yaml``.

3. If your MongoDB is not on ``127.0.0.1:27017``, open the file
   (``vi ~/.jfremote/aws_ht.yaml``) and change the ``queue`` and ``jobstore``
   settings.

The workers are ``aws_cpu`` (Fargate CPU queue), ``aws_cpu_ec2`` (EC2 CPU
queue), and ``aws_gpu`` (EC2 GPU queue). ``scheduler_kwargs`` are passed to
the AWS Batch scheduler (``qtoolkit.io.api.AWSBatchIO``). ``storage_kwargs``
are passed to the S3 storage (``jobflow_remote.remote.storage.S3Storage``).
``profile_name`` is the profile from Step 4. ``job_definition`` has no
revision number, so AWS Batch always uses the latest revision.

The job files stay in the S3 bucket until you delete them, for example with
``jf flow delete --files``. The results go to your ``JobStore``, as on HPC.

Step 6. Check the setup
-----------------------

1. On your computer, run the two commands below. The first checks that
   jobflow-remote can reach MongoDB, S3, and AWS Batch. The second creates the
   queue database.

   .. code-block:: bash

       jf -p aws_ht project check --errors
       jf -p aws_ht admin reset

2. On your computer, create a test script. It runs two small jobs on the
   Fargate CPU queue. Its ``pre_run`` installs jobflow-remote in the default
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
           resources=QCloudResources(vcpus=1, memory=2048, time_limit=300),
       )
       EOF

3. On your computer, submit the flow and start the ``Runner``:

   .. code-block:: bash

       python test_aws.py
       jf -p aws_ht runner run

4. On your computer, open a second terminal, activate the environment, and
   check the jobs:

   .. code-block:: bash

       jf -p aws_ht job list
       jf -p aws_ht job output 2

   After a few minutes both jobs are ``COMPLETED`` and the second command
   prints ``6``. Stop the ``Runner`` with ``Ctrl+C`` in the first terminal.

Part 2: Running a campaign
==========================

.. _cloud_build_image:

Step 1. Build a container image
-------------------------------

Every Python function that a job runs must be importable in the container
image. Build one image per software stack and give each image its own tag
(for example ``mace-gpu-v1``). All images go into the ECR repository of the
stack. The CodeBuild project of the stack builds the image, so you do not
need Docker.

1. In CloudShell, create the ``Dockerfile``. This example installs CUDA
   PyTorch, MACE, ``atomate2``, and ``pheasy`` for force-field phonon
   calculations.

   .. code-block:: bash

       cat > Dockerfile <<'EOF'
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

2. In CloudShell, build the image and push it to ECR:

   .. code-block:: bash

       IMAGE_TAG="mace-gpu-v1"
       BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
       CB_PROJ=$(aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='CodeBuildProjectName'].OutputValue" --output text)

       zip -q context.zip Dockerfile
       aws s3 cp context.zip "s3://${BUCKET}/build/context.zip"

       BUILD_ID=$(aws codebuild start-build --project-name "$CB_PROJ" \
         --environment-variables-override "name=IMAGE_TAG,value=${IMAGE_TAG},type=PLAINTEXT" \
         --query 'build.id' --output text)
       while [ "$(aws codebuild batch-get-builds --ids "$BUILD_ID" \
         --query 'builds[0].buildStatus' --output text)" = "IN_PROGRESS" ]; do
         sleep 10
       done
       aws codebuild batch-get-builds --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text

   The last command prints ``SUCCEEDED`` when the image is in ECR. If it
   prints ``FAILED``, type ``CodeBuild`` in the search box of the AWS Console
   and open the build to read its log.

Step 2. Point the job definitions at the image
----------------------------------------------

In CloudShell, in the folder with ``aws_batch.yaml`` from Part 1, Step 3,
update the stack so that the job definitions use the new image. Parameters
you do not pass keep their current values.

.. code-block:: bash

    ECR_REPO_URI=$(aws cloudformation describe-stacks --stack-name jfr-lab \
      --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)
    IMAGE_TAG="mace-gpu-v1"  # the tag you built in Step 1

    aws cloudformation deploy \
      --template-file aws_batch.yaml \
      --stack-name jfr-lab \
      --capabilities CAPABILITY_IAM \
      --parameter-overrides \
        ContainerImage="${ECR_REPO_URI}:${IMAGE_TAG}" \
        GpuContainerImage="${ECR_REPO_URI}:${IMAGE_TAG}"

The update creates a new revision of each job definition. The workers in
``aws_ht.yaml`` use the latest revision, so you do not need to change the
file.

To use several images on the same queues, register one job definition per
image. Copy a job definition of the stack and change only the image. The copy
then keeps the job role and the log settings, and for Fargate also the
execution role and the public IP setting. Without the job role of the stack,
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
``max_retries`` (how many times AWS Batch reruns a failed attempt, for
example after a Spot interruption).

The three queues place jobs differently:

.. list-table::
   :header-rows: 1

   * - Worker
     - Where a job runs
     - Typical use
   * - ``aws_cpu`` (Fargate)
     - Its own Fargate task. ``vcpus`` and ``memory`` must be one of the
       `Fargate combinations <https://docs.aws.amazon.com/batch/latest/APIReference/API_ResourceRequirement.html>`_.
     - Small numbers of CPU jobs, quick tests
   * - ``aws_cpu_ec2`` (EC2 CPU)
     - An EC2 instance that can be shared with other jobs. AWS Batch runs
       several jobs on one instance when its free vCPUs and memory allow.
       The instance keeps the image cached between jobs.
     - Many small CPU jobs, for example ``pheasy`` fits with 1 vCPU each
   * - ``aws_gpu`` (EC2 GPU)
     - An EC2 GPU instance. A job with ``gpus=1`` gets one whole GPU.
     - MLIP force calculations

On the EC2 queues, jobs cannot use all of the instance memory. The operating
system and the ECS agent use part of it. Ask for a little less than the full
memory per vCPU so that jobs fill the instance. ``c7i`` instances have 2 GiB
per vCPU, so the EC2 CPU job definition of the stack uses ``memory=1800`` with
``vcpus=1``.

Use ``submit_flow(..., resources=...)`` to set the resources of all jobs in a
flow. Use ``set_run_config`` to give jobs with a given name another worker or
other resources. The example in :ref:`cloud_submit` uses both.

.. _cloud_submit:

Step 4. Submit the flows
------------------------

The example below runs the ``atomate2`` force-field phonon workflow with
MACE-OMAT-0 and ``pheasy``. The force calculations run on the GPU queue. The
``pheasy`` fit (``generate_frequencies_eigenvectors``) and the
``structure_to_primitive`` step run on the EC2 CPU queue with 1 vCPU each.
For a campaign, call ``submit_flow`` once per structure. The EC2 CPU queue
then runs the fits of many structures at the same time.

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

       cpu_resources = QCloudResources(vcpus=1, memory=1800, time_limit=1800)
       for name in ("structure_to_primitive", "generate_frequencies_eigenvectors"):
           set_run_config(
               flow, name_filter=name, worker="aws_cpu_ec2", resources=cpu_resources
           )

       submit_flow(
           flow,
           worker="aws_gpu",
           project="aws_ht",
           resources=QCloudResources(
               vcpus=4, memory=14000, gpus=1, max_retries=2, time_limit=1800
           ),
       )
       EOF

   Keep ``run_phonon_displacements`` on ``aws_gpu``, because the force jobs it
   creates run on the same worker as this step.

3. On your computer, submit the flow and start the ``Runner`` as a daemon:

   .. code-block:: bash

       python submit_phonons.py
       jf -p aws_ht runner start

AWS Batch starts EC2 instances when jobs are queued and terminates them a few
minutes after the queues are empty.

Inspecting and stopping jobs
============================

On your computer, list or download the files of a job from S3:

.. code-block:: bash

    jf -p aws_ht job files ls 1
    jf -p aws_ht job files get 1 jfremote_out.json

On your computer, stop a queued or running job:

.. code-block:: bash

    jf -p aws_ht job stop <job_id>

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

1. On your computer, stop the ``Runner``:

   .. code-block:: bash

       jf -p aws_ht runner stop

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

3. On your computer, open ``~/.aws/credentials`` (for example with
   ``vi ~/.aws/credentials``) and delete the ``[jfr-lab]`` section.
