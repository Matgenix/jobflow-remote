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
   ``jf -fe execution run-cloud``. This command downloads the inputs from S3
   into the job folder on a shared EFS file system, runs the job there, and
   uploads the whole job folder back to S3 when the job ends.
3. When AWS Batch reports the job as finished, the ``Runner`` downloads the
   results from S3 and stores them in your ``JobStore``.

The container never connects to your MongoDB database.

.. warning::

    * Each job runs in its own container. The job folders are on an EFS file
      system that every container mounts at ``/mnt/jfr``. A job folder has the
      same path there as in the S3 bucket, for example
      ``/mnt/jfr/runs/jf_work/29/14/2b/<uuid>_1``. A job can read the folder
      of an earlier job, so flows that pass ``prev_dir`` work as on the
      scratch file system of an HPC center.
    * When a job ends, its whole folder is copied to the S3 bucket. The bucket
      keeps these copies, also after the stack is deleted, like a project file
      system.
    * If AWS stops a container before the job ends, for example when it runs
      out of memory or reaches its time limit, the files stay on EFS but are
      not copied to S3. A new attempt of the job (an AWS Batch retry after a
      Spot reclaim, or ``jf job rerun``) moves them into
      ``previous_attempt_<n>`` in the job folder and copies them to S3 with its
      own files. Step 2 of :ref:`cloud_cleanup` copies all folders on EFS to
      S3.
    * Each jobflow job runs as one AWS Batch job. A GPU job gets a whole GPU.
      Several jobs cannot share one GPU.

The setup has three parts. Part 1 is done once per lab or AWS account. Part 2
chooses where the ``Runner`` runs: your computer or a machine that is always
on. Part 3 is repeated for each campaign.

How to use this guide
=====================

This guide uses two terminals. Every command block says which one to use.

* **Your computer** is the machine where you submit flows and run the
  ``Runner``. It needs Python and access to your MongoDB database.
* Part 2 can move the ``Runner`` to an EC2 instance or to NERSC. Its blocks
  then say **on the instance** or **on a Perlmutter login node**.
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
            pip install "jobflow-remote[aws] @ git+https://github.com/hrushikesh-s/jobflow-remote.git@cloud-worker-docs" "qtoolkit[aws] @ git+https://github.com/hrushikesh-s/qtoolkit.git@aws-batch-api"

    .. tab-item:: uv
        :sync: uv

        .. code-block:: bash

            uv venv ~/.venvs/jfr --python 3.12
            source ~/.venvs/jfr/bin/activate
            uv pip install "jobflow-remote[aws] @ git+https://github.com/hrushikesh-s/jobflow-remote.git@cloud-worker-docs" "qtoolkit[aws] @ git+https://github.com/hrushikesh-s/qtoolkit.git@aws-batch-api"

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
* a private S3 bucket that keeps a copy of every finished job folder
* an EFS file system for the job folders, mounted at ``/mnt/jfr`` in every
  container
* a private ECR repository for your container images
* CloudWatch log groups
* IAM roles for the containers and an IAM user for the ``Runner``
* a CPU queue and an optional GPU queue on EC2 instances, both scaling to
  ``0`` instances when idle
* a CodeBuild project that builds container images
* optionally, an EC2 instance with MongoDB for the ``Runner`` (see
  the **EC2 instance** tab of :ref:`cloud_runner_host`)

When no jobs run, you pay only for the data kept in S3, EFS, ECR, and
CloudWatch Logs. S3 costs about 0.023 USD per GB and month. EFS costs about
0.30 USD per GB and month, and about 0.016 USD for files not read or written
for 30 days. EFS also charges about 0.06 USD per GB written and 0.03 USD per
GB read. While jobs run, you also pay for the EC2 instances and for the public
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
   new worker. If the ``Runner`` runs on the EC2 instance (Part 2), open a
   shell on it as in item 2 of the **EC2 instance** tab of :ref:`cloud_runner_host` and run:

   .. code-block:: bash

       source ~/.venvs/jfr/bin/activate
       MONGO_HOST=127.0.0.1 MONGO_PORT=27017 python make_project.py
       sudo systemctl restart jfr-runner

   If the ``Runner`` runs at NERSC (Part 2), run ``module load conda`` and
   ``conda activate jfr`` on a Perlmutter login node. Then run the block of
   item 3 of the **NERSC Perlmutter** tab of :ref:`cloud_runner_host` and restart the ``Runner``:

   .. code-block:: bash

       scancel --hurry --me -n jfr_runner
       scrontab "$HOME/jfr_runner.scron"

   Cancelling the job disables its ``scrontab`` entry. The second command
   loads the entry again, and ``scrontab`` starts the ``Runner`` within 10
   minutes. Do not run ``jf -p aws_ht admin reset`` here, because it deletes
   the queue.

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
     - ``0``
     - Days after upload when the bucket deletes the job files. ``0`` keeps
       them.
   * - ``DefaultTimeLimit``
     - ``172800``
     - Time limit in seconds (48 hours) of a job that sets no ``time_limit``
   * - ``EnableRunnerInstance``
     - ``false``
     - ``true`` adds an EC2 instance with MongoDB for the ``Runner``
   * - ``RunnerInstanceType``
     - ``t3a.small``
     - Instance type of the ``Runner`` instance
   * - ``RunnerImageId``
     - (empty)
     - Amazon Linux 2023 ``x86_64`` image of the ``Runner`` instance

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

1. On your computer, create the script ``make_project.py``. It reads the
   outputs of the stack and writes ``~/.jfremote/aws_ht.yaml`` with one
   worker per queue you created. It takes the MongoDB settings and the AWS
   profile from environment variables.

   .. code-block:: bash

       cat > make_project.py <<'EOF'
       import os
       from pathlib import Path

       import boto3
       import yaml

       stack = boto3.client("cloudformation").describe_stacks(StackName="jfr-lab")
       out = {o["OutputKey"]: o["OutputValue"] for o in stack["Stacks"][0]["Outputs"]}

       mongo = {"host": os.environ["MONGO_HOST"], "port": int(os.environ["MONGO_PORT"])}
       if os.environ.get("MONGO_USER"):
           mongo["username"] = os.environ["MONGO_USER"]
           mongo["password"] = os.environ["MONGO_PASSWORD"]
       database = os.environ.get("MONGO_DB")


       def store(store_type, default_database, collection):
           return {
               "type": store_type,
               **mongo,
               "database": database or default_database,
               "collection_name": collection,
           }


       aws = {"region_name": out["Region"]}
       if os.environ.get("AWS_PROFILE"):
           aws["profile_name"] = os.environ["AWS_PROFILE"]

       project = {
           "name": "aws_ht",
           "runner": {
               "delay_checkout": 10,
               "delay_check_run_status": 10,
               "delay_advance_status": 10,
           },
           "queue": {"store": store("MongoStore", "jfr_aws_queue", "jobs")},
           "jobstore": {
               "docs_store": store("MongoStore", "jfr_aws_outputs", "outputs"),
               "additional_stores": {
                   "data": store("GridFSStore", "jfr_aws_outputs", "data")
               },
           },
           "workers": {},
       }
       for worker, queue in [("aws_cpu", "Cpu"), ("aws_gpu", "Gpu")]:
           if queue + "JobQueueArn" not in out:
               continue
           project["workers"][worker] = {
               "type": "cloud",
               "scheduler_type": "aws_batch",
               "scheduler_kwargs": {
                   "job_queue": out[queue + "JobQueueArn"],
                   # Without the revision number, AWS Batch uses the latest revision.
                   "job_definition": out[queue + "JobDefinitionArn"].rsplit(":", 1)[0],
                   **aws,
               },
               "container_work_dir": "/mnt/jfr",
               "storage_type": "s3",
               "storage_kwargs": {"bucket": out["S3BucketName"], "prefix": "runs", **aws},
           }

       path = Path.home() / ".jfremote" / "aws_ht.yaml"
       path.parent.mkdir(exist_ok=True)
       path.touch(mode=0o600)
       path.chmod(0o600)
       path.write_text(yaml.safe_dump(project, sort_keys=False))
       print(f"Created {path}")
       EOF

2. On your computer, run the script with the ``Runner`` profile from Step 4
   and a MongoDB on your computer:

   .. code-block:: bash

       MONGO_HOST=127.0.0.1
       MONGO_PORT=27017
       env -u AWS_DEFAULT_REGION AWS_PROFILE=jfr-lab MONGO_HOST="$MONGO_HOST" MONGO_PORT="$MONGO_PORT" python make_project.py

3. On your computer, look at the file and check that MongoDB is reachable:

   .. code-block:: bash

       cat ~/.jfremote/aws_ht.yaml
       python -c "import pymongo; print(pymongo.MongoClient('mongodb://$MONGO_HOST:$MONGO_PORT', serverSelectionTimeoutMS=3000).admin.command('ping'))"

   The second command prints ``{'ok': 1.0}``. If it fails, start MongoDB. If
   your MongoDB runs on another host or port, change ``MONGO_HOST`` and
   ``MONGO_PORT`` in the first two lines of item 2 and run items 2 and 3
   again.

If you skipped Steps 3 and 4, write the file yourself in the same format,
with the values of your own queues, job definitions, and bucket.

The ``runner`` section makes the ``Runner`` check the database and AWS Batch
every 10 s instead of every 30 s, as in the example of the Runner options in
:ref:`projectconf`. Each job of a flow then waits less for the next step of
the ``Runner``.

The workers are ``aws_cpu`` (CPU queue) and ``aws_gpu`` (GPU queue).
``scheduler_kwargs`` are passed to the AWS Batch scheduler
(``qtoolkit.io.api.AWSBatchIO``). ``storage_kwargs`` are passed to the S3
storage (``jobflow_remote.remote.storage.S3Storage``). ``profile_name`` is the
profile from Step 4.

The results go to your ``JobStore``, as on HPC. ``container_work_dir`` makes
the jobs run in their folders on EFS. The bucket keeps a copy of every
finished job folder. ``jf flow delete --files`` deletes the copies of a flow
in the bucket. The folders on EFS stay until the stack is deleted.

Step 6. Check the setup
-----------------------

1. On your computer, run the two commands below. The first checks that
   jobflow-remote can reach MongoDB, S3, and AWS Batch. The second creates the
   queue database.

   .. code-block:: bash

       jf -p aws_ht project check --errors
       jf -p aws_ht admin reset

2. On your computer, create a test script. It submits two flows of two
   small jobs to the CPU queue. In the second flow, the second job reads a
   file from the folder of the first job, which checks the shared folders.
   Its ``pre_run`` installs jobflow-remote in the default Python image of the
   stack when the job starts, so you do not need to build an image for this
   test.

   .. code-block:: bash

       cat > test_aws.py <<'EOF'
       from jobflow import Flow
       from qtoolkit.core.data_objects import QCloudResources

       from jobflow_remote import submit_flow
       from jobflow_remote.config.base import ExecutionConfig
       from jobflow_remote.utils.examples import add, read_file, write_file

       exec_config = ExecutionConfig(
           pre_run=(
               "apt-get -qq update && apt-get -qq install -y git > /dev/null && "
               'pip install -q --no-cache-dir "jobflow-remote[aws] @ git+https://github.com/hrushikesh-s/jobflow-remote.git@cloud-worker-docs" "qtoolkit[aws] @ git+https://github.com/hrushikesh-s/qtoolkit.git@aws-batch-api"'
           )
       )
       resources = QCloudResources(vcpus=1, memory=1800, time_limit=900)

       job1 = add(1, 2)
       job2 = add(job1.output, 3)
       job3 = write_file("shared folders work")
       job4 = read_file(job3.output)
       for flow in (Flow([job1, job2]), Flow([job3, job4])):
           submit_flow(
               flow,
               worker="aws_cpu",
               project="aws_ht",
               exec_config=exec_config,
               resources=resources,
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
               jf -p aws_ht job output 4

       .. tab-item:: uv
           :sync: uv

           .. code-block:: bash

               source ~/.venvs/jfr/bin/activate
               jf -p aws_ht job list
               jf -p aws_ht job output 2
               jf -p aws_ht job output 4

   The first jobs wait 5 to 10 minutes while AWS starts an instance and the
   jobs install jobflow-remote. After about 15 minutes all four jobs are
   ``COMPLETED``. ``job output 2`` prints ``6``, and ``job output 4`` prints
   ``'shared folders work'``. Stop the ``Runner`` with ``Ctrl+C`` in the
   first terminal.

.. _cloud_runner_host:

Part 2: Choosing where the Runner runs
======================================

The ``Runner`` must run as long as flows run. If it stops, AWS Batch finishes
the jobs it already has. No new jobs are submitted and no results are stored
until the ``Runner`` runs again. A computer that sleeps or loses its network
stops the ``Runner``.

You can run the ``Runner`` in one of three places. Choose one and follow only
its tab below. The tabs of Part 3 and of :ref:`cloud_cleanup` follow your
choice.

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Place
     - MongoDB
     - Cost
     - Notes
   * - Your computer
     - On your computer
     - Free
     - Stops when the computer sleeps. Good for tests and short campaigns.
   * - EC2 instance
     - On the instance
     - About 0.019 USD per hour for a ``t3a.small`` in ``us-east-2``, plus
       0.005 USD per hour for its public IPv4 address and 0.08 USD per GB
       and month for its 20 GB disk
     - Runs without your computer. You submit flows on the instance.
   * - NERSC Perlmutter
     - A MongoDB database at NERSC
     - Needs access to the ``workflow`` QOS
     - Runs on a workflow node with ``scrontab``. You submit flows on a
       Perlmutter login node.

Each place has its own MongoDB, so each starts with an empty queue. Let the
flows of the old place finish before you move. Their results stay in the old
MongoDB. Run one ``Runner`` per MongoDB database.

NERSC Spin can also run always-on services, but it needs the SpinUp training
first. This guide does not cover it.

.. tab-set::
    :sync-group: runner

    .. tab-item:: Your computer
        :sync: computer

        1. On your computer, start the ``Runner`` as a daemon:

           .. code-block:: bash

               jf -p aws_ht runner start
               jf -p aws_ht runner status

           Run the last command again until it prints ``Daemon status: running``.
           If ``runner start`` prints ``Daemon process is already running``, a
           ``Runner`` from an earlier session is still running. It may use an
           older environment. Shut it down and start it again:

           .. code-block:: bash

               jf -p aws_ht runner shutdown --wait
               jf -p aws_ht runner start
               jf -p aws_ht runner status

        2. On macOS, keep the computer awake while flows run. Run this in a separate
           terminal and leave it open:

           .. code-block:: bash

               caffeinate -is

           ``-s`` works only while the computer is plugged in. Closing the lid still
           puts a laptop to sleep. Stop ``caffeinate`` with ``Ctrl+C`` when the flows
           are done.

    .. tab-item:: EC2 instance
        :sync: ec2

        The stack can create an instance with MongoDB for the ``Runner``. The
        instance gets its AWS permissions from an IAM role, so it needs no access
        keys. It has no open ports. You open a shell on it from CloudShell with
        Session Manager. Deleting the stack deletes the instance and its MongoDB
        data. Step 1 of :ref:`cloud_cleanup` copies the data to S3 first.

        1. In CloudShell, create ``aws_batch.yaml`` again with item 1 of Part 1,
           Step 3, so that you have the current template. Then add the instance to
           the stack. Update the stack only when no jobs are running.

           .. code-block:: bash

               RUNNER_AMI=$(aws ssm get-parameter \
                 --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
                 --query Parameter.Value --output text)
               aws cloudformation deploy \
                 --template-file aws_batch.yaml \
                 --stack-name jfr-lab \
                 --capabilities CAPABILITY_IAM \
                 --parameter-overrides EnableRunnerInstance=true RunnerImageId="$RUNNER_AMI"

           Run this block only once. Later updates without ``RunnerImageId`` keep
           this image, so they do not replace the instance. Running this block again
           after AWS releases a new image replaces the instance and deletes its
           MongoDB data.

        2. In CloudShell, add the command ``jfr_shell`` to ``~/.bashrc``. It
           opens a shell on the instance as the user ``ec2-user`` with Session
           Manager. CloudShell has the Session Manager plugin already and keeps
           ``~/.bashrc``, so you run this block once.

           .. code-block:: bash

               cat >> ~/.bashrc <<'EOF'
               jfr_shell() {
                 aws ssm start-session --document-name AWS-StartInteractiveCommand \
                   --parameters command="sudo -iu ec2-user" \
                   --target "$(aws cloudformation describe-stacks --stack-name jfr-lab \
                     --query "Stacks[0].Outputs[?OutputKey=='RunnerInstanceId'].OutputValue" --output text)"
               }
               EOF
               source ~/.bashrc

           Then, in CloudShell, open the shell on the instance:

           .. code-block:: bash

               jfr_shell

           It prints ``Starting session with SessionId``, and the prompt changes to
           ``[ec2-user@ip-... ~]$``. If it prints ``TargetNotConnected``, the
           instance is still starting. Wait a minute and run ``jfr_shell`` again.

           The next items run at this prompt. They say **on the instance**. A
           prompt ``~ $`` is CloudShell, not the instance. This is the case in a
           new CloudShell tab, and after the session closes after 20 minutes
           without input. Then run ``jfr_shell`` again. Type ``exit`` to close
           the shell on the instance.

           Closing the shell or CloudShell does not stop anything. The
           ``Runner`` runs as a service on the instance (item 6), and MongoDB
           keeps the queue on the disk of the instance. To check your flows
           later, run ``jfr_shell`` in CloudShell and use the ``jf`` commands.

        3. On the instance, wait until the setup of the instance is done and check
           that MongoDB runs:

           .. code-block:: bash

               cloud-init status --wait
               systemctl is-active mongod

           The commands print ``status: done`` and ``active``.

        4. On the instance, install uv:

           .. code-block:: bash

               curl -LsSf https://astral.sh/uv/install.sh | sh
               export PATH="$HOME/.local/bin:$PATH"

           Then run the **uv** tab of Part 1, Step 1 on the instance. Make
           every later shell on the instance activate the environment:

           .. code-block:: bash

               echo 'source ~/.venvs/jfr/bin/activate' >> ~/.bashrc && source ~/.bashrc

        5. On the instance, create ``make_project.py`` with item 1 of Part 1, Step 5.
           Then create the project file. Without ``AWS_PROFILE``, the ``Runner`` uses
           the IAM role of the instance.

           .. code-block:: bash

               MONGO_HOST=127.0.0.1 MONGO_PORT=27017 python make_project.py
               jf -p aws_ht project check --errors
               jf -p aws_ht admin reset

        6. On the instance, run the ``Runner`` as a service. The service starts the
           ``Runner`` again after a crash or a restart of the instance.

           .. code-block:: bash

               sudo tee /etc/systemd/system/jfr-runner.service > /dev/null <<EOF
               [Unit]
               Description=jobflow-remote Runner of the project aws_ht
               After=network-online.target mongod.service
               Wants=network-online.target

               [Service]
               User=$USER
               ExecStart=$HOME/.venvs/jfr/bin/jf -p aws_ht runner run
               Restart=always
               RestartSec=60

               [Install]
               WantedBy=multi-user.target
               EOF
               sudo systemctl daemon-reload && sudo systemctl enable --now jfr-runner && systemctl is-active jfr-runner

           The last command prints ``active``. To follow the log of the ``Runner``,
           run ``journalctl -u jfr-runner -f`` and stop it with ``Ctrl+C``.

        7. On the instance, create ``test_aws.py`` with item 2 of Part 1, Step 6.
           Then submit the test flows. The service runs them.

           .. code-block:: bash

               python test_aws.py

           Check them with the commands of the **uv** tab of item 4 of Part 1,
           Step 6.

        To read the results on your computer, copy the database to S3 on the
        instance:

        .. code-block:: bash

            BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
            mongodump --gzip --archive=jfr_aws.gz
            aws s3 cp jfr_aws.gz "s3://$BUCKET/mongodump/jfr_aws.gz"

        Then, on your computer, download it and load the outputs into the database
        ``jfr_ec2_outputs`` of your local MongoDB. ``mongorestore`` is part of the
        MongoDB Database Tools.

        .. code-block:: bash

            env -u AWS_DEFAULT_REGION python - <<'EOF'
            import boto3

            session = boto3.Session(profile_name="jfr-lab")
            stack = session.client("cloudformation").describe_stacks(StackName="jfr-lab")
            out = {o["OutputKey"]: o["OutputValue"] for o in stack["Stacks"][0]["Outputs"]}
            session.client("s3").download_file(out["S3BucketName"], "mongodump/jfr_aws.gz", "jfr_aws.gz")
            EOF
            mongorestore --gzip --archive=jfr_aws.gz --nsInclude 'jfr_aws_outputs.*' \
              --nsFrom 'jfr_aws_outputs.*' --nsTo 'jfr_ec2_outputs.*'

    .. tab-item:: NERSC Perlmutter
        :sync: nersc

        The ``Runner`` runs in the ``workflow`` QOS of Perlmutter, which is meant for
        light processes that run for a long time. A job in this QOS can run up to 90
        days on a workflow node. ``scrontab`` starts it again if it ends. If the job
        is cancelled, for example before a NERSC maintenance, Slurm disables the
        entry. Run ``scrontab -e`` and remove the ``#DISABLED`` marks to start it
        again. You need:

        * access to the ``workflow`` QOS. Request it with the form linked in the
          `NERSC docs of the workflow QOS
          <https://docs.nersc.gov/jobs/workflow/workflow-queue/>`_.
        * a MongoDB database at NERSC. Request one with the form linked in the
          `NERSC docs of databases <https://docs.nersc.gov/services/databases/>`_.
          Use a database that no other project uses, because Part 1, Step 6 runs
          ``jf -p aws_ht admin reset`` on it. NERSC gives you its host, database
          name, user, and password. It is reachable from Perlmutter, not from your
          computer or from AWS.

        1. On a Perlmutter login node, load conda:

           .. code-block:: bash

               module load conda

           Then run the **conda** tab of Part 1, Step 1. In every later shell on
           Perlmutter, run ``module load conda`` before ``conda activate jfr``.

        2. In CloudShell, run item 1 of Part 1, Step 4 to create a new access key.
           Then run item 2 of Part 1, Step 4 on a Perlmutter login node.

        3. On a Perlmutter login node, create ``make_project.py`` with item 1 of
           Part 1, Step 5. Then create the project file. Replace the values in
           ``<...>`` with those from NERSC. The block asks for the password of the
           database. The project file keeps it and is readable only by you.

           .. code-block:: bash

               {
               MONGO_HOST=<host>
               MONGO_PORT=27017
               MONGO_DB=<database>
               MONGO_USER=<user>
               printf "MongoDB password: "; read -rs MONGO_PASSWORD; echo
               export MONGO_PASSWORD
               env -u AWS_DEFAULT_REGION AWS_PROFILE=jfr-lab MONGO_HOST="$MONGO_HOST" MONGO_PORT="$MONGO_PORT" \
                 MONGO_DB="$MONGO_DB" MONGO_USER="$MONGO_USER" python make_project.py
               }

           With ``MONGO_DB``, the queue and the outputs share this one database.

        4. On a Perlmutter login node, run items 1 to 4 of Part 1, Step 6. The
           ``Runner`` of item 3 runs on the login node for this short test. Stop it
           with ``Ctrl+C`` when the four jobs are ``COMPLETED``.

        5. On a Perlmutter login node, run the ``Runner`` in the ``workflow`` QOS with
           ``scrontab``. Replace ``<project>`` with your NERSC project. The entry
           starts the ``Runner`` if it is not running, checking every 10 minutes.

           .. code-block:: bash

               NERSC_PROJECT=<project>
               cat > "$HOME/jfr_runner.sh" <<'EOF'
               #!/bin/bash
               module load conda
               conda activate jfr
               exec jf -p aws_ht runner run
               EOF
               chmod +x "$HOME/jfr_runner.sh"
               cat > "$HOME/jfr_runner.scron" <<EOF
               #SCRON -q workflow
               #SCRON -C cron
               #SCRON -A $NERSC_PROJECT
               #SCRON -t 30-00:00:00
               #SCRON --job-name=jfr_runner
               #SCRON --dependency=singleton
               #SCRON -o $HOME/jfr_runner-%j.out
               #SCRON --open-mode=append
               */10 * * * * $HOME/jfr_runner.sh
               EOF
               scrontab -l
               scrontab "$HOME/jfr_runner.scron"
               scrontab -l

           ``scrontab FILE`` replaces all your ``scrontab`` entries. If the first
           ``scrontab -l`` shows other entries, add them to ``jfr_runner.scron``
           first. ``squeue --me -q workflow`` shows the job of the ``Runner``. Its
           log is in ``~/jfr_runner-<jobid>.out``.

Part 3: Running a campaign
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
           pip install "numpy<=2.2" "mace-torch>=0.3.16" \
             "jobflow-remote[aws] @ git+https://github.com/hrushikesh-s/jobflow-remote.git@cloud-worker-docs" \
             "qtoolkit[aws] @ git+https://github.com/hrushikesh-s/qtoolkit.git@aws-batch-api" \
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
    :sync-group: queues

    .. tab-item:: CPU only
        :sync: cpu

        .. code-block:: bash

            ECR_REPO_URI=$(aws cloudformation describe-stacks --stack-name jfr-lab \
              --query "Stacks[0].Outputs[?OutputKey=='EcrRepositoryUri'].OutputValue" --output text)
            aws cloudformation deploy \
              --template-file aws_batch.yaml \
              --stack-name jfr-lab \
              --capabilities CAPABILITY_IAM \
              --parameter-overrides ContainerImage="${ECR_REPO_URI}:mace-cpu-v1"

    .. tab-item:: CPU and GPU
        :sync: gpu

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
``max_retries``. ``time_limit`` works like the walltime of a Slurm job. AWS
Batch stops the job when it is reached. A job without ``time_limit`` gets
``DefaultTimeLimit`` of the stack (48 hours).

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
**CPU only** tab in item 2. The force calculations then also run on
the CPU queue. For a campaign, call ``submit_flow`` once per structure. The
CPU queue then runs the fits of many structures at the same time.

``OMP_NUM_THREADS`` in the resources sets the number of threads of each job to
its vCPUs. Without it, PyTorch in each job starts one thread per core of the
instance, and the jobs that share an instance slow each other down.

1. Where you submit the flows, install ``atomate2`` in the environment from
   Part 1, Step 1. You need it to build the flows. This is your computer, the
   EC2 instance, or a Perlmutter login node, depending on where the
   ``Runner`` runs (Part 2).

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

2. Where you submit the flows, create the submission script:

   .. tab-set::
       :sync-group: queues

       .. tab-item:: CPU only
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

       .. tab-item:: CPU and GPU
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

3. Submit the flow where the ``Runner`` runs (Part 2):

   .. tab-set::
       :sync-group: runner

       .. tab-item:: Your computer
           :sync: computer

           On your computer:

           .. code-block:: bash

               python submit_phonons.py
               jf -p aws_ht runner status

           The last command prints ``Daemon status: running``. If not, start
           the ``Runner`` as in the **Your computer** tab of Part 2.

       .. tab-item:: EC2 instance
           :sync: ec2

           On the instance. The service of Part 2 runs the ``Runner``.

           .. code-block:: bash

               python submit_phonons.py
               systemctl is-active jfr-runner

           The last command prints ``active``.

       .. tab-item:: NERSC Perlmutter
           :sync: nersc

           On a Perlmutter login node. ``scrontab`` runs the ``Runner``.

           .. code-block:: bash

               python submit_phonons.py
               squeue --me -q workflow

           The last command shows the job ``jfr_runner``.

AWS Batch starts EC2 instances when jobs are queued and terminates them a few
minutes after the queues are empty.

Inspecting and stopping jobs
============================

Run the ``jf`` commands of this section where you submit the flows: on your
computer, on the EC2 instance, or on a Perlmutter login node (Part 2). Run
the ``aws`` commands in CloudShell. In a new terminal, activate the
environment first:

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

Each job has its own folder:

* While the job runs, the folder is on EFS under ``/mnt/jfr/runs/``. Only
  the containers mount EFS.
* When the job ends, the whole folder is copied to S3 under
  ``s3://<bucket>/runs/``. Until then, S3 has only ``jfremote_in.json`` and
  ``jfremote_out.json`` of the job.
* If AWS stops the container before the job ends, the files stay on EFS
  (see the warning at the top of this guide).
* The bucket keeps the folders after the stack is deleted, unless you set
  ``RunFilesRetentionDays`` in Part 1, Step 3.

List the jobs of a flow, their folders on EFS and in S3, and the files in
S3. Run it where you submit the flows. Replace ``<flow_id>`` with the flow id
from ``jf -p aws_ht flow list``.

.. code-block:: bash

    FLOW_ID=<flow_id> python - <<'EOF'
    import os

    from jobflow_remote import JobController

    jc = JobController.from_project_name("aws_ht")
    jobs = jc.get_jobs_info(flow_ids=os.environ["FLOW_ID"])
    for info in sorted(jobs, key=lambda info: int(info.db_id)):
        print(info.db_id, info.name, info.state.value)
        if not info.run_dir:
            continue
        worker = jc.project.workers[info.worker]
        storage = worker.get_storage()
        uri = storage.get_uri(info.run_dir)
        print("  EFS:", f"{worker.container_work_dir}/{uri.split('/', 3)[3]}")
        print("  S3: ", uri)
        for name in sorted(storage.listdir(info.run_dir)):
            print("   ", name)
    EOF

List the files of one job in S3, or download one file into the current
folder:

.. code-block:: bash

    jf -p aws_ht job files ls <db_id>
    jf -p aws_ht job files get <db_id> jfremote_out.json

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
in Part 1, Step 2. If a job asks for more vCPUs or memory than any instance
of the queue has, the queue cancels it after 30 minutes in ``RUNNABLE``, and
the job fails.

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

Download the whole folder of a job from S3 to your computer:

1. Where you submit the flows, show the folder of the job:

   .. code-block:: bash

       jf -p aws_ht job files ls <db_id>

   The first line ends with the folder, for example
   ``/jf_work/c7/97/bc/<uuid>_1``.

2. On your computer, set ``RUN_DIR`` to this folder and download it into the
   current folder. This needs only the ``jfr-lab`` profile of Part 1, Step 4,
   so it works wherever the ``Runner`` runs.

   .. code-block:: bash

       RUN_DIR=<run_dir>
       env -u AWS_DEFAULT_REGION RUN_DIR="$RUN_DIR" python - <<'EOF'
       import os
       from pathlib import PurePosixPath

       import boto3

       from jobflow_remote.remote.storage import S3Storage

       session = boto3.Session(profile_name="jfr-lab")
       stack = session.client("cloudformation").describe_stacks(StackName="jfr-lab")
       bucket = next(
           o["OutputValue"]
           for o in stack["Stacks"][0]["Outputs"]
           if o["OutputKey"] == "S3BucketName"
       )
       name = PurePosixPath(os.environ["RUN_DIR"]).name
       storage = S3Storage(bucket=bucket, prefix="runs", profile_name="jfr-lab")
       storage.download_dir(os.environ["RUN_DIR"], name)
       print("Downloaded to", name)
       EOF

Download the folders you need before step 3 of :ref:`cloud_cleanup`, which
deletes the access keys of the ``jfr-lab`` profile.

The containers write stdout and stderr to CloudWatch Logs, not to
``queue.out`` and ``queue.err``. For this reason ``jf job queue-out`` does not
work for cloud workers. ``jf job todir`` does not work either, because there
is no shell access to the container. Find the AWS Batch job id of a job in
``remote.process_id`` of ``jf -p aws_ht job info <db_id>``. Then, in
CloudShell, show its state and the logs of all its attempts. AWS Batch keeps
this information for 7 days after the job ends.

.. code-block:: bash

    BATCH_JOB=<process_id>
    aws batch describe-jobs --jobs "$BATCH_JOB" --query 'jobs[0].[status,statusReason]' --output text
    for S in $(aws batch describe-jobs --jobs "$BATCH_JOB" --query 'jobs[0].[attempts[].container.logStreamName, container.logStreamName][]' --output text | tr '\t' '\n' | grep -v None | sort -u); do aws logs tail /aws/batch/jfr-lab --log-stream-names "$S" --since 7d; done

.. _cloud_cleanup:

Deleting the AWS stack
======================

Deleting the stack deletes the EFS file system with all job folders on it.
The S3 bucket and its copies of the job folders are kept.

1. Stop the jobs that are still queued or running, then stop the
   ``Runner``. Use the tab of the place where the ``Runner`` runs (Part 2).

   .. tab-set::
       :sync-group: runner

       .. tab-item:: Your computer
           :sync: computer

           On your computer:

           .. code-block:: bash

               jf -p aws_ht job stop -s SUBMITTED -s RUNNING
               jf -p aws_ht runner shutdown --wait
               jf -p aws_ht runner status

           The last command prints ``Daemon status: shut_down``.

       .. tab-item:: EC2 instance
           :sync: ec2

           On the instance (item 2 of the **EC2 instance** tab of :ref:`cloud_runner_host`). The last
           three commands copy the MongoDB data to the S3 bucket, because
           deleting the stack deletes the instance. To load the outputs into
           your local MongoDB, run the download block at the end of
           the **EC2 instance** tab of :ref:`cloud_runner_host` on your computer before step 3.

           .. code-block:: bash

               source ~/.venvs/jfr/bin/activate
               jf -p aws_ht job stop -s SUBMITTED -s RUNNING
               sudo systemctl disable --now jfr-runner
               BUCKET=$(aws cloudformation describe-stacks --stack-name jfr-lab \
                 --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
               mongodump --gzip --archive=jfr_aws.gz
               aws s3 cp jfr_aws.gz "s3://$BUCKET/mongodump/jfr_aws.gz"

       .. tab-item:: NERSC Perlmutter
           :sync: nersc

           On a Perlmutter login node. ``scrontab -r`` removes all your
           ``scrontab`` entries. If you have others, write them without the
           ``jfr_runner`` lines to a file and load it with ``scrontab FILE``
           instead.

           .. code-block:: bash

               module load conda
               conda activate jfr
               jf -p aws_ht job stop -s SUBMITTED -s RUNNING
               scrontab -r
               scancel --hurry --me -n jfr_runner

2. In CloudShell, copy every job folder on EFS to S3. This also copies the
   folders of jobs whose container was stopped before the job ended. The
   command runs one AWS Batch job on the CPU queue that installs
   jobflow-remote if the image does not have it.

   .. code-block:: bash

       OUT() { aws cloudformation describe-stacks --stack-name jfr-lab --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
       CMD="from jobflow_remote.remote.storage import S3Storage; S3Storage(bucket='$(OUT S3BucketName)', prefix='runs', region_name='$(OUT Region)').upload_dir('/mnt/jfr/runs', '.')"
       OVERRIDES=$(python3 -c 'import json, sys; print(json.dumps({"command": ["sh", "-c", "apt-get -qq update && apt-get -qq install -y git > /dev/null && pip install -q --no-cache-dir \"jobflow-remote[aws] @ git+https://github.com/hrushikesh-s/jobflow-remote.git@cloud-worker-docs\" \"qtoolkit[aws] @ git+https://github.com/hrushikesh-s/qtoolkit.git@aws-batch-api\" && python -c \"$0\"", sys.argv[1]]}))' "$CMD")
       COPY_JOB=$(aws batch submit-job --job-name copy-job-folders --job-queue "$(OUT CpuJobQueueArn)" --job-definition "$(OUT CpuJobDefinitionArn)" --container-overrides "$OVERRIDES" --query jobId --output text)
       STATUS=SUBMITTED
       while [ "$STATUS" != SUCCEEDED ] && [ "$STATUS" != FAILED ]; do sleep 20; STATUS=$(aws batch describe-jobs --jobs "$COPY_JOB" --query 'jobs[0].status' --output text); echo "$STATUS"; done

   The last line is ``SUCCEEDED`` when all folders are in S3.

3. In CloudShell, delete the access keys of the ``Runner`` user and delete
   the stack. CloudFormation deletes all resources of the stack except the
   S3 bucket. The first command prints the name of the bucket. Note it.

   .. code-block:: bash

       aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text
       RUNNER_USER=$(aws cloudformation describe-stacks --stack-name jfr-lab \
         --query "Stacks[0].Outputs[?OutputKey=='RunnerUserName'].OutputValue" --output text) && \
       for k in $(aws iam list-access-keys --user-name "$RUNNER_USER" \
         --query 'AccessKeyMetadata[].AccessKeyId' --output text); do
         aws iam delete-access-key --user-name "$RUNNER_USER" --access-key-id "$k"
       done && \
       aws cloudformation delete-stack --stack-name jfr-lab && \
       aws cloudformation wait stack-delete-complete --stack-name jfr-lab

4. On your computer, and on Perlmutter if you used it, remove the profile
   ``jfr-lab`` from ``~/.aws/credentials`` and ``~/.aws/config``:

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

The bucket stays and costs about 0.023 USD per GB and month. To delete it
with all job folders in it, run in CloudShell with the name from step 3:

.. code-block:: bash

    aws s3 rb s3://<bucket> --force
