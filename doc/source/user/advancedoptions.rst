.. _advancedoptions:

****************
Advanced options
****************

In this section we illustrate some options and settings that may require additional
configurations, more care in using and dealing with related issues, or that are
still considered experimental and may subject to major changes in the future.

Limiting the number of submitted Jobs
=====================================

Due to the limitations from the computing center, or simply to avoid having too many
jobs in the queue, it may be convenient to limit the number of jobs that are simultaneously
submitted to the worker. This limit can be enforced by setting the ``max_jobs`` value
for the worker in the configuration file.

To avoid frequent queries to the database and requests to the worker's queue manager,
jobflow-remote keeps track internally of the number of jobs that are actually running and
only at higher time intervals will validate the internal count with the actual number
of jobs submitted to the worker's queue. This may cause slight delays in submitted jobs,
in case some jobs have finished running, but their state has not been updated in the
database yet.

OTP-based login
===============

.. warning::
  This functionality should be considered as experimental and may be subject to changes
  in the future.

In order to enforce security, some computing centers enforce ssh connections that
require providing a password or a generated one-time password (OTP) interactively. Since
jobflow-remote needs to use ssh connections in order to interact with remote workers,
a different approach is required in handling the runner in this case. While this
procedure should not expose any sensitive information, you should consider if it goes
against the security policies of the computing center you are working with.

In order to deal with this kind of configuration, first set up the worker that
requires an OTP in the standard way, as shown in the :ref:`projectconf worker` section.
Then, set the option ``interactive_login: true`` in the worker configuration.
This will signal to the system that interaction from the user is required during the
opening of the connection.

In addition to the ``interactive_login`` option, there are two different use cases
that should be handled separately by the user in the configuration:

#. The connection requires **ssh key+OTP**: the ssh key **should be defined explicitly**
   either in the ``~/.ssh/config`` file that defines the connection to the host, or
   in the configuration of the worker (using the ``key_filename`` option).
#. The connection requires **password+OTP**: no ssh key should be present in the
   configuration files.

.. warning::

    The rest of the procedure is independent of which option is being used, but it
    is crucial to properly define the options for the ssh key, depending on the use case.

When starting the runner, the daemon should also be told that the user needs to
input some information during the connection. The runner should thus be started with::

    jf runner start -s -i

Unlike the standard procedure, the command will not immediately daemonize (i.e., go to the background), and instead will
wait for the runner to start to give access to the I/O of the daemon process.
At this point the runner will prompt whichever request for interactive login is coming from the worker's server.
Once all the requests have been answered and the connection achieved, you can leave the interactive
mode with a keyboard interrupt (e.g., ``CTRL+C`` or ``CMD+C`` depending on your OS).

Limitations
-----------

Given the strict connection requirements, this approach comes with some limitations:

* The user will always need to provide an OTP when starting the Runner or whenever running
  commands that imply the connection to the worker.
* The Runner will not be able reconnect if the connection drops, so the connection should be stable.
* If the connection is killed the Runner should be restarted (some computing centres may automatically kill a connection after a given time period).
* The Runner can only run with a single process, not in the split mode.


Batch submission
================

.. warning::
  This functionality should be considered as experimental and may be subject to changes
  in the future.

The standard approach in jobflow-remote uses a single job in the worker (e.g. a SLURM job)
to execute a single jobflow Job. However, when working in a cluster with long queues, it may
be convenient to execute several Jobs in a single submitted job. To do this jobflow-remote
allows you to define special *batch* workers that will keep executing Jobs until some
conditions are met.

More explicitly, a Job that is assigned to a *batch* worker follows the standard procedure
until the ``UPLOADED`` state, but instead of having a single job submitted to the queue
and going in the ``SUBMITTED`` state a file will be added to a specific folder in the
file system of the worker and go in the ``BATCH_SUBMITTED`` state. This folder will act
as a pool of Jobs to be executed by jobs submitted to the worker queue. This
preserves the requirement of avoiding direct connections to the queue database from the
worker process.

Once the Job has been executed a file will be created in a different folder, that will
signal the runner that the Job has ``TERMINATED``. From this point onward the processing
of the Job by the runner proceeds in the standard way.

In order to define a *batch* worker the ``batch`` section for that worker should be filled
in the configuration file. In particular the ``jobs_handle_dir`` and ``work_dir`` should be
defined. These should represent paths in the file system of the workers that will be used
to manage the remote jobs. In addition the ``max_jobs`` option of the worker should be set.
This will define the maximum number of batch jobs simultaneously submitted to the queue.
An minimal configuration for a *batch* worker would thus be:

.. code-block:: yaml

    worker_name:
      scheduler_type: slurm
      work_dir: /home/guido/software/python/test_jfr/
      host: hpc_host
      max_jobs: 5
      batch:
        jobs_handle_dir: /remote/path/jfr_handle_dir
        work_dir: /remote/path/jfr_batch_jobs

See the :ref:`projectconf` section for the list of all the configuration options available
in the ``batch`` section.

Note that, since the completion of a Job and the subsequent potential switch of ``WAITING``
Jobs to their ``READY`` state is still managed by the runner, this functionality is effective
if several Jobs and Flows are managed at the same time. If too much time passes between the
end of one Job and the availability of a new one the *batch* job in the queue will stop.

.. warning::

    The ``batch`` section of a worker's configuration also has a ``max_jobs`` option.
    It allows for the definition of the maximum number of jobflow Jobs that will be executed in a single
    process submitted to the queue (e.g. a SLURM job). This should not be confused with
    the ``max_jobs`` value mentioned above, that defines the number of submitted *batch*
    processes (e.g. the maximum number of SLURM Jobs simultaneously in the queue).
