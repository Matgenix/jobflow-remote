.. _errors:

.. These roles are to color the text of the error states. From the jobflow_remote.css
.. role:: red
.. role:: darkorange

*******************
Dealing with errors
*******************

Given the several operations required to execute a Job, it is normal that
errors may arise. In particular errors may happen during the managing of the
Job by the runner, or inside the Job itself. In this section we explain
how to distinguish the different kind of errors, how to get information
about them and possibly how to fix them.

Types of errors
===============

When a Job fails to complete, the first task is to understand the source
of the error. In jobflow-remote the errors can fall mainly into two
categories:

* **Remote errors**: errors happening while the Runner deals with a Job.
  These include, for example, errors copying the file to and from a worker,
  or interacting with the queue manager.
* **Job errors**: errors during the execution of a Job on the worker.
  E.g. failures of the code being executed or bad inputs provided by the user.

Each of these kind of errors may have different kind of messages and solutions.

.. note::

    In some cases the distinction between the two types of errors may be less clear.
    An error related to the code execution may lead, for example, to a missing output
    file. In this case the system will just highlight that an output file is missing.
    A more in-depth analysis of the outputs should reveal that the source of the error
    is indeed in the executed calculation.

.. _remoteerrors:

Remote errors
=============

If a problem arises during the processing of the Job, the runner will typically retry
multiple times before setting the Job to the error state. This is because it is expected that
some of these failures will be temporary or transient. For example, a minor network issue preventing
the output files to be downloaded or an overload of the worker's queueing system that
may not respond in time. By default the code will retry 3 times at increasing time
intervals. After that the Job state will be set to ``REMOTE_ERROR``.

.. note::

    In some cases the runner may be able to determine that the error will not be solved
    even retrying at a later time and will directly set the state to ``REMOTE_ERROR``

Even before the Job has been set to the ``REMOTE_ERROR`` state, it is possible to
know that an error appeared in the Job and that will be retried after some time because
its state will be highlighted in orange when running ``jf job list``. For example

.. parsed-literal::

    ┏━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name      ┃ State       ┃ Job id  (Index)                           ┃ Worker    ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 1     │ job 1     │ :darkorange:`CHECKED_OUT` │ dc398a00-61b3-43e5-b4c5-f9c2ef8713b5  (1) │ worker1   │ 2024-02-27 15:16   │
    │ 2     │ job 2     │ WAITING     │ 3b2fdeae-750b-4b77-9395-971a88545f3e  (1) │ worker1   │ 2024-02-27 15:16   │
    └───────┴───────────┴─────────────┴───────────────────────────────────────────┴───────────┴────────────────────┘

It is also possible to visualize the time at which it will be retried using the ``-v`` option,
i.e. ``jf job list -v``.

.. note::

    The state reported in `jf job list` corresponds to the state in which the Job
    currently is. This means that in the example above the Job already reached the
    ``CHECKED_OUT`` state and the error was during the process of switching to **next**
    state. In this case during the procedure of uploading the files.

At this point there might be already some actions that can be taken. Running
``jf job info 1`` will show, among the other information, an error message or the stack
trace of the error in the ``remote.error`` field. For example::

    ╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ created_on = '2024-02-27 15:16'                                                                                                                           │
    │      db_id = '1'                                                                                                                                          │
    │      index = 1                                                                                                                                            │
    │   metadata = {}                                                                                                                                           │
    │       name = 'job 1'                                                                                                                                      │
    │    parents = []                                                                                                                                           │
    │   priority = 0                                                                                                                                            │
    │     remote = {                                                                                                                                            │
    │                  'step_attempts': 2,                                                                                                                      │
    │                  'retry_time_limit': '2024-02-27 16:51',                                                                                                  │
    │                  'error': Remote error: Could not create remote directory /path/to/run/folder/dc/39/8a/dc398a00-61b3-43e5-b4c5-f9c2ef8713b5_1 for db_id 1 │
    │              }                                                                                                                                            │
    │      state = 'CHECKED_OUT'                                                                                                                                │
    │ updated_on = '2024-02-27 15:16'                                                                                                                           │
    │       uuid = 'dc398a00-61b3-43e5-b4c5-f9c2ef8713b5'                                                                                                       │
    │     worker = 'worker1'                                                                                                                                    │
    ╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

If, for example, this was due to a problem with the connection to the worker that has
already been resolved, it is possible to avoid waiting for the job to be retried.
This can be done by running::

    jf job retry 1

If instead the runner has time to retry uploading the files multiple times the Job
will end up in the ``REMOTE_ERROR`` state

.. parsed-literal::

    ┏━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name      ┃ State        ┃ Job id  (Index)                           ┃ Worker    ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 1     │ job 1     │ :red:`REMOTE_ERROR` │ dc398a00-61b3-43e5-b4c5-f9c2ef8713b5  (1) │ worker1   │ 2024-02-27 16:05   │
    │ 2     │ job 2     │ WAITING      │ 3b2fdeae-750b-4b77-9395-971a88545f3e  (1) │ worker1   │ 2024-02-27 15:16   │
    └───────┴───────────┴──────────────┴───────────────────────────────────────────┴───────────┴────────────────────┘

In a similar way, the info of the Job will contain the details about the failure::

    ╭───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ created_on = '2024-02-27 15:16'                                                                                                                           │
    │      db_id = '1'                                                                                                                                          │
    │      index = 1                                                                                                                                            │
    │   metadata = {}                                                                                                                                           │
    │       name = 'job 1'                                                                                                                                      │
    │    parents = []                                                                                                                                           │
    │    previous_state = 'CHECKED_OUT'                                                                                                                         │
    │   priority = 0                                                                                                                                            │
    │     remote = {                                                                                                                                            │
    │                  'step_attempts': 3,                                                                                                                      │
    │                  'retry_time_limit': '2024-02-27 16:51',                                                                                                  │
    │                  'error': Remote error: Could not create remote directory /path/to/run/folder/dc/39/8a/dc398a00-61b3-43e5-b4c5-f9c2ef8713b5_1 for db_id 1 │
    │              }                                                                                                                                            │
    │      state = 'REMOTE_ERROR'                                                                                                                               │
    │ updated_on = '2024-02-27 16:05'                                                                                                                           │
    │       uuid = 'dc398a00-61b3-43e5-b4c5-f9c2ef8713b5'                                                                                                       │
    │     worker = 'worker1'                                                                                                                                    │
    ╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

It can be noticed that the state that was reached before the failure is now shown in
the ``previous_state`` value. Again, if the problem was temporary and has been fixed
the Job can be brought back to the ``previous_state`` (``CHECKED_OUT`` in this case)
with the command ``jf job retry 1``.

If for any reason the Job needs to be restarted from scratch, i.e. brought back to
the ``READY`` state, this can be achieved by running::

    jf job rerun 1

.. note::

    The ``jf job rerun`` and ``jf job retry`` commands have several options to select
    multiple Jobs as once. For example ``jf job retry -s REMOTE_ERROR`` to retry all
    the Jobs in the ``REMOTE_ERROR`` state. Check ``jf job retry -h`` for the full
    list of options available.

.. warning::
    Rerunning by default also marks the Job so that the folder on the worker where it
    was executed will be deleted. The deletion is performed by the Runner before
    reuploading the inputs, i.e. before the Job gets to the `UPLOADED` state.
    To avoid it use the ``--no-delete`` option.

.. warning::
    It is impossible to provide an exhaustive list of potential issues that could lead to
    a ``REMOTE_ERROR`` state. So except in some well defined cases, the error messages will be
    mainly given by the stack trace of the error.

.. _joberrors:

Job errors
==========

Errors may of course also arise during the execution of a Job in the worker. In this case
the runner will not be able to tell right away that an error has occurred. It will need to first download the
output of the Job and extract the error from it. In this case the Job will
first reach the ``DOWNLOADED`` state, and then will either become ``COMPLETED`` or
``FAILED``, depending whether the Job completed successfully or not.

The kind of errors that can lead to this could be:

* issues from the code executed by the Job
* bad input parameters
* unsuccessful calculation in the Job
* insufficient resources allocated in the worker
* a bug in the Job's code or in jobflow-remote

but the possible issues would strictly depend on the Jobs being executed.

As for the :ref:`remoteerrors`, the Jobs in this state can be identified from the Job
list with the CLI (``jf job list``):

.. parsed-literal::

    ┏━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name      ┃ State   ┃ Job id  (Index)                           ┃ Worker      ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 5     │ job raise │ :red:`FAILED`  │ 1f871d18-8d0d-4720-bc41-d647027fa5ec  (1) │ local_shell │ 2024-02-27 17:26   │
    │ 6     │ job 2     │ WAITING │ dc4ebf43-b0b4-46f8-b578-90c433ceb714  (1) │ local_shell │ 2024-02-27 17:25   │
    └───────┴───────────┴─────────┴───────────────────────────────────────────┴─────────────┴────────────────────┘

And the details of the error can be obtained with the ``jf job info 5`` command::

    ╭────────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ created_on = '2024-02-27 23:25'                                                                        │
    │      db_id = '5'                                                                                       │
    │   end_time = '2024-02-27 23:26'                                                                        │
    │      error = Traceback (most recent call last):                                                        │
    │                File "/python3.11/site-packages/jobflow_remote/jobs/run.py", line 58, in run_remote_job │
    │                  response = job.run(store=store)                                                       │
    │                             ^^^^^^^^^^^^^^^^^^^^                                                       │
    │                File "/python3.11/site-packages/jobflow/core/job.py", line 583, in run                  │
    │                  response = function(*self.function_args, **self.function_kwargs)                      │
    │                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^                      │
    │                File "/path/to/job/file.py", line 24, in raise_job                                      │
    │                  raise RuntimeError("A generic error")                                                 │
    │              RuntimeError: An error for a and b                                                        │
    │      index = 1                                                                                         │
    │   metadata = {}                                                                                        │
    │       name = 'add_raise'                                                                               │
    │    parents = []                                                                                        │
    │   priority = 0                                                                                         │
    │     remote = {'step_attempts': 0, 'process_id': '35379'}                                               │
    │    run_dir = '/run_jobflow/1f/87/1d/1f871d18-8d0d-4720-bc41-d647027fa5ec_1'                            │
    │ start_time = '2024-02-27 17:26'                                                                        │
    │      state = 'FAILED'                                                                                  │
    │ updated_on = '2024-02-27 17:26'                                                                        │
    │       uuid = '1f871d18-8d0d-4720-bc41-d647027fa5ec'                                                    │
    │     worker = 'worker1'                                                                                 │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────────╯

In this case an ad-hoc failing job was executed and the ``error`` contains the stack trace of the
Python error. In most cases however, a failure related to the Job execution may lead to an error
message that would not reveal the true nature of the problem. In that case the best option would be
to investigate the output files in the produced by the Job in the ``run_dir`` folder.
The folder should always contain a ``queue.out`` and a ``queue.out`` file, that contain the
``stdout`` and ``stderr`` of the executed script. Any issue related to the queuing system and
most issues related to the code executed by the Job are likely to be printed there. For this
reason a convenience tool is available in the CLI to fetch their content directly from the
worker's folder::

    jf job queue-out 5

If the content of these files does not help identifying the issue and the Job produces output
files, those should also be checked for errors.

The actions required to solve the issue will depend on the nature of the error itself. If this
resulted from a temporary issue (e.g. a failure of the cluster), simply rerunning the
job with::

    jf job rerun 5

will solve the issue.

.. warning::
    Rerunning by default also marks the Job so that the folder on the worker where it
    was executed will be deleted. The deletion is performed by the Runner before
    reuploading the inputs, i.e. before the Job gets to the `UPLOADED` state. If, as effect
    of a rerun children Jobs are also rerun, their folders will be marked for deletion as
    well. To avoid it use the ``--no-delete`` option.

.. note::

    Only ``jf job rerun`` should be applied to ``FAILED`` Jobs. ``jf job retry`` is **not**
    suitable in this case.

If the error is due to a lack of resources or wrong configuration options, these can be updated
using the specific commands::

    jf job set resources
    jf job set exec-config
    jf job set worker

With the correct resources set, the Job may complete correctly when it is executed again.

If instead the error is caused by a wrong input provided to the Job or to some problem
related to the Job itself, one potential option would be to try to
alter the content of the ``Job`` object in the queue database. However this will depend
on the type of Job being performed and cannot be directly handled by jobflow-remote.
In most cases this kind of errors will require to delete the old Flow (e.g. ``jf flow delete -jid 5``)
and resubmit it with the correct inputs.


States diagram
==============


The following diagram summarizes the possible transitions from failed states,
depending on the usage of the ``rerun`` and ``retry`` commands. Rerunning a Job
always brings back the Job to the ``READY`` state, while retrying will bring it
back to its previous state, when the remote error occurred.

.. mermaid::

    flowchart LR

    REMOTE_ERROR --> retry{Retry}
    retry --> CHECKED_OUT
    retry --> UPLOADED
    retry --> SUBMITTED
    retry --> RUNNING
    retry --> TERMINATED
    retry --> DOWNLOADED
    REMOTE_ERROR --> rerun{Rerun}
    FAILED --> rerun{Rerun}
    rerun --> READY


    classDef error fill:#E62A2A,color:white
    classDef running fill:#2a48e6,color:white
    classDef success fill:#289e21,color:white
    classDef ready fill:#8be485
    classDef wait fill:#eae433

    class REMOTE_ERROR,FAILED error
    class CHECKED_OUT,UPLOADED,SUBMITTED,RUNNING,TERMINATED,DOWNLOADED running
    class COMPLETED success
    class READY ready
    class WAITING wait


Rerun constraints
=================

In general, rerunning a simple ``FAILED`` Job should not pose any issue. However,
jobflow has a specific feature that allows Jobs to switch to the ``READY`` state
even if some of the parents have failed (see the ``OnMissing`` options for the Job
configuration). In such a case rerunning a the ``FAILED`` parent Job may be lead
to inconsistencies in the queue database.

Jobflow-remote tries to avoid such inconsistencies by limiting the options to
rerun a Job. In particular

* if a Job ``FAILED`` and all its children are in the ``READY`` and ``WAITING``
  state, the Job can always be rerun
* if any of the children (or further descendant) Jobs have a Job index larger than
  ``1``, the ``FAILED`` Job can never be rerun, as there is no way to handle the state
  of the children in a meaningful way.
* in all the other cases it would be possible to rerun the ``FAILED`` Job, using the
  the ``--force`` option in the ``jf job rerun`` command. The user should be aware
  that this may lead to inconsistencies if dynamical generation of Jobs is involved.
  Child Jobs will be rerun and set in ``READY`` or ``WAITING`` state.
  In this case, the Runner should be preferably stopped beforehand in order to minimize the risk of
  inconsistencies.


.. _errors runner:

Runner errors and Locked jobs
=============================

Even if less likely, the Runner itself may also have issues during its execution. Aside
from bugs, the process could also be stopped abruptly due to issues on the machine
that is hosting it or for having required too much resources (e.g. too much memory).

If this happens and the Runner was processing a Job, it is likely that the Job will
be left in a *locked* state. In fact, to avoid concurrent actions, when working on
a Job the system puts a lock on the corresponding document in the database. If the
Runner is killed the lock will not be released. This means that, even if the
Runner is restarted, no further process of the Job could happen, because the system
will expect that some other process is working on the locked Job.

Locked Jobs can be identified in the job list using the ``-v`` or ``-vv`` option::

    jf job list -v

which will give the following output, including the ``Locked`` column:

.. parsed-literal::

    ┏━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┓
    ┃ DB id ┃ Name      ┃ State    ┃ Job id  (Index)                           ┃ Worker      ┃ Last updated [CET] ┃ Queue id ┃ Run time ┃ Retry time [CET] ┃ Prev state ┃ Locked ┃
    ┡━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━┩
    │ 7     │ job 1     │ UPLOADED │ c527bdee-1edd-48df-b533-fe20a63fa8c6  (1) │ worker1     │ 2024-02-28 11:49   │          │          │                  │            │ *      │
    │ 8     │ job 2     │ WAITING  │ fcb75bca-d6f3-45c0-8980-79dec4ad0737  (1) │ worker1     │ 2024-02-28 11:46   │          │          │                  │            │        │
    └───────┴───────────┴──────────┴───────────────────────────────────────────┴─────────────┴────────────────────┴──────────┴──────────┴──────────────────┴────────────┴────────┘

Alternatively, the list of locked Jobs can be obtained with the ``jf job list -l`` command.

.. warning::

    The presence of a locked Job in the list does **not** imply an error in the Runner.
    Jobs will be constantly locked while performing operations on them. For example,
    transferring files from the worker may take some time and the Job will remain
    locked during the whole procedure.

If a Job appears to be locked for a long time or if the Runner is stopped and a Job is still
locked, it is likely that the lock was not properly released.

.. note::

    Remember that when running ``jf runner stop`` or ``jf runner shutdown`` the runner will
    not stop immediately, if it is in the middle of an operation. Check the Runner
    status with ``jf runner status`` or by inspecting the ``runner.log`` file in the
    ``~/.jfremote/PROJECT_NAME/log`` folder to determine if the Runner is active or not.

In case the Job should not be locked, it can be unlocked with the command::

    jf admin remove-lock -jid 5

At this point the Runner will repeat the action.

.. warning::
    Any action that could have been previously performed will be repeated.

Alternatively, it is also possible to entirely rerun the Job passing the ``--break-lock``
option::

    jf job rerun --break-lock 5

The runner should be preferably stopped before performing this command.

Runner logs
===========

In addition to the error messages, if the source of an error could not be determined,
it may be worth trying to inspect the log files produced by the runner.

Each project has its own folder (by default as a subfolder of ``~/.jfremote``) and the
logs can be found in the ``~/.jfremote/PROJECT_NAME/log`` directory. The ``runner.log`` file
contain the log message produced by the python ``Runner`` object. This is more likely
to contain information concerning errors related to the code. The ``supervisord.log``
is instead the log produced by supervisord, that manages the daemon processes.
