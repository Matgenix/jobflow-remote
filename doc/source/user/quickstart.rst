.. _quickstart:

=========================
Jobflow-Remote quickstart
=========================

After completing the :ref:`install`, it is possible to start submitting
``Flow`` for execution. If you are not familiar with the concept of ``Job``
and ``Flow`` in jobflow you can start checking its
`tutorials <https://materialsproject.github.io/jobflow/tutorials.html>`_.

Any jobflow's ``Flow`` can be executed with jobflow-remote,
but, at variance with jobflow simple examples, the Job functions should
be serializable and accessible by the runner. Simple custom examples based
on functions defined on the fly cannot thus be used.
For this reason a few simple ``Job`` s have been prepared for
test purposes in the ``jobflow_remote.utils.examples`` module.

For the execution of the following tutorial it may be convenient to define
a simple worker with ``type: local`` and ``scheduler_type: shell`` to speed up
the execution, but any worker is acceptable.

Submit a ``Flow``
=================

To run a workflow with jobflow-remote the first step is to insert it into the
database. A ``Flow`` can be created following the standard jobflow procedure.
Then it should be passed to the ``submit_flow`` function:

.. code-block:: python

    from jobflow_remote.utils.examples import add
    from jobflow_remote import submit_flow
    from jobflow import Flow

    job1 = add(1, 2)
    job2 = add(job1.output, 2)

    flow = Flow([job1, job2])

    print(submit_flow(flow, worker="local_shell"))


This code will print an integer unique id associated to the submitted ``Job`` s.

.. note::

    In addition to the uuid, the standard jobflow's identifier for Jobs,
    jobflow-remote also defines an incremental ``db_id``, to help quickly
    identify different Jobs. A ``db_id`` uniquely identifies each Job entry
    in jobflow-remote's queue database. The same entry is also uniquely
    identified by the ``uuid``, ``index`` pair.

.. note::

    On the worker selection:
     * The worker should match the name of one of the workers defined in the project.
     * In this way all the ``Job`` s will be assigned to the same worker.
     * If the argument is omitted the first worker in the project configuration is used.
     * In any case the worker is determined when the ``Job`` is inserted in the database.

.. warning::

    Once the flow has been submitted to the database, any further change to the
    ``Flow`` object will not be taken into account.

It is now possible to use the ``jf`` command line interface (CLI)::

    jf job list

to display the list of ``Job`` s in the database::

                                                   Jobs info
    ┏━━━━━━━┳━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name ┃ State   ┃ Job id  (Index)                           ┃ Worker      ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 2     │ add  │ WAITING │ 8b7a7841-37c7-4446-853b-ad3c00eb5227  (1) │ local_shell │ 2023-12-19 16:33   │
    │ 1     │ add  │ READY   │ ae020c67-72f0-4805-858e-fe48644e4bb0  (1) │ local_shell │ 2023-12-19 16:33   │
    └───────┴──────┴─────────┴───────────────────────────────────────────┴─────────────┴────────────────────┘

.. note::

    It is possible to use the ``-v`` flag to increase the **verbosity** of the output.
    Use ``-vv`` or ``-vvv`` to further increase the the verbosity.

    It is also possible to **filter** and *sort** the results. run ``jf job list -h``
    to see the available options.

One of the Jobs is in the ``READY`` state, signaling that it is ready to be executed.
The second Job is instead in the ``WAITING`` state since it will not start until the
first reaches the ``COMPLETED`` state. At this point nothing will happen, since the
process to handle the Jobs has not been started yet.

The Runner
==========

Jobflow-remote's ``Runner`` is an object that takes care of handling the
submitted ``Job`` s. It performs several actions to advance the state of the
workflows. For each ``Job`` it:

* Copies files to and from the **WORKER** (i.e. ``Job`` 's inputs and outputs)
* Interacts with the **WORKER**'s queue manager (e.g. SLURM, PBS, ...),
  submitting jobs and checking their state
* Updates the content of the database

Only the actual execution of the Jobs in the Workers are disconnected
from the ``Runner``. In all the other cases, the state of the ``Job`` s
can change only if the ``Runner`` is running.

The standard way to execute the ``Runner`` is through a daemon process
that can be started with the ``jf`` CLI::

    jf runner start

Since the process starts in the background, you can check that it properly
started with the command::

    jf runner status

If the ``Runner`` started correctly you should get::

    Daemon status: running

During the execution of the Job it is possible to check their status as
done before::

                                                     Jobs info
    ┏━━━━━━━┳━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name ┃ State     ┃ Job id  (Index)                           ┃ Worker      ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 2     │ add  │ RUNNING   │ 8b7a7841-37c7-4446-853b-ad3c00eb5227  (1) │ local_shell │ 2023-12-19 16:44   │
    │ 1     │ add  │ COMPLETED │ ae020c67-72f0-4805-858e-fe48644e4bb0  (1) │ local_shell │ 2023-12-19 16:44   │
    └───────┴──────┴───────────┴───────────────────────────────────────────┴─────────────┴────────────────────┘

.. note::

    The ``Runner`` checks the states of the Jobs at regular intervals. A few seconds may
    be required to have a change in the Job state.

The ``Runner`` will keep checking the database for the submission of new Jobs
and will update the state of each Job as soon as the previous action is completed.
If you plan to keep submitting workflows you can keep the daemon running, otherwise
you can stop the process with::

    jf runner stop

.. note::

    By default the daemon will spawn several processes, each taking care of some
    of the actions listed above.

.. warning::

    The ``stop`` command will send a ``SIGTERM`` command to the ``Runner`` processes, that
    will terminate the action currently being performed before actually stopping. This should
    prevent the presence on inconsistent states in the database.
    However, if you believe the ``Runner`` is stuck or need to halt the ``Runner`` immediately
    you can kill the processes with::

        jf runner kill

Results
=======

As in standard jobflow execution, when a ``Job`` is ``COMPLETED`` its output is
stored in the defined ``JobStore``. For simple cases like the one used in this
example the outputs can be fetched directly using the CLI::

    jf job output 2

That should print the expected result::

    5

.. note::

    The CLI commands that accept a single Job id, both the ``uuid`` or the ``db_id``
    can be passed. The code will automatically determine the

For more advanced workflows, the best way to obtain the results is using the
``JobStore``, as done with `usual jobflow's outputs <https://materialsproject.github.io/jobflow/tutorials/2-introduction.html#Examine-Flow-outputs>`_.
For jobflow-remote, a convenient way to access the ``JobStore`` in python is
to use the ``get_jobstore`` helper function.

.. code-block:: python

    from jobflow_remote import get_jobstore

    js = get_jobstore()
    js.connect()

    print(js.get_output("8b7a7841-37c7-4446-853b-ad3c00eb5227"))

CLI
===

On top of the CLI commands shown above a full list of the commands, sub-commands options
available is accessible through the ``-h`` flag. Here we present a few more of them
that can be useful to get started.

Job info
--------

Detailed information from a Job can be obtained running the command::

    jf job info 2

that prints a summary of the content of the Job document in the DB::

    ╭─────────────────────────────────────────────────────────────────────────────────────────────╮
    │ created_on = '2023-12-19 16:33'                                                             │
    │      db_id = 2                                                                              │
    │   end_time = '2023-12-19 16:44'                                                             │
    │      index = 1                                                                              │
    │   metadata = {}                                                                             │
    │       name = 'add'                                                                          │
    │    parents = ['ae020c67-72f0-4805-858e-fe48644e4bb0']                                       │
    │   priority = 0                                                                              │
    │     remote = {'step_attempts': 0, 'process_id': '89838'}                                    │
    │    run_dir = '/path/to/run/folder/8b/7a/78/8b7a7841-37c7-4446-853b-ad3c00eb5227_1'          │
    │ start_time = '2023-12-19 16:44'                                                             │
    │      state = 'COMPLETED'                                                                    │
    │ updated_on = '2023-12-19 16:44'                                                             │
    │       uuid = '8b7a7841-37c7-4446-853b-ad3c00eb5227'                                         │
    │     worker = 'local_shell'                                                                  │
    ╰─────────────────────────────────────────────────────────────────────────────────────────────╯

.. note::

    This will also contain the tracked error in case of failure of the Job.
    Dealing with failed Jobs will be dealt with in the troubleshooting section.

Flow list
---------

Similarly to the list of Jobs a list of Flows and their states can be obtained with::

    jf flow list

that returns::

                                                Flows info
    ┏━━━━━━━┳━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
    ┃ DB id ┃ Name ┃ State     ┃ Flow id                              ┃ Num Jobs ┃ Last updated [CET] ┃
    ┡━━━━━━━╇━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
    │ 1     │ Flow │ COMPLETED │ 959ffe14-7061-4b74-a3ad-10c3c12715ad │ 2        │ 2023-12-19 16:43   │
    └───────┴──────┴───────────┴──────────────────────────────────────┴──────────┴────────────────────┘

.. note::

    A Flow has its own uuid, while the DB id corresponds to the lowest DB id among the
    Jobs belonging to the Flow

Delete Flows
------------

In case you need to delete some Flows, without resetting the whole database,
you can use the command::

    jf flow delete -did 1

where filters similar to the ones of the ``list`` command can be used.
