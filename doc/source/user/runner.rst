.. _runner:

======
Runner
======

In jobflow-remote the Runner refers to one or more processes that handle the whole
execution of the jobflow workflows, including the interaction with the worker and the
writing of the outputs in the ``JobStore``.

The way the Job states change based on the action of the Runner has already been
described in the introductory :ref:`workingprinciple` section. This section will
instead focus on the technical aspects of the runner execution.

Setup
=====

As explained in the :ref:`setup options` section, and exemplified in the figure below,
the Runner process must have **access to all required resources** as specified in the project
configuration. In particular all the workers, the MongoDB database defined in the
``queue`` section and the output ``JobStore``.

.. image:: ../_static/img/daemon_schema.svg
   :width: 50%
   :alt: All-in-one configuration
   :align: center

Runner processes
================

The ``Runner`` performs different tasks, mainly divided in

1) checking out jobs from database to start a Flow execution
2) updating the states of the Jobs in the ``queue`` database
3) interacting with the worker hosts to upload/download files and check the job status
4) inserting the output data in the output ``JobStore``

While all these can be executed in a single process, to speed up the execution of the jobs,
the default option is to start different daemonized processes, each of which takes care
of one of the actions listed above. In addition, if many Jobs need to be dealt with
simultaneously, it is also possible to start multiple independent processes that will
deal with the tasks 3 and 4. This can allow to increase the throughput.

.. note::
    This means that multiple instances of the ``Runner`` process are simultaneously
    running on the machine, each one requiring a certain amount of memory. If the
    system memory is a limiting factor, all the actions can be executed in a single
    process by starting the daemon with the ``--single`` option.

When activated from the CLI, these run as daemonized processes, that are handled by
`Supervisor <http://supervisord.org/index.html>`_. The CLI, through the ``DaemonManager``
object, will provide an interface to interact with the daemon and start, stop and kill
the ``Runner`` processes.

Process management
==================

Start
-----

The ``Runner`` is usually started using the CLI command::

    jf runner start

This will start the Supervisor process, that will then spawn the single or
multiple Runner processes. Note that the command will not wait for all the processes
to start, so the successful completion of the command does not necessarily imply
that all the Runner processes are active.
The number of runner processes can only be managed at start time. The ``--single``
option will run all the actions described in the previous section in a single
process, instead that in multiple ones, which is the default. The ``--transfer``
and ``--complete`` options allow to increase the number of processes dedicated to
the steps 3 and 4.

.. warning::
    The ``Runner`` reads the project configurations when each of the processes is
    started and does not attempt to refresh them during the execution. Whenever
    changing

.. _runner stop:

Stop
----

Executing the stop command::

    jf runner stop

relies on Supervisor to send a ``SIGTERM`` signal (a termination signal that allows
the process to exit cleanly) to all the ``Runner`` processes.
In this case the supervisor process will remain active. Unless the ``--wait`` option
is specified, the completion of the command will not imply that all the ``Runner``
processes have been terminated.

.. warning::
    The ``Runner`` is designed to recognize the signal and **wait the completion of
    the action being performed**, before actually exiting.

.. note::
    Since the supervisor processes remains active, when starting the runner again after
    a stop it is not possible to switch from a single process to a split configuration
    or the other way round. It is necessary to shut down the whole daemon in that case.

Shutdown
--------
Shutting down the runner with the command::

    jf runner shutdown

is equivalent to the :ref:`runner stop`, except that also the Supervisor process will
be stopped.

Kill
----

It is possible to directly kill all the processes, without sending the ``SIGTERM``
signal and thus without waiting for the current action to be completed with the
command::

    jf runner kill

.. warning::
    If an was action was being performed, it is possible that the database may be
    left in an inconsistent state and/or that the Job that was being processed
    will be *locked*, as the runner puts an explicit lock on the document while
    working on a Job and/or on a Flow. See the :ref:`errors runner` section for
    how to handle these cases.

Information
-----------

It is possible to get an overall state of the runner daemon executing::

    jf runner status

This returns a custom global state defined in jobflow-remote. Typical
values are ``shut_down``, ``stopped`` and ``running``. A ``partially_running``
state means that some of the daemonized processes are active, while other
are either not yet started or have been stopped.
To get more details about the single processes it is possible to run::

    jf runner info

This prints a table like::

    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┓
    ┃ Process                                      ┃ PID   ┃ State   ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━┩
    │ supervisord                                  │ 12305 │ RUNNING │
    │ runner_daemon_checkout:run_jobflow_checkout  │ 90127 │ RUNNING │
    │ runner_daemon_complete:run_jobflow_complete0 │ 90128 │ RUNNING │
    │ runner_daemon_queue:run_jobflow_queue        │ 90129 │ RUNNING │
    │ runner_daemon_transfer:run_jobflow_transfer0 │ 90130 │ RUNNING │
    └──────────────────────────────────────────────┴───────┴─────────┘

providing the state of each individual daemon process and its system
process ID.

Running daemon check
====================

There are no strict limitations to which machine should be used to execute the ``Runner``
as a daemon, and, as explained in the :ref:`setup options` section, there are several
possible configurations. It is thus possible for a user to mistakenly start
the runner daemon on two different locations. While this should not corrupt
the database, thanks to the locking mechanism, it may be confusing as a user
may be unaware that a runner is already active on some machine.
To mitigate the possibility of this to occur, jobflow-remote also adds information
in the database about the machine where a ``Runner`` daemon is started. The
code will then prevent the system to start a daemon on a different machine. All the
commands will instead be allowed if the information about the machine where are
executed match those in the database.
If a machine where a ``Runner`` was previously active was switched off without
explicitly stopping it, the database will still consider that daemon to be active.
To start the daemon on another machine, if it is certain that ``Runner`` is not
active anymore, it is possible to clean the database reference to the previous
process with the command::

    jf runner reset

A new ``Runner`` daemon can then be started anywhere.

.. warning::
    This procedure is applied only for ``Runner`` processes started as a daemon.
    No check is done and no data is added to the database if the ``Runner`` is
    started directly. See the :ref:`runner direct` section below.

Backoff algorithm
=================

While performing its actions on the Jobs, the ``Runner`` processes may incur in some
issues. For example a connection error may occur. In order to avoid overloading
the processes and/or the resources, when such an error occurs the process will not
immediately retry to execute the action, but wait an increasingly larger amount of time
before retrying. After three failures, the Job will be set to the ``REMOTE_ERROR``
state. See the :ref:`remoteerrors` for more details.


.. _runner direct:

Direct execution
================

It is not the standard usage, but in some cases, for example during development
or debugging, it may be useful to run the ``Runner`` processes directly and not as
a daemon. The simplest option to do that would be to run::

    jf runner run

This will start a single ``Runner`` process performing all the actions.
Similarly, it is also possible to execute this from the python API with the code
below

.. code-block::python

    from jobflow_remote.jobs.runner import Runner
    Runner(project_name="xxx").run()
