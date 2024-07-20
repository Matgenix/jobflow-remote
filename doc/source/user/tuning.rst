.. _tuning:

********************
Tuning Job execution
********************

Jobs with time consuming calculations require to properly configure
the environment and the resources used to execute them. This
section focuses on which options can be tuned and the ways available
in jobflow-remote to change them.

Tuning options
==============

Worker
------

A worker is a computational unit that will actually execute the function
inside a Job. The list of workers is given in the :ref:`projectconf worker`
project configuration.

Workers are set by the name used to define them in the project and a worker
should always be defined for each Job when adding a Flow to the database.

.. note::

    A single worker should not necessarily be identified with a computation
    resource as a whole. Different workers referring to the same for example
    to the same HPC center, but with different configurations can be created.
    The ``Runner`` will still open a single connection if the host is the same.

Execution configuration
-----------------------

An execution configuration, represented as an ``ExecutionConfig`` object in
the code, contains information to run additional commands before and after
the execution of a Job.

These can be typically used to define the modules to load on an HPC center,
specific python environment to load or setting the ``PATH`` for some executable
needed by the Job.

They can be usually given as a string referring to the setting defined in the
project configuration file, or as an instance of ``ExecutionConfig``.

Resources
---------

If the worker executing the Job runs under the control of a queueing system
(e.g. SLURM, PBS), it is also important to specify which resources need to
be allocated when running a Job.

Since the all the operations involving the queueing system are handled with
`qtoolkit <https://matgenix.github.io/qtoolkit/>`_, jobflow-remote
supports the same functionalities. In particular it is either possible to
pass a dictionary containing the keywords specific to the selected queuing system
or to pass an instance of a ``QResources``, a generic object defining resources
for standard use cases. These will be used to fill in a template and generate
a suitable submission script.

.. note::

    There are `SLURM <https://matgenix.github.io/qtoolkit/api/qtoolkit.io.slurm.html>`_
    and `PBS <https://matgenix.github.io/qtoolkit/api/qtoolkit.io.pbs.html>`_
    specific keywords that can be passed to ``submit_flow`` to use the
    respective queueing system commands.

How to tune
===========

Different ways of setting the worker, execution configuration and resources
for each Job are available. A combination of them can be used to ease the
configuration for all the Jobs.

.. note::

    If not defined otherwise, Jobs generated dynamically will inherit
    the configuration of the Job that generated them.

Submission
----------

The first entry point to customize the execution of the Jobs in a Flow
is to use the arguments in the ``submit_flow`` function.

.. code-block:: python

    resource = {"nodes": 1, "ntasks": 4, "partition": "batch"}
    submit_flow(
        flow, worker="local_shell", exec_config="somecode_v.x.y", resources=resources
    )

This will set the passed values for all the Jobs for which have not been
set in the Job previously.

.. warning::

    Once the flow has been submitted to the database, any further change to the
    ``Flow`` object will not be taken into account.

JobConfig
---------

Each jobflow's Job has a ``JobConfig`` attribute. This can be used to store
a ``manager_config`` dictionary with configuration specific to that Job.

This can be done with the ``set_run_config`` function, that targets Jobs
based on their name or on the callable they are wrapping. Consider the
following example

.. code-block:: python

    from jobflow_remote.utils.examples import add, value
    from jobflow_remote import submit_flow, set_run_config
    from jobflow import Flow

    job1 = value(5)
    job2 = add(job1.output, 2)

    flow = Flow([job1, job2])

    flow = set_run_config(
        flow, name_filter="add", worker="secondw", exec_config="anotherconfig"
    )

    resource = {"nodes": 1, "ntasks": 4, "partition": "batch"}
    submit_flow(flow, worker="firstw", exec_config="somecode_v.x.y", resources=resources)

After being submitted to the database the ``value`` Job will be executed
on the ``firstw`` worker, while the ``add`` Job will be executed on the
``secondw`` worker. On the other hand, since ``resources`` is not set
explicitly when ``set_run_config`` is called the same ``resource`` dictionary
is applied to **all** the Jobs in the Flow.

.. note::

    If the values in a Job should not be overridden by those passed in
    ``submit_flow``, but no specific value is required, empty objects
    need to be passed. An empty ``dict`` or an empty ``QResources()`` to
    set an empty ``resources`` and an empty ``ExecutionConfig()`` to set
    and empty ``exec_config``

.. warning::

    If ``set_run_config`` is used to set a worker with ``scheduler_type: shell``
    always set ``resources`` to an empty dictionary (or ``QResources``) as well.
    Otherwise the ``resources`` value passed to ``submit_flow`` will be used and
    it will likely be incompatible, with the ``shell`` worker.

In addition, since ``set_run_config`` makes use of jobflow's ``update_config``
method, these updates will also automatically be applied to any new Job
automatically generated in the Flow.

.. warning::

    The ``name_filter`` matches any name containing the string passed.
    So using a ``name_filter=add`` will match both a job named ``add``
    and one named ``add more``.


CLI
---

After a Job has been added to the database, it is still possible to change
its settings. This can be achieved with the ``jf job set`` CLI command.
For example running::

    jf job set worker -did 8 example_worker

sets the worker for Job with DB id 8 to ``example_worker``. Similarly,
the ``jf job set resources`` and ``jf job set exec-config`` can be used
to set the values of the resources and execution configurations.

.. note::

    In order for this to be meaningful only Jobs that have not been started
    can be modified. So this commands can be applied only to Jobs in the
    ``READY`` or ``WAITING`` states.
