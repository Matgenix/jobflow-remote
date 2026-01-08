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

.. _tuning exec_config:

Execution configuration
-----------------------

An execution configuration, represented as an ``ExecutionConfig`` object in
the code, contains information to run additional commands before and after
the execution of a Job.

This can be typically used to define the modules to load on an HPC center,
specific python environment to load or setting the ``PATH`` for some executable
needed by the Job. In an ``ExecutionConfig`` different elements can be defined:

* ``modules``: a list of strings representing the modules that will be loaded in the
  submission script (e.g. the string ``OpenMPI`` will be applied by running ``module load OpenMPI``).
* ``export``: a dictionary with names and values of variable that will be exported
  in the submission script (e.g. ``{"X": 1}`` will be translated to ``export X=1``)
* ``pre_run``: a string that will be included in the submission script before starting
  the Job.
* ``post_run``: a string that will be included in the submission script after the Job
  has been executed.

To customize the job an execution configuration can be selected setting the
``exec_config`` argument of ``submit_flow`` or ``set_run_config``, where the values can be:

1) a string pointing to an ``exec_config`` defined in the Project configuration
2) an instance of ``ExecutionConfig``

An example of ``exec_config`` in the configuration file is

.. code-block:: yaml

    exec_config:
      example_config:
        modules:
        - releases/2021b
        - intel/2021b
        export:
          PATH: /path/to/your/code/bin:$PATH
        pre_run: "echo 'test'\necho 'test2'"
        post_run:

where ``example_config`` is a custom name. This can be used when submitting a Job as

.. code-block:: python

    submit_flow(flow, exec_config="example_config", worker="worker_1")

and will result in the following lines being added to the submission script

.. code-block:: bash

    echo 'test'
    echo 'test2'
    export PATH=/path/to/your/code/bin:$PATH
    module load releases/2021b
    module load intel/2021b

The same can be achieved by defining an instance of ``ExecutionConfig`` in the submission
script and passing it to ``submit_flow``.

.. note::

    These lines will be added **after** the worker ``pre_run`` in the submission script.

.. note::

    Multiple ``exec_config`` can be defined in the Project configuration, but only one
    can be passed to an ``exec_config`` argument.

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

    If relying on the specific queueing system keywords, the values that can be
    passed are those present in the `templates defined in qtoolkit <https://matgenix.github.io/qtoolkit/user/resources.html#scheduler-templates>`_
    Given the large number of options usually available for each scheduler, the
    templates for each scheduler only contains a subset of the available options.
    However, in each of the templates it is possible to pass a ``qverbatim`` option
    that will be added to the header as is, allowing to pass any scheduler-specific option.
    For example, in the case of a Slurm scheduler:

    .. code-block:: python

        options = {
            "partition": "standard",
            "qverbatim": "#SBATCH --tmp=10G\n#SBATCH --nice=100",
        }
        script = slurm_io.get_submission_script(commands="echo 'Hello'", options=options)


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
        flow, name_filter="add", worker="secondw", exec_config="anotherconfig", priority=10
    )

    resource = {"nodes": 1, "ntasks": 4, "partition": "batch"}
    submit_flow(flow, worker="firstw", exec_config="somecode_v.x.y", resources=resources)

After being submitted to the database the ``value`` Job will be executed
on the ``firstw`` worker, while the ``add`` Job will be executed on the
``secondw`` worker. ``add`` will also have a priority of 10, while ``value`` will
remain with the default 0 value. On the other hand, since ``resources`` is not set
explicitly when ``set_run_config`` is called, the same ``resource`` dictionary
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

    In order for this to be meaningful only a subset of Jobs states are
    acceptable. This commands can be applied only to Jobs in the
    ``READY``, ``WAITING``, ``COMPLETED``, ``FAILED``, ``PAUSED`` and
    ``REMOTE_ERROR`` states.
