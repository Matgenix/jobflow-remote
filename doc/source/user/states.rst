.. _states:

******
States
******


Job States
**********

During their execution by the ``Runner`` a Job can reach different states.
Each of the states describes the current status of the after the ``Runner``
has finished switching from one state to another.

Since the ``Runner`` can be stopped and will update the different states at
predefined intervals, it may be that the state does not reflect the
actual situation of a Job (for example it could be that the process of a
Job in the ``RUNNING`` state has finished, but the ``Runner`` did not
update its state yet.

Description
===========

The states can then be grouped in

* Waiting states: describing Jobs that has not started yet.
* Running states: states for which the ``Runner`` has started working on the Job
* Completed state: a state where the Job has been completed successfully
* Error states: states associated with some error in the Job, either programmatic
  or during the execution.

The list of Job states is defined in the :py:class:`jobflow_remote.jobs.state.JobState`
object. Here we present a list of each state with a short description.

WAITING
-------

Waiting state. A Job that has been inserted into the database but has
to wait for other Jobs to be completed before starting.

READY
-----

Waiting state. A Job that is ready to be executed by the ``Runner``.

CHECKED_OUT
-----------

Running state. A Job that has been selected by the ``Runner`` to
start its execution.

UPLOADED
--------

Running state. All the inputs required by the Job has been copied
to the worker.

SUBMITTED
---------

Running state. The Job has been submitted to the queueing
system of the worker.

RUNNING
-------

Running state. The ``Runner`` verified that the Job has started is being
executed on the worker.

TERMINATED
----------

Running state. The process executing the Job on the worked has finished
running. No knowledge of whether this happened for an error or because
the Job was completed correctly is available at this point.

DOWNLOADED
----------

Running state. The ``Runner`` has copied to the local machine all the
files containing the Job response and outputs to be stored.

COMPLETED
---------

Completed state. A Job that has completed correctly.

FAILED
------

Error state. The procedure to execute the Job completed correctly, but
an error happened during the execution of the Job's function, so the
Job did not complete successfully.

REMOTE_ERROR
------------

Error state. An error occurred during the procedure to execute the Job.
For example the files could not be copied due to some network error and
the maximum number of attempts has been reached. The Job may or may not
be executed, depending on the action that generated the issue, but in
any case no information is available about it. This failure is independent
from the correct execution of the Job's function.

PAUSED
------

Waiting state. The Job has been paused by the user and will not be
executed by the ``Runner``. A Job in this state can be started again.

STOPPED
-------

Error state. The Job was stopped by another Job as a consequence of a
``stop_jobflow`` or ``stop_children`` actions in the Job's response.
This state cannot be modified.

USER_STOPPED
------------

Error state. A Job stopped by the user. This state cannot be modified.

BATCH_SUBMITTED
---------------

Running state. A Job submitted for execution to a batch worker. Differs
from the ``SUBMITTED`` state since the ``Runner`` does not have to
check its state in the queueing system.

BATCH_RUNNING
-------------

Running state. A Job that is being executed by a batch worker. Differs
from the ``RUNNING`` state since the ``Runner`` does not have to
check its state in the queueing system.


Evolution
=========

If the state of a Job is not directly modified by user, the ``Runner``
will consistently update the state of each Job in a running state.

The following diagram illustrates which states transitions can
be performed by the ``Runner`` on a Job. This includes the transitions
to intermediate or final error states.

.. mermaid::

    stateDiagram-v2
        WAITING --> READY
        READY --> CHECKED_OUT
        CHECKED_OUT --> UPLOADED
        UPLOADED --> SUBMITTED
        SUBMITTED --> RUNNING
        RUNNING --> TERMINATED
        SUBMITTED --> TERMINATED
        TERMINATED --> DOWNLOADED
        DOWNLOADED --> COMPLETED
        DOWNLOADED --> FAILED

        CHECKED_OUT --> REMOTE_ERROR
        UPLOADED --> REMOTE_ERROR
        SUBMITTED --> REMOTE_ERROR
        RUNNING --> REMOTE_ERROR
        TERMINATED --> REMOTE_ERROR
        DOWNLOADED --> REMOTE_ERROR



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

Flow states
***********

Each Flow in the database also has a global state. This is a function of
the states of each of the Jobs included in the Flow. As for the Jobs,
the Flow states can change due to the action of the ``Runner``
or of the user.

Description
===========

The list of Flow states is simplified compared to the Job's states, since several
Job state will be grouped under a single Flow state.

The list of Flow states is defined in the :py:class:`jobflow_remote.jobs.state.FlowState`
object. Here we present a list of each state with a short description.

READY
-----

There is at least one Job in the READY state. No Jobs have started or have failed.

RUNNING
-------

At least one of the Jobs is being or has been executed. The state will not be
changed if a single Job completes, but there are still other Jobs to be executed.

COMPLETED
---------

All the left Jobs of the Flow are in the ``COMPLETED`` state. This means that some
intermediate Job may be in the ``FAILED`` state, but its children are set to
not give an error in the ``on_missing_references`` of the ``JobConfig``.

FAILED
------

At least one of the Jobs failed and the Flow is not ``COMPLETED``.

STOPPED
-------

At least one of the Job is in the ``STOPPED``  or the ``USER_STOPPED`` state
and the flow is not in one of the previous states.

PAUSED
------

At least one of the Job is in the ``PAUSED`` state and the flow is not in one
of the previous states.
