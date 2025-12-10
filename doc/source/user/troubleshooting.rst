.. _troubleshooting:


***************
Troubleshooting
***************

This section contains explanations and possible solutions for common issues
that can be encountered when using jobflow-remote. The focus here is on a
brief description of the problem and its potential solutions, with references to
specific sections of the documentation for more in depth descriptions of
the jobflow-remote components.

Note that this section mainly addresses technical issues related to the setup or
execution of jobflow-remote. For suggestions about how to deal with runtime errors
consult the :ref:`errors` section.

Changes to the Project not taken into account
=============================================

You may update the files containing the :ref:`projectconf`, but find that Jobs are
failing because the changes are not being recognized. For example a new worker
or a new execution configuration is added or modified, but after the the job fails
reporting that such a new configuration or worker does not exist.

The most likely explanation is that the **runner was not restarted
after the file updates**. As detailed in the :ref:`runner` section, the
Runner **reads the project configurations when the processes is started**
and does not attempt to refresh them during the execution.

The primary attempt to solve the issue is **restarting the Runner**.

If the problem persist, or appears to occur randomly, it is possible that
another Runner process has remained active and keeps operating on the database
the outdated version of the Project configuration. In this case it is necessary
to **identify the dangling process and kill it manually**. See also the
:ref:`troubleshooting multi_daemon` section.

.. _troubleshooting multi_daemon:

Multiple active daemons
=======================

When trying to start/stop the Runner, the system prompts an error stating that::

    A daemon runner process associated to this database may be already running

or, alternatively, more than one set of daemon processes is already
running simultaneously (which should not happen).

Key concepts about the Runner in these context are:

* Each active Runner daemon (which is a set of processes) can run multiple subprocesses
  to parallelize the execution of jobs.
* **Jobflow-remote is designed to have only one active daemon set at any given time**.
* The system employs safeguards (files in the project folder, database records) to
  prevent multiple daemons from starting. However, a user can bypass these checks
  (e.g., by using the jf runner reset command).

For more detailed information read the full :ref:`runner` section of the documentation.

Common scenarios leading to this issues can be:

* A cluster front-end that has **multiple front-end nodes** sharing the same file system.
* **Same project configuration** on two different physical machines that do not share the same
  file system. For example a local workstation and a cluster frontend.
* **Multiple projects pointing to the same "queue" database and collections** in the project
  configuration (see the documentation for the :ref:`projectconf queuestore`. Note that
  this is a misconfiguration: each Project should have a different Queue Store)

Daemon already running warning
------------------------------

When the daemon is successfully started on one machine/project, any subsequent attempt to
start it on another associated system will issue a warning that the Runner is already
active elsewhere.

In this scenario, it is critical to **identify where the other active daemon is running
and shut it down** before starting it on your current machine. The error message mentioned
above includes the hostname, project name, and start time of the other daemon to help
you locate it.

For additional information about the daemon process, run:

    jf runner info

If you have already verified that the daemon is **no longer active** in the other location
(e.g. the machine it was on has been shut down), you can force the removal of the previous
daemon record from the database by running::

    jf runner reset

Lost daemon processes
---------------------

One way to discover if there are multiple Runners active associated to a database
is to run the command::

    jf runner info --pings

that will list all the last pings from Runners. If you expect that the daemon
is shut down but you keep seeing pings from some processes there might be an
active Runner somewhere (note that the runner pings the database once every hour).

If you discover that multiple daemons are actively running you will
need to **manually terminate the lost processes**. This is because
jobflow-remote may have lost control over them.

Connect to the different machines and look for processes containing "runner"
and "supervisor". The output of ``ps -ef | grep runner`` should list several
processes similar to these (or a single one, if the daemon was started with the
``--single`` flag)::

    username    1850106 1850105  0 Nov24 ?        10:05:23 /bin/python /bin/jf -p test runner run -pid --checkout -log info
    username    1850107 1850105  0 Nov24 ?        10:05:26 /bin/python /bin/jf -p test runner run -pid --complete -log info
    username    1850108 1850105  0 Nov24 ?        10:05:28 /bin/python /bin/jf -p test runner run -pid --queue -log info
    username    1850109 1850105  0 Nov24 ?        10:05:37 /bin/python /bin/jf -p test runner run -pid --transfer -log info

The output of ``ps -ef | grep supervisor``, should list the parent process, similar
to this::

    username    1850105       1  0 Nov24 ?        10:00:36 /bin/python3.12 /bin/supervisord -c ~/.jfremote/test/daemon/supervisord.conf

You must manually kill all these identified processes on the other machines
before finally attempting to restart the Runner on your desired machine.
