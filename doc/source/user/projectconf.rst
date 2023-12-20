.. _projectconf:

**********************
Projects configuration
**********************

Jobflow-remote allows to handle multiple configurations, defined projects. Since
for most of the users a single project is enough let us first consider the configuration
of a single project. The handling of multiple projects will be described below.

The configurations allow to control the behaviour of the Job execution, as well as
the other objects in jobflow-remote. Here a full description of the project's
configuration file will be given. If you are looking for a minimal example with its
description you can find it in the :ref:`minimal project config` section.

The specifications of the project's attributes are given by the ``Project`` pydantic
model, that serves the purpose of parsing and validating the configuration files, as
well as giving access to the associated objects (e.g. the ``JobStore``).
A graphical representation of the ``Project`` model and thus of the options available
in the configuration file is given below (generated with `erdantic <https://erdantic.drivendata.org/stable/>`_)

.. image:: ../_static/img/project_erdantic.png
   :width: 100%
   :alt: All-in-one configuration
   :align: center

A description for all the types and keys of the project file is given in the :ref:`project detailed specs`
section below, while an example for a full configuration file can be generated running::

    jf project generate --full YOUR_PROJECT_NAME

Note that, while the default file format is YAML, JSON and TOML are also acceptable format.
You can generate the example in the other formats using the ``--format`` option.

Project options
===============

Name and folders
----------------

The project name is given by the ``name`` attribute. The name will be used to create
a subfolder containing

* files with the parsed outputs copied from the remote workers
* logs
* files used by the daemon

For all these folders the paths are set with defaults, but can be customised setting

``tmp_dir``, ``log_dir`` and ``daemon_dir``.

.. warning::
  The project name does not take into consideration the configuration file name.
  For coherence it would be better to give use the project name as file name.

Workers
-------

Multiple workers can be defined in a project. In the configuration file they are given
with their name as keyword, and their properties in the contained dictionary.

Several defining properties should be set in the configuration of each workers.
First it should be specified the ``type``. At the moment the possible worker types are

* ``local``: a worker running on the same system as the ``Runner``. No connection is
  needed for the ``Runner`` to reach the queueing system.
* ``remote``: a worker on a different machine than the ``Runner``, requiring an SSH
  connection to reach it.

Since the ``Runner`` needs to constantly interact with the workers, for the latter
type all the credentials to connect automatically should be provided. The best option
would be to set up a passwordless connection and define it in the ``~/.ssh/config``
file.

The other key property of the workers is the ``scheduler_type``.

.. note::

    If a single worker is defined it will be used as default in the submission
    of new Flows.


Multiple Projects
=================

asdsd

.. _project detailed specs:

Project specs
=============

.. raw:: html
   :file: ../_static/project_schema.html
