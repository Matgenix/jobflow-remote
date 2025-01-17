.. _gui:

===
GUI
===

A web based GUI is available for jobflow remote.
Being based on the `FastHTML framework <https://docs.fastht.ml/>`_,
requires the installation of the ``python-fasthtml`` package::

    pip install python-fasthtml

The package can also be installed when setting up jobflow-remote with::

    pip install jobflow-remote[gui]

.. warning::

    The web GUI is an experimental feature and in addition does not
    cover all the options available through the CLI.

The GUI can be started from the CLI with the command::

    jf gui

This will start a local server and the GUI can be reached with
a browser at the address `http://localhost:5001 <http://localhost:5001>`_.
The default port is 5001, but can be customized with the ``-p``
option.

Some of the features available in the GUI are:

* Selecting the project
* Starting and stopping the Runner
* Query Jobs and Flows based on different criteria
* Explore Jobs and Flows details
* Apply some standard actions on Jobs and Flows
