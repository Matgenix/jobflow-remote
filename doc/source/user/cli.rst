.. _cli:

===
CLI
===

Jobflow-remote allows to manage Jobs and Flows through the ``jf`` command line
interface (CLI). The most useful commands are already discussed in the
specific sections. A list of all the commands available can be obtained
running::

    jf --tree

or for the commands available for a subsection with, for example::

    jf job --tree

All the commands have an associated help that can be shown with the
``--help`` flag. Below are reported the help for all the commands
available in ``jf``.

.. typer:: jobflow_remote.cli:app
    :preferred: html
    :width: 65
    :show-nested:
    :make-sections:
