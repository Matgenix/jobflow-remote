.. _devinstall:

********************************
Developer setup and installation
********************************

When developing jobflow-remote, one does not necessarily need to set up
a remote runner environment and database.
Instead, one can use the containerized setup provided as part of the integration
tests.
This requires a `Docker engine <https://docs.docker.com/engine/install/>`_ to be running on your local machine,
as it will create and launch containers running MongoDB and Slurm to test job submission.

These tests do not run by default (i.e., when simply running ``pytest``) as they conflict with the
units tests, but can instead be installed and then invoked with:

.. code-block:: shell

    pip install .[tests]
    CI=1 pytest tests/integration

When adding new features to jobflow-remote, please consider adding an
integration test to ensure that the feature works as expected, before
following the contributing instructions at :ref:`contributing`.

.. warning::

    The integration tests will create a container running MongoDB and Slurm
    on your local machine that will be cleaned up when the tests finish.
    If you ``KeyboardInterrupt`` the tests, the container may not be cleaned
    successfully.
    A random free port will be chosen for Slurm and MongoDB each; if you
    encounter errors with these ports (as the process can be system-dependent),
    please raise an issue detailing your setup.
    In this case, you may wish to manually override the pytest fixtures for
    port specification in ``tests/integration/conftest.py`` (but do not commit
    these changes).
