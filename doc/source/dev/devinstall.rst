.. _devinstall:

********************************
Developer setup and installation
********************************

Jobflow-remote is tested as much as possible for various cases. In order to guarantee long-term
functioning of the software, even when new features are added to the code, a test suite with three
different types of tests: unit, database and integration tests. To be able to run the tests, you should
install jobflow-remote with the [tests] optional dependency:

.. code-block:: shell

    pip install .[tests]

Types of tests
==============

The most basic tests are the *unit* tests, which cover a very small part of the code. They do not
require any database or external tool or service to be executed besides the python stack.

The *database* tests require a MongoDB database to run. As jobflow-remote relies heavily on MongoDB
(actually MongoDB is a requirement to be able to use jobflow-remote in production), this set of tests
is the minimal requirement for any new feature implemented in jobflow-remote. If you plan to implement
a new feature in jobflow-remote, you should set up a local MongoDB database with no authentication.

The last set of tests is the *integration* tests. These tests rely on a containerized setup that
requires a `Docker engine <https://docs.docker.com/engine/install/>`_ to be running on your local machine.
This Docker engine will create and launch containers running MongoDB and Slurm/Pbs/SGE workers to test
job submission.

These integration tests do not run by default (i.e., when simply running ``pytest``) as they need docker,
and take quite a long time to run.

All tests are marked as either unit, db (for database tests) or integration. You can then run the unit
or database tests using

.. code-block:: shell

    pytest -m unit

or

.. code-block:: shell

    pytest -m db

For integration tests, you also have to set the CI=1 variable:

.. code-block:: shell

    CI=1 pytest -m integration

You can of course run all tests at once by executing:

.. code-block:: shell

    CI=1 pytest

Test coverage
=============

You can get a test coverage by using the standard coverage options of pytest-cov:

.. code-block:: shell

    CI=1 pytest --cov=jobflow_remote --cov-report= --cov-report=html --cov-config=pyproject.toml

The above will provide the total coverage of all the tests. Note that this should be executed at the
root level of the jobflow-remote repository so that the pyproject.toml file is found.

We have also added an option (--coverage-per-flag) to gather the coverage per group ("flag") of tests.
By using this option, you will get a coverage per flag. Currently, the following flags are used:

- unit: coverage for unit tests
- db: coverage for database tests
- integration_local: coverage for integration tests (only the part of jobflow-remote executed "locally")
- integration_remote: coverage for integration tests (only the part of jobflow-remote executed "remotely",
  i.e. on the Slurm/Pbs/Sge containers)
- integration: coverage for integration tests (combination of the two coverages above)
- all_local: coverage for unit + database + "local" part of integration tests
- all: total coverage

Adding a new feature
====================

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

.. note::

    All the tests in jobflow-remote must be marked as either unit, db or integration. For
    tests added within the tests/unit, tests/db or tests/integration, this is done automatically
    If for some reason, you have to add a test outside of these directories, you should mark
    it explicitly (as either unit, db or integration). If you think your test should fall outside
    of these categories, please contact us by opening an issue.
