.. _cliplugin:

**********
CLI Plugin
**********

The jobflow-remote CLI supports a plugin mechanism that allows external packages
to extend the ``jf`` command with additional functionality. This enables
developers to create packages that seamlessly integrate with the jobflow-remote
CLI without modifying the core codebase.

Plugin Architecture
===================

CLI Framework
-------------

The jobflow-remote CLI is built using `Typer <https://typer.tiangolo.com/>`_, a Python library for building command-line interfaces. The CLI code is organized in the ``src/jobflow_remote/cli/`` directory, where each module typically corresponds to a command group (e.g., ``job.py`` for ``jf job`` commands, ``flow.py`` for ``jf flow`` commands). Understanding this structure helps when extending existing command groups or creating new ones.

Plugin System
-------------

The plugin system is based on Python entry points and follows these key principles:

- **Entry Point Discovery**: Plugins are discovered through the ``jobflow_remote.plugins`` entry point group
- **Automatic Loading**: Plugins are automatically loaded when the CLI starts (if enabled in settings)
- **Function-Based Setup**: Each plugin defines a ``setup_jf_plugin`` function that registers commands
- **Typer Integration**: Plugins can extend existing typer apps or create new command groups

Creating a Plugin
=================

Step 1: Define Entry Point
---------------------------

In your package's ``pyproject.toml``, define an entry point in the ``jobflow_remote.plugins`` group:

.. code-block:: toml

    [project.entry-points."jobflow_remote.plugins"]
    my_plugin = "my_package.jf_plugin"

Where:

- ``my_plugin`` is the plugin name
- ``my_package.jf_plugin`` is the module path containing your plugin code

Step 2: Implement Plugin Module
-------------------------------

Create a module (e.g., ``my_package/jf_plugin.py``) with a ``setup_jf_plugin`` function:

.. code-block:: python

    def setup_jf_plugin():
        """
        Plugin setup function called by jobflow-remote CLI.
        This function should import typer apps and register commands.
        """
        from jobflow_remote.cli.job import app_job
        from jobflow_remote.cli.utils import out_console

        @app_job.command()
        def my_command():
            """My custom command added to 'jf job' group."""
            out_console.print("Hello from my plugin!")

This example adds a ``my-command`` to the ``jf job`` command group, accessible as:

.. code-block:: shell

    jf job my-command

Creating New Command Groups
===========================

You can also create entirely new command groups:

.. code-block:: python

    def setup_jf_plugin():
        from jobflow_remote.cli.jf import app
        from jobflow_remote.cli.jfr_typer import JFRTyper
        from jobflow_remote.cli.utils import out_console

        # Create a new command group
        app_backup = JFRTyper(
            name="backup",
            help="Backup and restore operations",
            no_args_is_help=True,
        )

        @app_backup.command()
        def create():
            """Create a backup."""
            out_console.print("Creating backup...")

        @app_backup.command()
        def restore():
            """Restore from backup."""
            out_console.print("Restoring backup...")

        # Register the new command group
        app.add_typer(app_backup)

This creates a new ``jf backup`` command group with ``create`` and ``restore`` subcommands.

Plugin Management
=================

Users can manage plugins using the built-in plugin commands:

List Available Plugins
-----------------------

.. code-block:: shell

    jf plugin list

This shows all discovered plugins and their status.

View Plugin Errors
-------------------

.. code-block:: shell

    jf plugin list --error

This displays detailed error messages for plugins that failed to load.

Best Practices
==============

Error Handling
--------------

Implement robust error handling in your plugin setup function:

.. code-block:: python

    def setup_jf_plugin():
        try:
            from jobflow_remote.cli.job import app_job

            @app_job.command()
            def my_command():
                """My plugin command."""
                pass

        except ImportError as e:
            # Handle missing dependencies gracefully
            import logging

            logging.debug(f"Plugin setup failed: {e}")

Logging Integration
-------------------

The jobflow-remote CLI uses its own logging system initialized through
the ``initialize_cli_logger`` function. By default, only log messages
from the ``jobflow_remote`` logger and explicitly registered additional
loggers are displayed in the CLI output.

If your plugin needs to have its log messages shown in the CLI, register
your logger names using the ``add_cli_logger_names`` function in ``setup_jf_plugin``:

.. code-block:: python

    def setup_jf_plugin():
        from jobflow_remote.cli.jf import add_cli_logger_names

        # Register your package's loggers to be displayed in CLI
        add_cli_logger_names(["my_package", "my_package.submodule"])


This ensures that log messages from your plugin modules are properly displayed alongside jobflow-remote's own logging output.

Naming Conventions
------------------

- Use descriptive plugin names in entry points
- Follow existing CLI naming patterns for commands

Troubleshooting
===============

Plugin Fails to Load
---------------------

- Use ``jf plugin list --error`` to see detailed error messages
- Check for missing dependencies or import errors
- Verify the ``setup_jf_plugin`` function exists and is callable

Commands Not Appearing
-----------------------

- Ensure plugins are enabled in jobflow-remote settings
- Check that the ``setup_jf_plugin`` function completes without errors
- Verify you're importing the correct typer app objects
