import importlib
import logging
import traceback
from dataclasses import dataclass
from typing import Annotated

import typer
from rich.padding import Padding

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import (
    exit_with_error_msg,
    exit_with_warning_msg,
    out_console,
)

logger = logging.getLogger(__name__)


PLUGIN_GROUP = "jobflow_remote.plugins"
PLUGIN_LOAD_FUNCTION = "setup_jf_plugin"

app_plugin = JFRTyper(
    name="plugin",
    help="Commands for managing plugins",
    no_args_is_help=True,
)
app.add_typer(app_plugin)


@dataclass
class PluginStatus:
    """
    Outcome of the loading of a plugin.

    Attributes
    ----------
    value
        The value of the entry point (i.e. the module of the plugin).
    error
        The error that prevented the plugin from being loaded. None if the
        plugin was loaded successfully.
    """

    value: str
    error: str | None = None


# The outcome of the plugins loaded by load_plugins, with the entry point names
# as keys, and the error raised while discovering the plugins, if any.
# _loaded_plugins is None if load_plugins has not been called.
_loaded_plugins: dict[str, PluginStatus] | None = None
_discovery_error: str | None = None


def _setup_plugins() -> tuple[dict[str, PluginStatus], str | None]:
    """
    Load the plugins from the entry points and run their setup function.

    Returns
    -------
    tuple
        A dictionary with the entry point names as keys and the outcome of
        the loading as values, and the traceback of the error raised while
        discovering the entry points, if any.
    """
    plugins: dict[str, PluginStatus] = {}
    try:
        for entry_point in importlib.metadata.entry_points().select(group=PLUGIN_GROUP):
            try:
                plugin_module = entry_point.load()

                # Call the plugin's setup function
                if hasattr(plugin_module, PLUGIN_LOAD_FUNCTION):
                    plugin_module.setup_jf_plugin()
                    plugins[entry_point.name] = PluginStatus(entry_point.value)
                else:
                    plugins[entry_point.name] = PluginStatus(
                        entry_point.value,
                        error=f"No {PLUGIN_LOAD_FUNCTION} function",
                    )

            except Exception:
                logging.debug(
                    f"Failed to load plugin {entry_point.name}", exc_info=True
                )
                plugins[entry_point.name] = PluginStatus(
                    entry_point.value, error=traceback.format_exc()
                )

    except Exception:
        logging.debug("Error discovering plugins", exc_info=True)
        return plugins, traceback.format_exc()

    return plugins, None


def load_plugins() -> None:
    """
    Load and register plugins from entry points.

    Main function used to load the plugin that will be added to the main CLI.
    The outcome for each plugin is stored, to be reported by `jf plugin list`
    without running the setup of the plugins again.
    """
    global _loaded_plugins, _discovery_error
    _loaded_plugins, _discovery_error = _setup_plugins()


@app_plugin.command(name="list")
def list_plugins(
    error: Annotated[
        bool,
        typer.Option(
            "--error",
            "-e",
            help="Print the error messages for the plugin that cannot be loaded",
        ),
    ] = False,
):
    """List all plugins with an entry point"""
    from jobflow_remote import SETTINGS

    if _loaded_plugins is None:
        # plugins have not been loaded when starting the CLI (e.g. disabled
        # in the settings). Load them here to check if they are available.
        plugins_status, discovery_error = _setup_plugins()
    else:
        plugins_status, discovery_error = _loaded_plugins, _discovery_error

    if discovery_error:
        msg = discovery_error if error else discovery_error.strip().splitlines()[-1]
        exit_with_error_msg(f"Error discovering plugins: {msg}")

    plugins = {n: s.value for n, s in plugins_status.items() if s.error is None}
    error_msgs = {n: s.error for n, s in plugins_status.items() if s.error is not None}

    if plugins:
        out_console.print("Available plugins:")
        for name, module in plugins.items():
            out_console.print(f" - {name}: {module}")
    else:
        out_console.print("No plugins found.")

    if error_msgs:
        if error:
            out_console.print("Errors discovering plugins:")
            for name, error_msg in error_msgs.items():
                out_console.print(f" - {name}:")
                out_console.print(Padding(error_msg, pad=(0, 0, 0, 4)))
        else:
            msg = "Errors discovering plugins."
            if SETTINGS.cli_suggestions:
                msg += " Run the command with the --error option to get the details"
            exit_with_warning_msg(msg)
