import importlib
import logging
import traceback
from typing import Annotated

import typer
from rich.padding import Padding

from jobflow_remote.cli.jf import app
from jobflow_remote.cli.jfr_typer import JFRTyper
from jobflow_remote.cli.utils import exit_with_warning_msg, out_console

logger = logging.getLogger(__name__)


PLUGIN_GROUP = "jobflow_remote.plugins"
PLUGIN_LOAD_FUNCTION = "setup_jf_plugin"

app_plugin = JFRTyper(
    name="plugin",
    help="Commands for managing plugins",
    no_args_is_help=True,
)
app.add_typer(app_plugin)


def load_plugins():
    """
    Load and register plugins from entry points.

    Main function used to load the plugin that will be added to the main CLI
    """

    try:
        for entry_point in importlib.metadata.entry_points().select(group=PLUGIN_GROUP):
            try:
                plugin_module = entry_point.load()

                # Call the plugin's setup function
                if hasattr(plugin_module, PLUGIN_LOAD_FUNCTION):
                    plugin_module.setup_jf_plugin()

            except Exception:
                logging.debug(
                    f"Failed to load plugin {entry_point.name}", exc_info=True
                )

    except Exception:
        logging.debug("Error discovering plugins", exc_info=True)


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

    plugins = {}
    error_msgs = {}
    # if there is an error discovering plugins let it pass as it should
    # be logged by the CLI
    for entry_point in importlib.metadata.entry_points().select(group=PLUGIN_GROUP):
        try:
            plugin_module = entry_point.load()

            # Call the plugin's setup function
            if hasattr(plugin_module, PLUGIN_LOAD_FUNCTION):
                plugin_module.setup_jf_plugin()
                plugins[entry_point.name] = entry_point.value
            else:
                error_msgs[entry_point.name] = f"No {PLUGIN_LOAD_FUNCTION} function"

        except Exception:
            error_msgs[entry_point.name] = traceback.format_exc()

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
