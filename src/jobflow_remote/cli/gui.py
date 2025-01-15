from typing import Annotated, Optional

import typer

from jobflow_remote.cli.jf import app


@app.command()
def gui(
    port: Annotated[
        Optional[int],
        typer.Option(
            "--port",
            "-p",
            help="The port where the web server will be started. Defaults to 5001",
        ),
    ] = None,
) -> None:
    """
    Start the server for the GUI
    """
    from jobflow_remote.webgui.webgui import start_gui

    start_gui(port=port)
