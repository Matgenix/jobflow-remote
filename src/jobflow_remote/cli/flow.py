import typer

from jobflow_remote.cli.jf import app

app_flow = typer.Typer(
    name="flow", help="Commands for managing the flows", no_args_is_help=True
)
app.add_typer(app_flow)
