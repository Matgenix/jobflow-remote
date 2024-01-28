from typing import Callable

import typer
from typer.models import CommandFunctionType

from jobflow_remote.cli.utils import cli_error_handler


class JFRTyper(typer.Typer):
    """
    Subclassing typer to intercept exceptions and print nicer error messages
    """

    def __init__(self, *args, **kwargs):
        if "epilog" not in kwargs:
            kwargs["epilog"] = (
                "Run [bold]'jf -h'[/] to display the [bold]global options[/]"
            )

        if "rich_markup_mode" not in kwargs:
            kwargs["rich_markup_mode"] = "rich"

        # if "result_callback" not in kwargs:
        #     kwargs["result_callback"] = test_cb

        super().__init__(*args, **kwargs)

    def command(
        self, *args, **kwargs
    ) -> Callable[[CommandFunctionType], CommandFunctionType]:
        typer_wrapper = super().command(*args, **kwargs)

        def wrapper(fn):
            fn = cli_error_handler(fn)
            return typer_wrapper(fn)

        return wrapper

    def callback(
        self, *args, **kwargs
    ) -> Callable[[CommandFunctionType], CommandFunctionType]:
        typer_wrapper = super().callback(*args, **kwargs)

        def wrapper(fn):
            fn = cli_error_handler(fn)
            return typer_wrapper(fn)

        return wrapper
