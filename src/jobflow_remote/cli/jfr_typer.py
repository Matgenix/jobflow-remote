from typing import Callable

import typer
from typer.models import CommandFunctionType

from jobflow_remote.cli.utils import cli_error_handler


class JFRTyper(typer.Typer):
    """
    Subclassing typer to intercept exceptions and print nicer error messages
    """

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
